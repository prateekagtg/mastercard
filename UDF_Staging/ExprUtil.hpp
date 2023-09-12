/******************************************************************************
 * Copyright (c) 2023, TigerGraph Inc.
 * All rights reserved.
 * Project: TigerGraph Query Language
 *
 * - This library is for defining struct and helper functions that will be used
 *   in the user-defined functions in "ExprFunctions.hpp". Note that functions
 *   defined in this file cannot be directly called from TigerGraph Query
 *scripts. Please put such functions into "ExprFunctions.hpp" under the same
 *directory where this file is located.
 *
 * - Please don't remove necessary codes in this file
 *
 * - A backup of this file can be retrieved at
 *     <tigergraph_root_path>/dev_<backup_time>/gdk/gsql/src/QueryUdf/ExprUtil.hpp
 *   after upgrading the system.
 *
 ******************************************************************************/

#ifndef EXPRUTIL_HPP_
#define EXPRUTIL_HPP_

#include <algorithm>
#include <cmath>
#include <gle/engine/cpplib/headers.hpp>
#include <utility/gutil/glogging.hpp>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <unistd.h>
#include <atomic>

#include <iostream>
#include <exception>
#include <librdkafka/rdkafkacpp.h>
#include <unordered_set>

/*Added on 03/10/2023 for KafkaUDF*****************************************************/
class SimpleDeliveryReportCb : public RdKafka::DeliveryReportCb
{
public:
    int error_code;
    void dr_cb(RdKafka::Message &message)
    {
        /* If message.err() is non-zero the message delivery failed permanently
         * for the message. */
        if (message.err()) {
            std::cerr << "% Message delivery failed: " << message.errstr()
                      << std::endl;
            error_code = message.err();
        }
        else {
            // std::cerr << "% Message delivered to topic " << message.topic_name()
            //           << " [" << message.partition() << "] at offset "
            //           << message.offset() << std::endl;
            error_code = 0;
        }
    }
};
/***********************************************************************************/
/*Added on 06/22/2023 Egress to CEPH S3*********************************************/
class GlobalStringSet
{
public:
    static size_t count(const std::string& k)
    {
        return string_set.count(k);
    }

    static void insert(const std::string& k)
    {
        string_set.insert(k);
    }

    static size_t erase(const std::string& k)
    {
        return string_set.erase(k);
    }

    static void clear()
    {
        string_set.clear();
    }

private:
    inline static std::unordered_set<std::string> string_set;
};
/***********************************************************************************/
typedef std::string string;

// XXX DON'T REMOVE
/*
 * Define structs that used in the functions in "ExprFunctions.hpp"
 * below. For example,
 *
 *   struct Person {
 *     string name;
 *     int age;
 *     double height;
 *     double weight;
 *   }
 *
 */
struct Spinlock {
  std::atomic<bool> lock_ = {false};
  void lock() {
    for (;;) {
      if (!lock_.exchange(true, std::memory_order_acquire)) {
        break;
      }
      while (lock_.load(std::memory_order_relaxed)) {
        __builtin_ia32_pause();
      }
    }
  }
  void unlock() { lock_.store(false, std::memory_order_release); }
};

static const uint32_t SegmentBits = 20;
static const uint32_t SegmentSize = (1 << SegmentBits); // 1048576;
static const uint32_t SegmentMask = SegmentSize - 1;

struct SegmentCSR {
  // src->tgt adjcent list
  std::vector<uint64_t> adj_list_;
  // edges for i:  [offset[i-1]|0 , offset[i])
  // degree for i: offset[i] - offset[i-1]|0
  std::vector<int64_t> offset_list_;
  SegmentCSR() = default;
  SegmentCSR(SegmentCSR &&) = default;
};

struct WeightedSegmentCSR {
  // src -> <tgt,weight> adjcent list
  std::vector<std::pair<uint64_t, float>> adj_list_;
  // edges for i:  [offset[i-1]|0 , offset[i])
  // degree for i: offset[i] - offset[i-1]|0
  // weight for i: weight[i]
  std::vector<int64_t> offset_list_;
  std::vector<float> weight_list_;
  WeightedSegmentCSR() = default;
  WeightedSegmentCSR(WeightedSegmentCSR &&) = default;
};

// Graph/Edge data
struct Graph {
public:
  Graph() {}

  void Initialize(const std::string &request,
                  const ListAccum<uint64_t> &vertices,
                  const MapAccum<uint64_t, ListAccum<uint64_t>> &src,
                  const MapAccum<uint64_t, ListAccum<uint64_t>> &tgt,
                  const MapAccum<uint64_t, ListAccum<float>> &weight) {
    {
      char *end;
      int64_t timeout_ms = std::strtoll(request.c_str(), &end, 10);
      int64_t segSize = std::strtoll(end, &end, 10);
      string rid = request;

      std::lock_guard<std::mutex> lock(graph_mutex_);
      // already initialized
      if (running_) {
        return;
      }
      LOG(INFO) << "UDF updating " << request << " " << &graph_mutex_;
      timeout_ = timeout_ms;
      running_ = true;
      requestid_ = rid;
      totalSeg_ = segSize;
    }
    ResetGraph(totalSeg_);
    std::cout << "Graph INFO: " << vertices.size()
              << " " << src.size()
              << " " << tgt.size()
              << " " << weight.size()
              << " " << totalSeg_ << "\n";
    for (auto &v : vertices)
      Fill(v);

    for (auto& s : src.data_) {
      auto& source = s.second;
      auto& target = tgt.data_[s.first];
      auto& wei = weight.data_[s.first];
      std::cout << "SEG " << std::to_string(s.first) << " "
	        << std::to_string(source.size()) << "\n";
      if (source.size() != target.size() || target.size() != wei.size())
	throw std::runtime_error("Edge has inconsistency, abort..");
      for (int i=0; i<source.size(); ++i)
        Fill(source.data_[i], target.data_[i], wei.data_[i]);
    }
    // Fill missing vertices with no edges at the end
    for (int segid = 0; segid < totalSeg_; ++segid) {
      auto maxvid = max_vids_[segid];
      if (wdata_[segid].offset_list_.size() < maxvid + 1) {
        if (maxvid != 0) {
          std::cout << "Expand segment " << std::to_string(segid)
              << " " << std::to_string(wdata_[segid].offset_list_.size())
              << " " << std::to_string(maxvid) << "\n";
        }
        size_t edge = wdata_[segid].adj_list_.size();
        wdata_[segid].offset_list_.resize(maxvid + 1, edge);
        wdata_[segid].weight_list_.resize(maxvid + 1, 0);
      }
    }
    std::cout << "Graph creation " << request << " done\n";
  }

  void ResetGraph(size_t segsize) {
    data_.clear();
    data_.resize(segsize);
    wdata_.clear();
    wdata_.resize(segsize);
    max_vids_.clear();
    max_vids_.resize(segsize, 0);
  }

  inline int64_t GetDegree(uint64_t src, bool weight) {
    uint64_t segid = src >> SegmentBits;
    uint64_t localid = src & SegmentMask;
    if (weight) {
      return GetDegree(segid, localid, wdata_);
    } else {
      return GetDegree(segid, localid, data_);
    }
  }

  template <class Data>
  inline int64_t GetDegree(uint64_t segid, uint64_t localid, Data &data) {
    if (UNLIKELY(segid >= totalSeg_ || localid >= data[segid].offset_list_.size()))
      throw std::runtime_error("Outdegree lookup error " + std::to_string(segid)
	  + " " + std::to_string(localid)
	  + " " + std::to_string(totalSeg_)
	  + " " + std::to_string(data[segid].offset_list_.size()));
    return localid == 0 ? data[segid].offset_list_[localid]
                        : (data[segid].offset_list_[localid] -
                           data[segid].offset_list_[localid - 1]);
  }

  inline float GetWeight(uint64_t src) {
    uint64_t segid = src >> SegmentBits;
    uint64_t localid = src & SegmentMask;
    return GetWeight(segid, localid);
  }

  inline float GetWeight(uint64_t segid, uint64_t localid) {
    if (UNLIKELY(segid >= totalSeg_ || localid >= wdata_[segid].weight_list_.size()))
      throw std::runtime_error("Weight lookup error " + std::to_string(segid)
          + " " + std::to_string(localid)
	  + " " + std::to_string(totalSeg_)
          + " " + std::to_string(wdata_[segid].weight_list_.size()));
    return wdata_[segid].weight_list_[localid];
  }

  inline uint32_t GetMaxVid(uint32_t segid) { return max_vids_[segid]; }

  inline size_t GetSegSize() { return totalSeg_; }

  inline void Fill(uint64_t v) {
    uint64_t segid = v >> SegmentBits;
    uint64_t localid = v & SegmentMask;
    if (max_vids_[segid] < localid) {
      max_vids_[segid] = localid;
    }
  }

  inline void Fill(uint64_t src, uint64_t tgt) {
    // x,y,z,i,j,k
    // 0, 0, 3, 3, 5, 6 [ offset[i-1]|0 , offset[i] )
    //      (x,y,z)(i,j)(k)
    // 0  0  3  0  2  1 (degree)
    uint32_t segid = src >> SegmentBits;
    uint32_t localid = src & SegmentMask;
    if (data_[segid].offset_list_.size() <= localid) {
      size_t edge = data_[segid].adj_list_.size();
      data_[segid].offset_list_.resize(localid + 1, edge);
    }
    data_[segid].offset_list_[localid]++;
    data_[segid].adj_list_.push_back(tgt);
  }

  inline void Fill(uint64_t src, uint64_t tgt, float weight) {
    // x-w1,y-w2,z-w3,i-w4,j-w5,k-w6
    // 0, 0, 3, 3, 5, 6 [ offset[i-1]|0 , offset[i] )
    //      (x,y,z)(i,j)(k)
    // 0  0  3  0  2  1 (degree)
    // 0  0 1.4 0 0.8 0.4 (total weight)
    // Note:
    // Last few vertices in a segment may not have offset populated
    // yet. To be filled at the end of InitializeGraph()
    uint32_t segid = src >> SegmentBits;
    uint32_t localid = src & SegmentMask;
    if (UNLIKELY(segid >= totalSeg_ || localid > max_vids_[segid]))
      throw std::runtime_error("Vertex too large "
          + std::to_string(segid) + " " + std::to_string(localid)
          + " " + std::to_string(totalSeg_)
          + " " + std::to_string(max_vids_[segid]));

    if (wdata_[segid].offset_list_.size() <= localid) {
      size_t edge = wdata_[segid].adj_list_.size();
      wdata_[segid].offset_list_.resize(localid + 1, edge);
      wdata_[segid].weight_list_.resize(localid + 1, 0);
    }
    uint32_t segid2 = tgt >> SegmentBits;
    uint32_t localid2 = tgt & SegmentMask;
    if (UNLIKELY(segid2 >= totalSeg_ || localid2 > max_vids_[segid2]))
      throw std::runtime_error("Target vertex too large "
          + std::to_string(segid2) + " " + std::to_string(localid2)
          + " " + std::to_string(totalSeg_)
          + " " + std::to_string(max_vids_[segid2]));

    wdata_[segid].offset_list_[localid]++;
    wdata_[segid].weight_list_[localid] += weight;
    wdata_[segid].adj_list_.push_back(std::make_pair(tgt, weight));
  }

  const std::vector<SegmentCSR> &GetData() { return data_; }
  const std::vector<WeightedSegmentCSR> &GetWeightedData() { return wdata_; }

private:
  // Graph in use, only one query can run
  std::mutex graph_mutex_;
  bool running_ = false;
  string requestid_ = "";
  uint64_t timeout_ = 0;
  std::vector<SegmentCSR> data_;
  std::vector<WeightedSegmentCSR> wdata_;
  std::vector<uint32_t> max_vids_;
  size_t totalSeg_ = 25000;
};

struct Compute {
  Spinlock *lock_;
  std::vector<float> *current_ = nullptr;
  std::vector<float> *prior_ = nullptr;
  std::unique_ptr<std::vector<float>> rank_;
  std::unique_ptr<std::vector<float>> rank2_;
  Compute(Compute &&) noexcept = default;
  Compute() { lock_ = new Spinlock(); }
  ~Compute() { delete lock_; }
};

struct PageRank {
public:
  const static size_t thread_num_ = 1;

  /* void InitializeCompute(Graph &graph) {
    graph_ = &graph;
    compute.clear();
    compute.resize(graph.GetSegSize());
    for (size_t i = 0; i < graph.GetSegSize(); ++i) {
      compute[i].rank_ =
	  std::unique_ptr<std::vector<float>>(
	      new std::vector<float>(graph.GetMaxVid(i) + 1, 1));
      compute[i].current_ = compute[i].rank_.get();
      compute[i].rank2_ =
          std::unique_ptr<std::vector<float>>(
              new std::vector<float>(graph.GetMaxVid(i) + 1, 0));
      compute[i].prior_ = compute[i].rank2_.get();
    }
  }

  void InitializeCompute(Graph &graph, const ListAccum<uint64_t> &input) {
    graph_ = &graph;
    compute.clear();
    compute.resize(graph.GetSegSize());
    for (size_t i = 0; i < graph.GetSegSize(); ++i) {
      compute[i].rank_ =
          std::unique_ptr<std::vector<float>>(
              new std::vector<float>(graph.GetMaxVid(i) + 1, 0));
      compute[i].current_ = compute[i].rank_.get();
      compute[i].rank2_ =
          std::unique_ptr<std::vector<float>>(
              new std::vector<float>(graph.GetMaxVid(i) + 1, 0));
      compute[i].prior_ = compute[i].rank2_.get();
    }
    for (auto v : input.data_) {
      uint64_t segid = v >> SegmentBits;
      uint64_t localid = v & SegmentMask;
      if (UNLIKELY(segid >= graph.GetSegSize())) {
        throw std::runtime_error("Initializing vertex with large segment "
	    + std::to_string(segid) + " " + std::to_string(localid)
	    + " " + std::to_string(graph.GetSegSize()));
      }
      if (compute[segid].rank_) {
        if (UNLIKELY(localid >= compute[segid].rank_->size())) {
           throw std::runtime_error("Initializing vertex does not exist "
               + std::to_string(segid) + " " + std::to_string(localid)
               + " " + std::to_string(compute[segid].rank_->size()));
        }
        compute[segid].rank_->at(localid) = 1;
        start_set_.insert(v);
      }
    }
  } */

  void InitializeComputeSparse(Graph &graph, const ListAccum<uint64_t> &input) {
    graph_ = &graph;
    compute.clear();
    compute.resize(graph.GetSegSize());
    for (size_t i = 0; i < graph.GetSegSize(); ++i) {
      if (graph.GetMaxVid(i) > 0) {
        compute[i].rank_ =
            std::unique_ptr<std::vector<float>>(
                new std::vector<float>(graph.GetMaxVid(i) + 1, 0));
        compute[i].current_ = compute[i].rank_.get();
      }
    }
    for (auto v : input.data_) {
      start_set_.insert(v);
    }
  }

  void RunOneIterationSparse(bool weight, const ListAccum<uint64_t> &output_list, float damping) {
    const auto &data = graph_->GetWeightedData();
    for (uint64_t v : start_set_) {
      uint64_t segid = v >> SegmentBits;
      uint64_t localid = v & SegmentMask;
      uint64_t ss = (localid == 0 ? 0 : data[segid].offset_list_[localid - 1]);
      uint64_t ee = data[segid].offset_list_.at(localid);
      float r = 1.0;
      auto w = graph_->GetWeight(segid, localid);
      if (ee > ss && r > 0 && w != 0) {
        float rankbydegree = r / w;
        for (uint64_t s = ss; s < ee; ++s) {
          auto &vv = data[segid].adj_list_[s];
          uint64_t segid2 = vv.first >> SegmentBits;
          uint64_t localid2 = vv.first & SegmentMask;
          (*compute[segid2].current_).at(localid2) += (vv.second * rankbydegree);
          next_set_.insert(vv.first); 
        }
      }
    }
    for (auto& v : next_set_) {
      uint64_t segid2 = v >> SegmentBits;
      uint64_t localid2 = v & SegmentMask;
      float val = (*compute[segid2].current_).at(localid2);
      if (start_set_.find(v) != start_set_.end())
        (*compute[segid2].current_).at(localid2) = (1 - damping) + damping * val;
      else
        (*compute[segid2].current_).at(localid2) = damping * val;
    }
    for (auto& v : start_set_) {
      if (next_set_.find(v) == next_set_.end()) {
        uint64_t segid2 = v >> SegmentBits;
        uint64_t localid2 = v & SegmentMask;
	(*compute[segid2].current_).at(localid2) = (1 - damping);
        next_set_.insert(v);
      }
    }
  }

  MapAccum<uint64_t, float> RunOneIterationFinal(bool weight, 
						 const ListAccum<uint64_t> &output_list,
						 float damping) {
    MapAccum<uint64_t, float> output;
    for (auto& v : output_list) output.data_[v] = 0; 
    const auto &data = graph_->GetWeightedData();
    for (uint64_t v : next_set_) {
      uint64_t segid = v >> SegmentBits;
      uint64_t localid = v & SegmentMask;
      uint64_t ss = (localid == 0 ? 0 : data[segid].offset_list_[localid - 1]);
      uint64_t ee = data[segid].offset_list_.at(localid);
      auto r = (*compute[segid].current_).at(localid);
      auto w = graph_->GetWeight(segid, localid);
      if (ee > ss && r > 0 && w != 0) {
        float rankbydegree = r / w;
        for (uint64_t s = ss; s < ee; ++s) {
          auto &vv = data[segid].adj_list_[s];
          if (output.data_.find(vv.first) == output.data_.end()) continue;
          output.data_[vv.first] += (vv.second * rankbydegree);     
        }
      }
    }

    for (auto& o : output.data_) {
      if (o.second != 0) {
        if (start_set_.find(o.first) != start_set_.end())
          o.second = (1 - damping) + damping * o.second;
        else
          o.second = damping * o.second;     
      } else {
        uint64_t h = o.first >> SegmentBits;
        uint64_t i = o.first & SegmentMask;
        o.second = (*compute[h].current_).at(i);
      }
    }
    return output;
  }

  void
  ProcessWeightedPageRank(std::vector<std::pair<uint64_t, uint64_t>> assignment) {
    const auto &data = graph_->GetWeightedData();
    if (sparse_) {
      // Optimize for first iteration
      // uint64_t ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      //                  std::chrono::system_clock::now().time_since_epoch())
      //                  .count();
      // uint64_t edges = 0;
      for (uint64_t v : start_set_) {
        uint64_t segid = v >> SegmentBits;
        uint64_t localid = v & SegmentMask;        
      //  if (UNLIKELY(segid >= data.size() ||
      //      localid >= data[segid].offset_list_.size())) {
      //    throw std::runtime_error("First iteration src error " + std::to_string(segid)
      //        + " " + std::to_string(localid)
      //        + " " + std::to_string(data.size())
      //        + " " + std::to_string(data[segid].offset_list_.size()));          
      //  }
        uint64_t ss = (localid == 0 ? 0 : data[segid].offset_list_[localid - 1]);
        uint64_t ee = data[segid].offset_list_.at(localid);
        auto r = (*compute[segid].current_).at(localid);
        auto w = graph_->GetWeight(segid, localid);
        if (ee > ss && r > 0 && w != 0) {
          float rankbydegree = r / w;
          for (uint64_t s = ss; s < ee; ++s) {
            auto &v = data[segid].adj_list_[s];
            uint64_t segid2 = v.first >> SegmentBits;
            uint64_t localid2 = v.first & SegmentMask;
      //      if (UNLIKELY(segid2 >= data.size() ||
      //          localid2 >= data[segid2].offset_list_.size())) {
      //        throw std::runtime_error("First iteration tgt error " + std::to_string(segid2)
      //            + " " + std::to_string(localid2)
      //            + " " + std::to_string(data.size())
      //            + " " + std::to_string(data[segid2].offset_list_.size()));
      //      }
//          compute[segid2].lock_->lock();
            (*compute[segid2].prior_).at(localid2) += (v.second * rankbydegree);
	    next_set_.insert(v.first);
//          compute[segid2].lock_->unlock();
          }
        }
      }
      return;
    }

    // #pragma omp parallel
    {
      // uint64_t ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      //                  std::chrono::system_clock::now().time_since_epoch())
      //                  .count();
      size_t id = 0; // omp_get_thread_num();
      std::vector<std::vector<std::pair<uint64_t, float>>> thread_local_buffer(
          graph_->GetSegSize());
      size_t count = 0;
      // when id == 0, start_localvid will be +1 in the loop
      int64_t start_segid = 0, start_localvid = -1, end_segid = -1,
              end_localvid = -1;
      if (id < assignment.size()) {
        if (id != 0) {
          start_segid = assignment[id - 1].first;
          start_localvid = assignment[id - 1].second;
        }
        end_segid = assignment[id].first;
        end_localvid = assignment[id].second;
      }
      size_t edges = 0;
      float rankbydegree = 0;
      for (auto segid = start_segid; segid <= end_segid; ++segid) {
        if (data[segid].adj_list_.size() > 0) {
          int64_t start = 0, end = data[segid].offset_list_.size() - 1;
          if (segid == start_segid)
            start = start_localvid + 1; // exclusive
          if (segid == end_segid)
            end = end_localvid; // inclusive

          for (auto localid = start; localid <= end; ++localid) {
//            if (UNLIKELY(segid >= data.size() ||
//                localid >= data[segid].offset_list_.size())) {
//              throw std::runtime_error("Iteration src error " + std::to_string(segid)
//                  + " " + std::to_string(localid)
//                  + " " + std::to_string(data.size())
//                  + " " + std::to_string(data[segid].offset_list_.size()));
//            }
            uint64_t ss =
                (localid == 0 ? 0 : data[segid].offset_list_[localid - 1]);
            uint64_t ee = data[segid].offset_list_.at(localid);
            auto r = (*compute[segid].current_).at(localid);
            auto w = graph_->GetWeight(segid, localid);
            if (ee > ss && r > 0 && w != 0) {
              rankbydegree = r / w;
              for (uint64_t s = ss; s < ee; ++s) {
                auto &v = data[segid].adj_list_.at(s);
                // edges++;
                uint64_t segid2 = v.first >> SegmentBits;
                uint64_t localid2 = v.first & SegmentMask;
  //              if (UNLIKELY(segid2 >= data.size() ||
  //                  localid2 >= data[segid2].offset_list_.size())) {
  //                throw std::runtime_error("Iteration tgt error " + std::to_string(segid2)
  //                    + " " + std::to_string(localid2)
  //                    + " " + std::to_string(data.size())
  //                    + " " + std::to_string(data[segid2].offset_list_.size()));
  //              }
                thread_local_buffer[segid2].push_back(
                    std::make_pair(localid2, v.second * rankbydegree));
                if (++count >= 100000) {
                  for (auto i = 0; i < thread_local_buffer.size(); ++i) {
                    if (thread_local_buffer[i].size() > 0) {
//                      compute[i].lock_->lock();
                      for (auto &pair : thread_local_buffer[i]) {
                        (*compute[i].prior_).at(pair.first) += pair.second;
                      }
//                      compute[i].lock_->unlock();
                      thread_local_buffer[i].clear();
                    }
                  }
                  count = 0;
                }
              }
            }
          }
        }
      }
      for (auto i = 0; i < thread_local_buffer.size(); ++i) {
        if (thread_local_buffer[i].size() > 0) {
//          compute[i].lock_->lock();
          for (auto &pair : thread_local_buffer[i]) {
            (*compute[i].prior_).at(pair.first) += pair.second;
          }
//          compute[i].lock_->unlock();
          thread_local_buffer[i].clear();
        }
      }
    }
  }

/*  bool FinishOnePersonalizedPRIteration(float damping, float maxChange) {
    bool finish = true;
    for (auto& v : next_set_) {
      uint64_t h = v >> SegmentBits;
      uint64_t i = v & SegmentMask;
      auto &d = compute[h];
      if (d.prior_) {
        float &p = (*d.prior_).at(i), &c = (*d.current_).at(i);
        if (p != 0) {
          if (init_vertex_set_.find(v) != init_vertex_set_.end())
            p = (1 - damping) + damping * p;
          else
            p = damping * p;
//          if (finish && abs(p - c) > maxChange) {
//            finish = false;
//          }
        } else if (c != 0) {
          p = 1 - damping;
        }
        c = 0;
      }
    }
    for (auto& v : init_vertex_set_) {
      uint64_t h = v >> SegmentBits;
      uint64_t i = v & SegmentMask;
      auto &d = compute[h];
      if (d.prior_) {
        float &p = (*d.prior_).at(i), &c = (*d.current_).at(i);
        if (p == 0 && c != 0) {
          p = 1 - damping;
          c = 0;
        }
      }
    }
    for (auto& comp : compute) {
      std::swap(comp.prior_, comp.current_);
    }
    LOG(INFO) << "Finish one iteration " << start_set_.size() 
        << " " << next_set_.size(); 
    start_set_.swap(next_set_);
    next_set_.clear();
    return finish;
  }

  float GetRank(uint64_t vid) {
    auto segid = vid >> SegmentBits;
    auto localid = vid & SegmentMask;
    return (*compute[segid].current_).at(localid);
  }
*/
private:
  std::vector<Compute> compute; // per segment
  std::unordered_set<uint64_t> start_set_;
  std::unordered_set<uint64_t> next_set_;
  bool sparse_ = true;
  Graph *graph_;
};

#endif /* EXPRUTIL_HPP_ */
