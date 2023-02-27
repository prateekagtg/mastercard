/******************************************************************************
 * Copyright (c) 2016, TigerGraph Inc.
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
#include <memory>
#include <mutex>
#include <stdexcept>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <unistd.h>
#include <atomic>

typedef std::string string; // XXX DON'T REMOVE

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
  std::vector<int64_t> adj_list_;
  // edges for i:  [offset[i-1]|0 , offset[i])
  // degree for i: offset[i] - offset[i-1]|0
  std::vector<int64_t> offset_list_;
  SegmentCSR() noexcept = default;
  SegmentCSR(SegmentCSR &&) noexcept = default;
};

struct WeightedSegmentCSR {
  // src -> <tgt,weight> adjcent list
  std::vector<std::pair<int64_t, float>> adj_list_;
  // edges for i:  [offset[i-1]|0 , offset[i])
  // degree for i: offset[i] - offset[i-1]|0
  // weight for i: weight[i]
  std::vector<int64_t> offset_list_;
  std::vector<float> weight_list_;
  WeightedSegmentCSR() noexcept = default;
  WeightedSegmentCSR(WeightedSegmentCSR &&) noexcept = default;
};

// Graph/Edge data
struct Graph {
public:
  Graph(size_t segsize)
      : data_(segsize), wdata_(segsize), max_vids_(segsize),
        totalSeg_(segsize) {}

  void Initialize(const EngineServiceRequest &request,
                  const ListAccum<uint64_t> &vertices,
                  const ListAccum<string> &edge_list) {
    {
      uint64_t current =
          std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::system_clock::now().time_since_epoch())
              .count();
      std::string rid = request.requestid_.str();
      std::lock_guard<std::mutex> lock(graph_mutex_);
      // already initialized
      if (request.timeout_ts_ == timeout_ && rid == requestid_) {
	return;
      }
      // no query running or previous query timed out
      if (!running_ || current > timeout_ + 5000) {
        auto meta = request.GetSegmentMeta();
        if (!meta)
          throw std::runtime_error("Segment meta is not ready");
        timeout_ = request.timeout_ts_;
        running_ = true;
        requestid_ = rid;
        totalSeg_ = meta->size();
      } else {
        throw std::runtime_error("Another query running, abort..");
      }
    }
    ResetGraph(totalSeg_);
    std::cout << "Graph INFO: " << vertices.size() 
              << " " << edge_list.size() << "\n";    
    for (auto &v : vertices)
      Fill(v);
    for (auto &e : edge_list) {
      char *end;
      uint64_t src = std::strtoll(e.c_str(), &end, 10);
      uint64_t tgt = std::strtoll(end, &end, 10);
      float weight = std::atof(end);
      Fill(src, tgt, weight);
    }
  }

  void Initialize(const std::string &request,
                  const ListAccum<uint64_t> &vertices,
		  const ListAccum<uint64_t> &src,
		  const ListAccum<uint64_t> &tgt,
		  const ListAccum<float> &weight) {
    {
      char *end;
      int64_t timeout_ms = std::strtoll(request.c_str(), &end, 10);
      int64_t segSize = std::strtoll(end, &end, 10);
      string rid(end + 1, request.size() - (end - request.c_str()));

      uint64_t current =
          std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::system_clock::now().time_since_epoch())
              .count();
      std::lock_guard<std::mutex> lock(graph_mutex_);
      // already initialized
      if (timeout_ms == timeout_ && rid == requestid_) {
        return;
      }
      // no query running or previous query timed out
      if (!running_ || current > timeout_ + 5000) {
        std::cout << "Request INFO: " << request << "\n";             
        timeout_ = timeout_ms;
        running_ = true;
        requestid_ = rid;
        totalSeg_ = segSize;
      } else {
        throw std::runtime_error("Another query running, abort..");
      }
    }
    ResetGraph(totalSeg_);
    std::cout << "Graph INFO: " << vertices.size()
              << " " << src.size() 
	      << " " << tgt.size() 
	      << " " << weight.size() 
	      << " " << totalSeg_ << "\n";
    size_t size = src.size();
    if (tgt.size() != size || weight.size() != size) {
      throw std::runtime_error("Edge has inconsistency, abort..");	    
    }
    for (auto &v : vertices)
      Fill(v);

    for (auto i=0; i<size; ++i) {
      Fill(src.data_[i], tgt.data_[i], weight.data_[i]);
    }
  }

  void Initialize(const std::string &request,
                  const ListAccum<uint64_t> &vertices,
                  const MapAccum<uint64_t, ListAccum<uint64_t>> &src,
                  const MapAccum<uint64_t, ListAccum<uint64_t>> &tgt,
                  const MapAccum<uint64_t, ListAccum<float>> &weight) {
    {
      char *end;
      int64_t timeout_ms = std::strtoll(request.c_str(), &end, 10);
      int64_t segSize = std::strtoll(end, &end, 10);
      string rid(end + 1, request.size() - (end - request.c_str()));

      uint64_t current =
          std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::system_clock::now().time_since_epoch())
              .count();
      std::lock_guard<std::mutex> lock(graph_mutex_);
      // already initialized
      if (timeout_ms == timeout_ && rid == requestid_) {
        return;
      }
      // no query running or previous query timed out
      if (!running_ || current > timeout_ + 5000) {
        std::cout << "Request INFO: " << request << "\n";
        timeout_ = timeout_ms;
        running_ = true;
        requestid_ = rid;
        totalSeg_ = segSize;
      } else {
        throw std::runtime_error("Another query running, abort..");
      }
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
    std::cout << "Graph creation done \n";
  }

  void Clear(const EngineServiceRequest &request) {
    {
      std::lock_guard<std::mutex> lock(graph_mutex_);
      if (!running_ || request.timeout_ts_ != timeout_ ||
          request.requestid_.str() != requestid_) {
        // Someone else already had access, likely
        // due to this query already timed out
        return;
      }
      ResetGraph(totalSeg_);
      running_ = false;
    }
  }

  void Clear(const std::string &request) {
    {
      char *end;
      int64_t timeout_ms = std::strtoll(request.c_str(), &end, 10);
      int64_t segSize = std::strtoll(end, &end, 10);
      string rid(end + 1, request.size() - (end - request.c_str()));
      std::lock_guard<std::mutex> lock(graph_mutex_);
      if (!running_ || timeout_ms != timeout_ || rid != requestid_) {
        // Someone else already had access, likely
        // due to this query already timed out
        return;
      }
      ResetGraph(totalSeg_);
      running_ = false;
    }
    std::cout << "Clear graph done\n";
  }

  void ResetGraph(size_t segsize) {
    data_.clear();
    data_.resize(segsize);
    wdata_.clear();
    wdata_.resize(segsize);
    max_vids_.clear();
    max_vids_.resize(segsize);
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
    if (localid >= data[segid].offset_list_.size())
      return 0;
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
    if (localid >= wdata_[segid].weight_list_.size())
      return 0;
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
    uint32_t segid = src >> SegmentBits;
    uint32_t localid = src & SegmentMask;
    if (wdata_[segid].offset_list_.size() <= localid) {
      size_t edge = wdata_[segid].adj_list_.size();
      wdata_[segid].offset_list_.resize(localid + 1, edge);
      wdata_[segid].weight_list_.resize(localid + 1, 0);
    }
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
  std::atomic<size_t> totalSeg_{10};
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

  void InitializeCompute(Graph &graph) {
    graph_ = &graph;
    compute.clear();
    compute.resize(graph.GetSegSize());
    for (size_t i = 0; i < graph.GetSegSize(); ++i) {
      compute[i].rank_ =
          std::make_unique<std::vector<float>>(graph.GetMaxVid(i) + 1, 1);
      compute[i].current_ = compute[i].rank_.get();
      compute[i].rank2_ =
          std::make_unique<std::vector<float>>(graph.GetMaxVid(i) + 1, 0);
      compute[i].prior_ = compute[i].rank2_.get();
    }
  }

  void InitializeCompute(Graph &graph, const ListAccum<uint64_t> &input) {
    graph_ = &graph;
    compute.clear();
    compute.resize(graph.GetSegSize());
    init_vertex_set_.resize(graph.GetSegSize());
    for (size_t i = 0; i < graph.GetSegSize(); ++i) {
      compute[i].rank_ =
          std::make_unique<std::vector<float>>(graph.GetMaxVid(i) + 1, 0);
      compute[i].current_ = compute[i].rank_.get();
      compute[i].rank2_ =
          std::make_unique<std::vector<float>>(graph.GetMaxVid(i) + 1, 0);
      compute[i].prior_ = compute[i].rank2_.get();
    }
    for (auto v : input.data_) {
      uint64_t segid = v >> SegmentBits;
      uint64_t localid = v & SegmentMask;
      if (compute[segid].rank_) {
        // Just be cautious. This shouldn't happen.
        if (localid >= compute[segid].rank_->size()) {
          compute[segid].rank_->resize(localid + 1);
          compute[segid].rank2_->resize(localid + 1);
        }
        compute[segid].rank_->at(localid) = 1;
        init_set_.push_back(std::make_pair(segid, localid));
        init_vertex_set_[segid].insert(localid);
      }
    }
  }

  MapAccum<uint64_t, float> Retrieve(const ListAccum<uint64_t> &output_list) {
    MapAccum<uint64_t, float> output;
    for (auto &vid : output_list.data_) {
      output.data_[vid] = GetRank(vid);
    }
    return output;
  }

  void RunOneIteration(bool weight) {
    if (weight) {
      ProcessWeightedPageRank(ScheduleEdges(graph_->GetWeightedData()));
    } else {
      ProcessPageRank(ScheduleEdges(graph_->GetData()));
    }
  }

  template <class Data>
  std::vector<std::pair<uint64_t, uint64_t>> ScheduleEdges(const Data &data) {

    // Assign edges to each thread
    std::vector<std::pair<uint64_t, uint64_t>> assignment;
    assignment.push_back(std::make_pair(
        data.size() - 1, data[data.size() - 1].offset_list_.size() - 1));

    /*    uint64_t beg =
       std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        int64_t total = 0;
        for (size_t segid=0; segid<data.size(); ++segid) {
          total += data[segid].adj_list_.size();
        }
        size_t segid = 0, sum = 0, avg = total / thread_num_, partial_seg = -1,
       lastvid = -1; while (segid < data.size()) {
          // First evaluate if the current_ segment can be all (or remaining
       edges) included size_t remaining = data[segid].adj_list_.size(); if
       (partial_seg == segid) { remaining = data[segid].adj_list_.size() *
       (data[segid].offset_list_.size() - lastvid - 1) /
       data[segid].offset_list_.size();
          }
          if (sum + remaining >= avg) {
            // segment can provide edges that exceed average
            int64_t partial = avg - sum;
            int64_t split = partial * data[segid].offset_list_.size() /
       data[segid].adj_list_.size(); if (split >=
       data[segid].offset_list_.size()) { split =
       data[segid].offset_list_.size() - 1;
            }
            std::cout << "Assignment " << segid << "|" << split << "|" <<
       data[segid].offset_list_.size()
                      << " edges " << partial << "|" <<
       data[segid].adj_list_.size()
                      << "|" << assignment.size() << "\n";
            assignment.push_back(std::make_pair(segid, split));
            sum = 0;
            // Last thread left
            if (assignment.size() == thread_num_ - 1) {
              assignment.push_back(std::make_pair(data.size()-1,
       data[data.size()-1].offset_list_.size()-1)); break;
            }
            if (split != data[segid].offset_list_.size() - 1) {
              partial_seg = segid;
              lastvid = split;
            } else {
              ++segid;
            }
          } else {
            // take all the remaining edges;
            sum += remaining;
            ++segid;
          }
        }
        if (assignment.size() < thread_num_) {
          assignment.push_back(std::make_pair(data.size()-1,
       data[data.size()-1].offset_list_.size()-1)); std::cout << "Assignment
       last " << (data.size()-1) << "|"
                    << (data[data.size()-1].offset_list_.size()-1) << "\n";
        }

        std::cout << "Prepare time " <<
       (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()
       - beg) << " ms \n"; */
    return assignment;
  }

  void ProcessPageRank(std::vector<std::pair<uint64_t, uint64_t>> assignment) {
    const auto &data = graph_->GetData();
    if (firstIter) {
      // Optimize for first iteration
//      uint64_t ms = std::chrono::duration_cast<std::chrono::milliseconds>(
//                        std::chrono::system_clock::now().time_since_epoch())
//                        .count();
      uint64_t edges = 0;
      for (auto &[segid, localid] : init_set_) {
        uint64_t ss = (localid == 0 ? 0 : data[segid].offset_list_[localid - 1]);
        uint64_t ee = data[segid].offset_list_[localid];
        if (ee > ss) {
          float rankbydegree =
              (*compute[segid].current_)[localid] /
              graph_->GetDegree(segid, localid, graph_->GetData());
          for (uint64_t s = ss; s < ee; ++s) {
            auto &v = data[segid].adj_list_[s];
            edges++;
            uint64_t segid2 = v >> SegmentBits;
            uint64_t localid2 = v & SegmentMask;
            compute[segid2].lock_->lock();
            (*compute[segid2].prior_)[localid2] += rankbydegree;
            compute[segid2].lock_->unlock();
          }
        }
      }
      firstIter = false;
      // uint64_t t = std::chrono::duration_cast<std::chrono::milliseconds>(
      //                 std::chrono::system_clock::now().time_since_epoch())
      //                 .count() - ms;
      //      std::cout << " Thread processed personalized weighted " << edges
      //      << " in "
      //                << t << " ms\n";
      return;
    }
    //    omp_set_num_threads(std::min(thread_num_, assignment.size()));
    // #pragma omp parallel
    {
      uint64_t ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now().time_since_epoch())
                        .count();
      size_t id = 0; // omp_get_thread_num();
      std::vector<std::vector<std::pair<uint64_t, float>>> thread_local_buffer(
          graph_->GetSegSize());
      size_t count = 0;
      // when id == 0, start_localvid will be +1 in the loop
      uint64_t start_segid = 0, start_localvid = -1, end_segid = -1,
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
          uint64_t start = 0, end = data[segid].offset_list_.size() - 1;
          if (segid == start_segid)
            start = start_localvid + 1; // exclusive
          if (segid == end_segid)
            end = end_localvid; // inclusive

          for (auto localid = start; localid <= end; ++localid) {
            uint64_t ss =
                (localid == 0 ? 0 : data[segid].offset_list_[localid - 1]);
            uint64_t ee = data[segid].offset_list_[localid];
            if (ee > ss) {
              rankbydegree =
                  (*compute[segid].current_)[localid] /
                  graph_->GetDegree(segid, localid, graph_->GetData());
              for (uint64_t s = ss; s < ee; ++s) {
                auto &v = data[segid].adj_list_[s];
                edges++;
                uint64_t segid2 = v >> SegmentBits;
                uint64_t localid2 = v & SegmentMask;
                thread_local_buffer[segid2].push_back(
                    std::make_pair(localid2, rankbydegree));
                if (++count >= 100000) {
                  for (auto i = 0; i < thread_local_buffer.size(); ++i) {
                    if (thread_local_buffer[i].size() > 0) {
                      compute[i].lock_->lock();
                      for (auto &pair : thread_local_buffer[i]) {
                        (*compute[i].prior_)[pair.first] += pair.second;
                      }
                      compute[i].lock_->unlock();
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
          compute[i].lock_->lock();
          for (auto &pair : thread_local_buffer[i]) {
            (*compute[i].prior_)[pair.first] += pair.second;
          }
          compute[i].lock_->unlock();
          thread_local_buffer[i].clear();
        }
      }
      // uint64_t t = std::chrono::duration_cast<std::chrono::milliseconds>(
      //                 std::chrono::system_clock::now().time_since_epoch())
      //                 .count() - ms;
      //      std::cout << " Thread " << id << " processed unweight " << edges
      //      << " in "
      //                << t << " ms\n";
    }
  }

  void
  ProcessWeightedPageRank(std::vector<std::pair<uint64_t, uint64_t>> assignment) {
    const auto &data = graph_->GetWeightedData();
    if (firstIter) {
      // Optimize for first iteration
      uint64_t ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now().time_since_epoch())
                        .count();
      uint64_t edges = 0;
      for (auto &[segid, localid] : init_set_) {
        uint64_t ss = (localid == 0 ? 0 : data[segid].offset_list_[localid - 1]);
        uint64_t ee = data[segid].offset_list_[localid];
        auto r = (*compute[segid].current_)[localid];
        auto w = graph_->GetWeight(segid, localid);
        if (ee > ss && r > 0 && w != 0) {
          float rankbydegree = r / w;
          for (uint64_t s = ss; s < ee; ++s) {
            auto &v = data[segid].adj_list_[s];
            edges++;
            uint64_t segid2 = v.first >> SegmentBits;
            uint64_t localid2 = v.first & SegmentMask;
            compute[segid2].lock_->lock();
            (*compute[segid2].prior_)[localid2] += v.second * rankbydegree;
            compute[segid2].lock_->unlock();
          }
        }
      }
      firstIter = false;
      // uint64_t t = std::chrono::duration_cast<std::chrono::milliseconds>(
      //                 std::chrono::system_clock::now().time_since_epoch())
      //                 .count() - ms;
      //      std::cout << " Thread processed personalized weighted " << edges
      //      << " in "
      //                << t << " ms\n";
      return;
    }

    //    omp_set_num_threads(std::min(thread_num_, assignment.size()));
    //    #pragma omp parallel
    {
      uint64_t ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now().time_since_epoch())
                        .count();
      size_t id = 0; // omp_get_thread_num();
      std::vector<std::vector<std::pair<uint64_t, float>>> thread_local_buffer(
          graph_->GetSegSize());
      size_t count = 0;
      // when id == 0, start_localvid will be +1 in the loop
      uint64_t start_segid = 0, start_localvid = -1, end_segid = -1,
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
          uint64_t start = 0, end = data[segid].offset_list_.size() - 1;
          if (segid == start_segid)
            start = start_localvid + 1; // exclusive
          if (segid == end_segid)
            end = end_localvid; // inclusive

          for (auto localid = start; localid <= end; ++localid) {
            uint64_t ss =
                (localid == 0 ? 0 : data[segid].offset_list_[localid - 1]);
            uint64_t ee = data[segid].offset_list_[localid];
            auto r = (*compute[segid].current_)[localid];
            auto w = graph_->GetWeight(segid, localid);
            if (ee > ss && r > 0 && w != 0) {
              rankbydegree = r / w;
              for (uint64_t s = ss; s < ee; ++s) {
                auto &v = data[segid].adj_list_[s];
                edges++;
                uint64_t segid2 = v.first >> SegmentBits;
                uint64_t localid2 = v.first & SegmentMask;
                thread_local_buffer[segid2].push_back(
                    std::make_pair(localid2, v.second * rankbydegree));
                if (++count >= 100000) {
                  for (auto i = 0; i < thread_local_buffer.size(); ++i) {
                    if (thread_local_buffer[i].size() > 0) {
                      compute[i].lock_->lock();
                      for (auto &pair : thread_local_buffer[i]) {
                        (*compute[i].prior_)[pair.first] += pair.second;
                      }
                      compute[i].lock_->unlock();
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
          compute[i].lock_->lock();
          for (auto &pair : thread_local_buffer[i]) {
            (*compute[i].prior_)[pair.first] += pair.second;
          }
          compute[i].lock_->unlock();
          thread_local_buffer[i].clear();
        }
      }
      // uint64_t t = std::chrono::duration_cast<std::chrono::milliseconds>(
      //                 std::chrono::system_clock::now().time_since_epoch())
      //                 .count() - ms;
      //      std::cout << " Thread " << id << " processed " << edges << " in "
      //      << t
      //                << " ms\n";
    }
  }

  bool FinishOneIteration(float damping, float maxChange) {
    bool finish = true;
    {
      //    omp_set_num_threads(thread_num_);
      //  #pragma omp parallel for schedule(static, 10)
      for (size_t h = 0; h < compute.size(); ++h) {
        auto &d = compute[h];
        if (d.prior_) {
          for (size_t i = 0; i < d.prior_->size(); ++i) {
            float &p = (*d.prior_)[i], &c = (*d.current_)[i];
            p = (1 - damping) + damping * p;
            if (finish && abs(p - c) > maxChange) {
              finish = false;
            }
            c = 0;
          }
          std::swap(d.prior_, d.current_);
        }
      }
    }
    return finish;
  }

  bool FinishOnePersonalizedPRIteration(float damping, float maxChange) {
    bool finish = true;
    {
      //    omp_set_num_threads(thread_num_);
      // #pragma omp parallel for schedule(static, 10)
      for (size_t h = 0; h < compute.size(); ++h) {
        auto &d = compute[h];
        if (d.prior_) {
          for (size_t i = 0; i < d.prior_->size(); ++i) {
            float &p = (*d.prior_)[i], &c = (*d.current_)[i];
            if (p != 0) {
              if (init_vertex_set_[h].find(i) != init_vertex_set_[h].end())
                p = (1 - damping) + damping * p;
              else
                p = damping * p;
              if (finish && abs(p - c) > maxChange) {
                finish = false;
              }
            } else if (c != 0 && init_vertex_set_[h].find(i) !=
                                 init_vertex_set_[h].end()) {
              p = 1 - damping;
            }
            c = 0;
          }
          std::swap(d.prior_, d.current_);
        }
      }
    }
    return finish;
  }

  float GetRank(uint64_t vid) {
    auto segid = vid >> SegmentBits;
    auto localid = vid & SegmentMask;
    return (*compute[segid].current_)[localid];
  }

private:
  std::vector<Compute> compute; // per segment
  std::vector<std::pair<uint64_t, uint64_t>> init_set_;
  std::vector<std::unordered_set<uint32_t>> init_vertex_set_;
  bool firstIter = true;
  Graph *graph_;
};

#endif /* EXPRUTIL_HPP_ */
