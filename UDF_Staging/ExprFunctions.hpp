/******************************************************************************
* Copyright (c) 2015-2016, TigerGraph Inc.
* All rights reserved.
* Project: TigerGraph Query Language
* udf.hpp: a library of user defined functions used in queries.
*
* - This library should only define functions that will be used in
*   TigerGraph Query scripts. Other logics, such as structs and helper
*   functions that will not be directly called in the GQuery scripts,
*   must be put into "ExprUtil.hpp" under the same directory where
*   this file is located.
*
* - Supported type of return value and parameters
*     - int
*     - float
*     - double
*     - bool
*     - string (don't use std::string)
*     - accumulators
*
* - Function names are case sensitive, unique, and can't be conflict with
*   built-in math functions and reserve keywords.
*
* - Please don't remove necessary codes in this file
*
* - A backup of this file can be retrieved at
*     <tigergraph_root_path>/dev_<backup_time>/gdk/gsql/src/QueryUdf/ExprFunctions.hpp
*   after upgrading the system.
*
******************************************************************************/
/*Added on 02/15/2023 For TG UDF for w PPR*************************************************************************************************/
#ifndef EXPRFUNCTIONS_HPP_
#define EXPRFUNCTIONS_HPP_

#include <chrono>
#include <cmath>
#include <ctime>
#include <omp.h>
/*******************************************************************************************************/

#include <stdlib.h>
#include <stdio.h>
#include <string>
#include <gle/engine/cpplib/headers.hpp>

#include "ExprUtil.hpp"
/**     XXX Warning!! Put self-defined struct in ExprUtil.hpp **
*  No user defined struct, helper functions (that will not be directly called
*  in the GQuery scripts) etc. are allowed in this file. This file only
*  contains user-defined expression function's signature and body.
*  Please put user defined structs, helper functions etc. in ExprUtil.hpp
*/
#include <iostream>
#include <dlfcn.h>
#include <vector>
#include <cstdio>


namespace UDIMPL {
  typedef std::string string; //XXX DON'T REMOVE

  /****** BIULT-IN FUNCTIONS **************/
  /****** XXX DON'T REMOVE ****************/
  inline int64_t str_to_int (string str) {
      return atoll(str.c_str());
  }

  inline int64_t float_to_int (float val) {
      return (int64_t) val;
  }

  inline string to_string (double val) {
      char result[200];
      sprintf(result, "%g", val);
      return string(result);
  }

  /*Added on 02/15/2023 functions defined outside namespace UDIMPL******************************************************************************************/
  /*Added on 02/15/2023 for fastRP*****************************************************/
  inline float str_to_float(std::string input) {
	  return std::stof(input);
	}
  /***********************************************************************************/


  inline std::string float_to_string(ListAccum<double> val)
  {
      std::ostringstream ss;
      ss << val;
      return ss.str();
  }

  template<typename T> inline std::string stringify(T val)
  {
      std::ostringstream ss;
      ss << val;
      return ss.str();
  }
  inline int write_to_kafka(std::string brokerAddress, std::string topic, std::string key, std::string message, std::string securityProtocol, std::string saslMechanism, std::string saslUsername, std::string saslPassword, std::string sslCALocation)
  {
      std::cerr << "Please upgrade to use Kafka streaming function." << std::endl;
      return 777;
  }
  /*******************************************************************************************************/

  /*Added on 02/15/2023 TG UDF for w PPR*******************************************************************************/
  template <typename T> inline std::string to_string(T val) {
    return std::to_string(val);
  }

  /*
    Print double/float in %g format because to_string function
    does not work for small/large/mixed values, e.g.:
      to_string(1E-9) -> 0.0000000
      to_string(1E11) -> 100000000000
      to_string(1000.1) -> 1000.099997
    Recommended Buffer Sizes
    | Single| Double | Extended | Quad  |
    |:-----:|:------:|:--------:|:-----:|
    |   16  |  24    |    30    |  45   |
  */

  template <> inline std::string to_string(double val) {
    char result[25];
    int len = snprintf(result, 25, "%g", val);
    return std::string(result, len);
  }

  template <> inline std::string to_string(float val) {
    char result[17];
    int len = snprintf(result, 17, "%g", val);
    return std::string(result, len);
  }

  template <> inline std::string to_string(bool val) {
    return val ? "true" : "false";
  }

  /*** start of matrix like implementation ***/
  /*** ----------------------------------- ***/
  /*** ----------------------------------- ***/
  /*** ----------------------------------- ***/
  /*** ----------------------------------- ***/

  class GraphInstance {
   public:
    static Graph& GetInstance() {
      static Graph instance(10);
      return instance;
    }
   private:
    GraphInstance() {}
    GraphInstance(GraphInstance const&) = delete;
    void operator=(GraphInstance const&)  = delete;
  };

  class PageRankInstance {
   public:
    static PageRank& GetInstance() {
      static PageRank instance;
      return instance;
    }
   private:
    PageRankInstance() {}
    PageRankInstance(PageRankInstance const&) = delete;
    void operator=(PageRankInstance const&)  = delete;
  };

  inline void InitializeGraph(const EngineServiceRequest &request,
                       const ListAccum<uint64_t> &vertices,
                       const ListAccum<string> &edge_list) {
    GraphInstance::GetInstance().Initialize(request, vertices, edge_list);
  }

  inline void InitializeGraph(const std::string &request,
      const ListAccum<uint64_t> &vertices,
      const MapAccum<uint64_t, ListAccum<uint64_t>> &src,
      const MapAccum<uint64_t, ListAccum<uint64_t>> &tgt,
      const MapAccum<uint64_t, ListAccum<float>> &weight) {
    GraphInstance::GetInstance().Initialize(request, vertices, src, tgt, weight);
  }

  inline void InitializeGraph(const std::string &request,
      const ListAccum<uint64_t> &vertices,
      const ListAccum<uint64_t> &src,
      const ListAccum<uint64_t> &tgt,
      const ListAccum<float> &weight) {
    GraphInstance::GetInstance().Initialize(request, vertices, src, tgt, weight);
  }

  inline void ClearGraph(const EngineServiceRequest &request) {
    GraphInstance::GetInstance().Clear(request);
  }

  inline void ClearGraph(const std::string &request) {
    GraphInstance::GetInstance().Clear(request);
  }

  inline std::string GetReq(const EngineServiceRequest &request) {
    return request.requestid_.str();
  }

  inline string GetRequest(const EngineServiceRequest &request) {
    auto meta = request.GetSegmentMeta();
    if (!meta)
      throw std::runtime_error("Segment meta is not ready");
    string output = std::to_string(request.timeout_ts_) + " " +
                    std::to_string(meta->size()) + " " + request.requestid_.str();
    return output;
  }

  inline int64_t GetDegree(uint64_t src, bool weight) {
    return GraphInstance::GetInstance().GetDegree(src, weight);
  }

  inline float GetWeight(uint64_t src) {
    return GraphInstance::GetInstance().GetWeight(src);
  }

  inline void FillVertex(uint64_t src) {
    GraphInstance::GetInstance().Fill(src);
  }

  inline void FillEdge(uint64_t src, uint64_t tgt) {
    GraphInstance::GetInstance().Fill(src, tgt);
  }

  inline void FillWeightedEdge(uint64_t src, uint64_t tgt, float weight) {
    GraphInstance::GetInstance().Fill(src, tgt, weight);
  }

  inline MapAccum<uint64_t, float>
  RunPersonalizedPageRank(bool weight, float damping, float maxChange,
                          int maxIter, const ListAccum<uint64_t> &init_list,
                          const ListAccum<uint64_t> &output_list) {
    PageRank pr;
    pr.InitializeCompute(GraphInstance::GetInstance(), init_list);
    for (int i = 0; i < maxIter; ++i) {
      pr.RunOneIteration(weight);
      pr.FinishOnePersonalizedPRIteration(damping, maxChange);
    }
    return pr.Retrieve(output_list);
  }

  inline void InitializePageRankCompute() {
    PageRankInstance::GetInstance().InitializeCompute(GraphInstance::GetInstance());
  }

  inline bool FinishOnePageRankIteration(float damping, float maxChange) {
    return PageRankInstance::GetInstance().FinishOneIteration(damping, maxChange);
  }

  inline void RunOnePageRankIteration(bool weight) {
    PageRankInstance::GetInstance().RunOneIteration(weight);
  }

  inline float GetRank(int64_t t) {
    return PageRankInstance::GetInstance().GetRank(t);
  }

  inline float Diff(int64_t vid, SumAccum<float> res) {
    float rank = PageRankInstance::GetInstance().GetRank(vid);
    float r = (res == 0 ? rank : std::abs(1 - rank / res));
    if (r > 0.001) {
      std::string o = "Anomaly: " + std::to_string(vid) + "|" +
                      std::to_string(res) + "|" + std::to_string(rank) + "\n";
      std::cout << o;
    }
    return r;
  }
  /*** ----------------------------------- ***/
  /*** ----------------------------------- ***/
  /*** ----------------------------------- ***/
  /*** ----------------------------------- ***/
  /*** end of matrix like implementation   ***/
  /*******************************************************************************************************/
  inline void *getDynamicFunction(const std::string &funcName) {
      std::string SOFILEPATH = "libzetta_accel.so";
      void* handle = dlopen(SOFILEPATH.c_str(), RTLD_LAZY | RTLD_NOLOAD);
      if (handle == nullptr) {
          std::cout << "ZettaBolt: " << SOFILEPATH << " not loaded. Loading now..." << std::endl;
          handle = dlopen(SOFILEPATH.c_str(), RTLD_LAZY | RTLD_GLOBAL);
          std::cout << "ZettaBolt: after dlopen" << std::endl;
          if (handle == nullptr) {
              std::cout << "ZettaBolt: inside handle==nullptr" << std::endl;
              std::cout << "ZettaBolt :Cannot open library " << SOFILEPATH << ": " << dlerror()
                        << ".  Please ensure that the library's path is in LD_LIBRARY_PATH."  << std::endl;
              std::cout << "ZettaBolt: after oss filling" << std::endl;
          }
      }
      dlerror();
      void *pFunc = dlsym(handle, funcName.c_str());
      const char* dlsym_error2 = dlerror();
      if (dlsym_error2) {
          std::cout << "ZettaBolt: Cannot load symbol '" << funcName << "': " << dlsym_error2
                    << ".  Possibly an older version of library " << SOFILEPATH
                    << " is in use.  Please install the correct version." << std::endl;
          std::cout << "ZettaBolt: after 2nd oss filling" << std::endl;
      }
      std::cout << "ZettaBolt: before return" << std::endl;
      return pFunc;
  }

  inline uint64_t udf_zetta_start_timer(){
      typedef uint64_t (*get_zetta_timer)();
      static  get_zetta_timer pCreateFunc = nullptr;
      if(pCreateFunc == nullptr)
          pCreateFunc = (get_zetta_timer) getDynamicFunction("get_zetta_timer");
      return pCreateFunc();
  }

  inline float udf_zetta_elapsed_time(uint64_t timer){
      typedef float (*zetta_elapsed_time_secs)(uint64_t);
      static  zetta_elapsed_time_secs pCreateFunc = nullptr;
      if(pCreateFunc == nullptr)
          pCreateFunc = (zetta_elapsed_time_secs) getDynamicFunction("zetta_elapsed_time_secs");
      return pCreateFunc(timer);
  }

  inline bool udf_zetta_pre_processing_done(){
      typedef bool (*zetta_pre_processing_done)();
      static  zetta_pre_processing_done pCreateFunc = nullptr;
      if(pCreateFunc == nullptr)
          pCreateFunc = (zetta_pre_processing_done) getDynamicFunction("zetta_pre_processing_done");

      return pCreateFunc();
  }

  inline ListAccum <float> udf_zetta_pagerank(ListAccum<uint64_t> sourceID, float alpha, float tolerance, int maxIter,
                                              ListAccum<uint64_t> valid_merchant_ids){
      ListAccum <float> pagerank;
      std::vector<float> pagerank_f;
      pagerank_f.resize(valid_merchant_ids.size());
      typedef bool (*zetta_pagerank)(uint64_t , uint64_t *, float ,float ,int ,uint64_t , uint64_t *,
                                     float *);
      static zetta_pagerank pCreateFunc = nullptr;
      if(pCreateFunc == nullptr)
          pCreateFunc = (zetta_pagerank) getDynamicFunction("zetta_pagerank");

      bool out = pCreateFunc( sourceID.size(),sourceID.data_.data(), alpha, tolerance,
                              maxIter,valid_merchant_ids.size(),valid_merchant_ids.data_.data(),pagerank_f.data());

      if(out == false)
          std::cout<<"ZettaBolt: zetta_pre_processing not done"<<'\n';

      for(auto x:pagerank_f){
          pagerank+=x;
      }
      return pagerank;
  }

  inline bool udf_zetta_pre_processing(uint64_t total_vertices,uint64_t total_edges,ListAccum <uint64_t> rows ,
                                       ListAccum <uint64_t> columns,ListAccum<float> weigths,
                                       ListAccum<uint64_t> v_ids) {
      typedef bool (*udf_amd_load_coo)(uint64_t , uint64_t *, uint64_t , uint64_t *,
                                       uint64_t *, float *);
      udf_amd_load_coo pCreateFunc = (udf_amd_load_coo) getDynamicFunction("zetta_pre_processing");

      return pCreateFunc( total_vertices,v_ids.data_.data(), total_edges, rows.data_.data(),
                          columns.data_.data(), weigths.data_.data());
  }

  inline void udf_zetta_release_cache(){
      typedef void (*zetta_release_cache)();
      static  zetta_release_cache pCreateFunc = nullptr;
      if(pCreateFunc == nullptr)
          pCreateFunc = (zetta_release_cache) getDynamicFunction("zetta_release_cache");
      pCreateFunc();
  }

}// namespace UDIMPL

#endif /* EXPRFUNCTIONS_HPP_ */
