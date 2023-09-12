/******************************************************************************
* Copyright (c) 2015-2023, TigerGraph Inc.
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
#include <unordered_map>
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
#include <cstdlib>
#include <sstream>
#include <random>

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
/*
inline string to_string (double val) {
    char result[200];
    sprintf(result, "%g", val);
    return string(result);
}*/

/*Added on 06/22/2023 Egress to CEPH S3*/
inline int upload_s3(std::string file_path, std::string bucket, std::string object_path, int retry)
{
    static std::mutex upload_task_mutex;
    bool to_upload = false;
    int ret = 0;
    // Check whether the file is being uploaded by another thread
    upload_task_mutex.lock();
    if (GlobalStringSet::count(file_path) == 0) {
        // No. Upload the file and let others know.
        to_upload = true;
        GlobalStringSet::insert(file_path);
    }
    upload_task_mutex.unlock();

    if (to_upload) 
    {
        char current_dir[512];
        char hostname[256];
        std::array<char, 128> buffer;
        std::string response;
        std::ostringstream cmds;
        // Get the directory for binaries
        getcwd(current_dir, 512);
        // Get hostname
        gethostname(hostname, 256);
        // Assemble the command to call. Insert hostname to the file path to avoid overwriting.
        cmds << std::string(current_dir) << "/tg_infr_goblin s3 upload";
        cmds << " --access-key-entry System.Backup.S3.AWSAccessKeyID";
        cmds << " --secret-access-key-entry System.Backup.S3.AWSSecretAccessKey";
        cmds << " --endpoint-key-entry System.Backup.S3.Endpoint";
        cmds << " --bucket " << bucket;
        cmds << " --filename " << file_path;
        cmds << " --object ";
        std::size_t found=object_path.rfind("/");
        if (found!=std::string::npos) {
            cmds << object_path.substr(0, found+1) << std::string(hostname) << "/" << object_path.substr(found+1);
        }
        else {
            cmds << std::string(hostname) << "/" << object_path;
        }
        cmds << " 2>&1";
        std::string cmd = cmds.str();
        std::cout << cmd << std::endl;
        // Start uploading file. 
        int iteration = 0;
        do
        {
            // Run the command
            std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd.c_str(), "r"), pclose);
            if (!pipe) {
                throw std::runtime_error("popen() failed!");
            }
            // Get response
            response.clear();
            while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
                response += buffer.data();
            }
            if (response.find("{\"Code\":") == std::string::npos) 
            {
                std::cout << "Uploaded " << file_path << " successfully" << std::endl;
                ret = 0;
                break;
            }
            else {
                ret = 1;
                std::cout << "Error: " << response << std::endl;
                std::cout <<  retry - iteration << " retries left" << std::endl;
            }
            iteration++;
        } while (iteration<=retry);
    }

    return ret;
}

inline int cleanup_s3(std::string file_path, bool rm_file)
{
    static std::mutex clean_task_mutex;
    bool to_clean = false;
    int ret = 0;
    // Check whether the file is being cleaned up by another thread
    clean_task_mutex.lock();
    if (GlobalStringSet::count(file_path) > 0) {
        // No. Clean up the file and let others know.
        to_clean = true;
        GlobalStringSet::erase(file_path);
    }
    clean_task_mutex.unlock();

    if (to_clean) 
    {
        if (rm_file) 
        {
            ret = std::remove(file_path.c_str());
            if( ret != 0 )
                std::cerr << "Error deleting file " << file_path << std::endl;
            else
                std::cout << "Deleted " << file_path << std::endl;
        }
    }

    return ret;
}

inline int clear_global_string_set()
{
    GlobalStringSet::clear();
    return 0;
}


/***********************************************************************************/

/*Added on 05/15/2023 Functions for Random Walk*/

//random function, generate a random value between 0 and 1
inline float random(){
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(0, 1);
    return dis(gen);
}

// generate a int random value given a range
inline int64_t random_range(int64_t start, int64_t end){
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(start, end);
    return dis(gen);

}
// generate a random value based on probability distribution
// For example: given {0.5,0.3,0.2}, this function will generate {0,1,2} based on its probability
inline int random_distribution(ListAccum<float> p){
    std::vector<float> a;
    for (auto it : p.data_){
        a.push_back(it);
    }
    std::random_device rd;
    std::mt19937 gen(rd());
    std::discrete_distribution<> dis(a.begin(), a.end());
    return dis(gen);
}
/***********************************************************************************/

/*Added on 02/15/2023 functions defined outside namespace UDIMPL******************************************************************************************/
/*Added on 02/15/2023 for fastRP*****************************************************/
inline float str_to_float(std::string input) {
  return std::stof(input);
}
/***********************************************************************************/

/*Added on 03/10/2023 KafkaUDF*****************************************************/
template<typename T> inline std::string stringify(T val)
{
    std::ostringstream ss;
    ss << val;
    return ss.str();
}

inline int write_to_kafka(std::string brokerAddress, std::string topic, std::string key, std::string message, std::string securityProtocol, std::string saslMechanism, std::string saslUsername, std::string saslPassword, std::string sslCALocation)
{
    /*
     * Create configuration object
     */
    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

    std::string errstr;

    /* Set bootstrap broker(s) as a comma-separated list of
     * host or host:port (default port 9092).
     * librdkafka will use the bootstrap brokers to acquire the full
     * set of brokers from the cluster. */
    if (conf->set("bootstrap.servers", brokerAddress, errstr) !=
        RdKafka::Conf::CONF_OK)
    {
        std::cerr << errstr << std::endl;
        return 1;
    }

    /* Set max message size */
    if (conf->set("message.max.bytes", "100000000", errstr) !=
        RdKafka::Conf::CONF_OK)
    {
        std::cerr << errstr << std::endl;
        return 1;
    }

    /* Set credentials */
    if (!securityProtocol.empty()) {
        if (conf->set("security.protocol", securityProtocol, errstr) !=
            RdKafka::Conf::CONF_OK){
            std::cerr << errstr << std::endl;
            return 1;
        }
    }
    if (!saslMechanism.empty()) {
        if (conf->set("sasl.mechanism", saslMechanism, errstr) !=
            RdKafka::Conf::CONF_OK){
            std::cerr << errstr << std::endl;
            return 1;
        }
    }
    if (!saslUsername.empty()) {
        if (conf->set("sasl.username", saslUsername, errstr) !=
            RdKafka::Conf::CONF_OK){
            std::cerr << errstr << std::endl;
            return 1;
        }
    }
    if (!saslPassword.empty()) {
        if (conf->set("sasl.password", saslPassword, errstr) !=
            RdKafka::Conf::CONF_OK){
            std::cerr << errstr << std::endl;
            return 1;
        }
    }
    if (!sslCALocation.empty()) {
        if (conf->set("ssl.ca.location", sslCALocation, errstr) !=
            RdKafka::Conf::CONF_OK) {
            std::cerr << "sslError:" << errstr << std::endl;
            return 1;
        }
    }

    /* Set the delivery report callback.
     * This callback will be called once per message to inform
     * the application if delivery succeeded or failed.
     * See dr_msg_cb() above.
     * The callback is only triggered from ::poll() and ::flush().
     *
     * IMPORTANT:
     * Make sure the DeliveryReport instance outlives the Producer object,
     * either by putting it on the heap or as in this case as a stack variable
     * that will NOT go out of scope for the duration of the Producer object.
     */
    SimpleDeliveryReportCb simple_dr_cb;

    if (conf->set("dr_cb", &simple_dr_cb, errstr) != RdKafka::Conf::CONF_OK)
    {
        std::cerr << errstr << std::endl;
        return 1;
    }

    /*
     * Create producer instance.
     */
    RdKafka::Producer *producer = RdKafka::Producer::create(conf, errstr);
    if (!producer)
    {
        std::cerr << "Failed to create producer: " << errstr << std::endl;
        return 2;
    }

    delete conf;

    /*
     * Send/Produce message.
     * This is an asynchronous call, on success it will only
     * enqueue the message on the internal producer queue.
     * The actual delivery attempts to the broker are handled
     * by background threads.
     * The previously registered delivery report callback
     * is used to signal back to the application when the message
     * has been delivered (or failed permanently after retries).
     */
retry:
    RdKafka::ErrorCode err = producer->produce(
        /* Topic name */
        topic,
        /* Any Partition: the builtin partitioner will be
         * used to assign the message to a topic based
         * on the message key, or random partition if
         * the key is not set. */
        RdKafka::Topic::PARTITION_UA,
        /* Make a copy of the value */
        RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
        /* Value */
        const_cast<char *>(message.c_str()), message.size(),
        /* Key */
        key.empty() ? NULL : const_cast<char *>(key.c_str()), key.empty() ? 0 : key.size(),
        /* Timestamp (defaults to current time) */
        0,
        /* Message headers, if any */
        NULL,
        /* Per-message opaque value passed to
         * delivery report */
        NULL);

    if (err != RdKafka::ERR_NO_ERROR)
    {
        if (err == RdKafka::ERR__QUEUE_FULL)
        {
            /* If the internal queue is full, wait for
             * messages to be delivered and then retry.
             * The internal queue represents both
             * messages to be sent and messages that have
             * been sent or failed, awaiting their
             * delivery report callback to be called.
             *
             * The internal queue is limited by the
             * configuration property
             * queue.buffering.max.messages */
            producer->poll(1000 /*block for max 1000ms*/);
            goto retry;
        }
        std::cerr << "% Failed to produce to topic " << topic << ": "
                  << RdKafka::err2str(err) << std::endl;
        return 3;
    }
    else
    {
        // std::cerr << "% Enqueued message (" << message.size() << " bytes) "
        //           << "for topic " << topic << std::endl;
    }

    /* A producer application should continually serve
     * the delivery report queue by calling poll()
     * at frequent intervals.
     * Either put the poll call in your main loop, or in a
     * dedicated thread, or call it after every produce() call.
     * Just make sure that poll() is still called
     * during periods where you are not producing any messages
     * to make sure previously produced messages have their
     * delivery report callback served (and any other callbacks
     * you register). */
    producer->poll(0);

    /* Wait for final messages to be delivered or fail.
     * flush() is an abstraction over poll() which
     * waits for all messages to be delivered. */
    // std::cerr << "% Flushing final messages..." << std::endl;
    producer->flush(10 * 1000); /* wait for max 10 seconds */

    if (producer->outq_len() > 0) {
        std::cerr << "% " << producer->outq_len()
                  << " message(s) were not delivered" << std::endl;
        return 4;
    }

    delete producer;

    return simple_dr_cb.error_code;
}


/*******************************************************************************************************/

/*Added on 02/15/2023 TG UDF for w PPR*******************************************************************************/
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

/*** start of matrix like implementation ***/
/*** ----------------------------------- ***/
/*** ----------------------------------- ***/
/*** ----------------------------------- ***/
/*** ----------------------------------- ***/

class GraphInstance {
 public:
  static Graph& GetInstance(const std::string& req) {
    std::lock_guard<std::mutex> lock(instance_mutex_);
    for (auto it = instances_.begin(); it != instances_.end(); ) {
      char *end;
      int64_t timeout_ms = std::strtoll(it->first.c_str(), &end, 10);
      uint64_t ms = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch()).count();
      if (timeout_ms + 120000 < ms) {
        LOG(INFO) << "Remove timed out request after 2 mins " << it->first
	    << " Mutex " << &instance_mutex_;
        it = instances_.erase(it);
      } else {
        ++it;
      }
    }
    if (instances_.find(req) == instances_.end()) {
      if (instances_.size() >= 12) {
        throw std::runtime_error("More than 12 jobs running. Aborting...");
      } else {
        //LOG(INFO) << "Request " << req << " Mutex " << &instance_mutex_;
      }
    }
    return instances_[req];
  }

  static void Clear(const std::string& req) {
    std::lock_guard<std::mutex> lock(instance_mutex_);
    //LOG(INFO) << "Clear request " << req << " Mutex " << &instance_mutex_;
    if (instances_.find(req) != instances_.end()) {
      instances_.erase(req);
    }
  }

 private:
  inline static std::unordered_map<std::string, Graph> instances_;
  inline static std::mutex instance_mutex_;
  GraphInstance() {}
  GraphInstance(GraphInstance const&) = delete;
  void operator=(GraphInstance const&)  = delete;
};

inline void InitializeGraph(const std::string &request,
    const ListAccum<uint64_t> &vertices,
    const MapAccum<uint64_t, ListAccum<uint64_t>> &src,
    const MapAccum<uint64_t, ListAccum<uint64_t>> &tgt,
    const MapAccum<uint64_t, ListAccum<float>> &weight) {
  GraphInstance::GetInstance(request).Initialize(request, vertices, src, tgt, weight);
}

inline void ClearGraph(const std::string &request) {
  GraphInstance::Clear(request);
}

inline string GetRequest(const EngineServiceRequest &request) {
  auto meta = request.GetSegmentMeta();
  if (!meta)
    throw std::runtime_error("Segment meta is not ready");
  string output = std::to_string(request.timeout_ts_) + " " +
                  std::to_string(meta->size()) + " " + request.requestid_.str();
  return output;
}

inline MapAccum<uint64_t, float>
RunPersonalizedPageRank(const std::string& req, bool weight, float damping, float maxChange,
                        int maxIter, const ListAccum<uint64_t> &init_list,
                        const ListAccum<uint64_t> &output_list) {
  PageRank pr;
  pr.InitializeComputeSparse(GraphInstance::GetInstance(req), init_list);
  //  iter = 1
  pr.RunOneIterationSparse(weight, output_list, damping);
  //  iter = 2
  return pr.RunOneIterationFinal(weight, output_list, damping);
}

/*** ----------------------------------- ***/
/*** ----------------------------------- ***/
/*** ----------------------------------- ***/
/*** ----------------------------------- ***/
/*** end of matrix like implementation   ***/
/*******************************************************************************************************/

}// namespace UDIMPL

#endif /* EXPRFUNCTIONS_HPP_ */
