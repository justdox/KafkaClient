#ifndef KAFKACONSUMER_H
#define KAFKACONSUMER_H

#pragma once

#include <librdkafka/rdkafkacpp.h>
#include <iostream>
#include <stdio.h>
#include <string>
#include <vector>

// 设置事件回调
class ConsumerEventCb : public RdKafka::EventCb {
public:
  void event_cb(RdKafka::Event& event) {
    switch (event.type()) {
    case RdKafka::Event::EVENT_ERROR:
      if (event.fatal()) {
        std::cerr << "FATAL ";
      }
      std::cerr << "ERROR (" << RdKafka::err2str(event.err())
        << "): " << event.str() << std::endl;
      break;

    case RdKafka::Event::EVENT_STATS:
      std::cerr << "\"STATS\": " << event.str() << std::endl;
      break;

    case RdKafka::Event::EVENT_LOG:
      fprintf(stderr, "LOG-%i-%s: %s\n", event.severity(),
        event.fac().c_str(), event.str().c_str());
      break;

    case RdKafka::Event::EVENT_THROTTLE:
      std::cerr << "THROTTLED: " << event.throttle_time() << "ms by "
        << event.broker_name() << " id " << (int)event.broker_id()
        << std::endl;
      break;

    default:
      std::cerr << "EVENT " << event.type() << " ("
        << RdKafka::err2str(event.err()) << "): " << event.str()
        << std::endl;
      break;
    }
  }
};

// 设置消费者组再平衡回调
// 注册该函数会关闭 rdkafka 的自动分区赋值和再分配
class ConsumerRebalanceCb : public RdKafka::RebalanceCb {
private:
  // 打印当前获取的分区
  static void printTopicPartition(const std::vector<RdKafka::TopicPartition*>& partitions)
  {
    for (unsigned int i = 0; i < partitions.size(); i++) {
      std::cerr << partitions[i]->topic() << "[" << partitions[i]->partition() << "], ";
    }
    std::cerr << "\n";
  }

public:
  // 消费者组再平衡回调
  void rebalance_cb(RdKafka::KafkaConsumer* consumer, RdKafka::ErrorCode err,
    std::vector<RdKafka::TopicPartition*>& partitions) {
    std::cerr << "RebalanceCb: " << RdKafka::err2str(err) << ": ";
    printTopicPartition(partitions);

    // 分区分配成功
    if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
      // 消费者订阅这些分区
      consumer->assign(partitions);
      // 获取消费者组本次订阅的分区数量，可以属于不同的topic
      partition_count = (int)partitions.size();
    }
    // 分区分配失败
    else {
      // 消费者取消订阅所有的分区
      consumer->unassign();
      // 消费者订阅分区的数量为0
      partition_count = 0;
    }
  }

private:
  int partition_count;    // 消费者组本次订阅的分区数量
};

class KafkaConsumer {
public: /**
         * @brief KafkaConsumer
         * @param brokers
         * @param groupID
         * @param topics
         * @param partition
         */
  explicit KafkaConsumer(const std::string& brokers,
    const std::string& groupID,
    const std::vector<std::string>& topics,
    int partition);
  void pullMessage();
  ~KafkaConsumer();

protected:
  std::string m_brokers;
  std::string m_groupID;
  std::vector<std::string> m_topicVector;
  int m_partition;
  RdKafka::Conf* m_config;
  RdKafka::Conf* m_topicConfig;
  RdKafka::KafkaConsumer* m_consumer;
  RdKafka::EventCb* m_event_cb;
  RdKafka::RebalanceCb* m_rebalance_cb;
};

#endif // KAFKACONSUMER_H
