#ifndef KAFKAPRODUCER_H
#define KAFKAPRODUCER_H

#pragma once

#include <librdkafka/rdkafkacpp.h>
#include <iostream>
#include <string>

// 生产者投递报告回调
class ProducerDeliveryReportCb : public RdKafka::DeliveryReportCb {
public:
  void dr_cb(RdKafka::Message& message) {
    // 发送出错的回调
    if (message.err()) {
      std::cerr << "Message delivery failed: " << message.errstr() << std::endl;
    }
    // 发送正常的回调
    // Message delivered to topic test [2] at offset 4169
    else {
      std::cout << "Message delivered to topic " << message.topic_name()
        << " [" << message.partition() << "] at offset "
        << message.offset() << std::endl;
    }
  }
};

// 生产者事件回调函数
class ProducerEventCb : public RdKafka::EventCb {
public:
  void event_cb(RdKafka::Event& event) {
    switch (event.type()) {
    case RdKafka::Event::EVENT_ERROR:
      std::cout << "RdKafka::Event::EVENT_ERROR: "
        << RdKafka::err2str(event.err()) << std::endl;
      break;
    case RdKafka::Event::EVENT_STATS:
      std::cout << "RdKafka::Event::EVENT_STATS: " << event.str()
        << std::endl;
      break;
    case RdKafka::Event::EVENT_LOG:
      std::cout << "RdKafka::Event::EVENT_LOG " << event.fac()
        << std::endl;
      break;
    case RdKafka::Event::EVENT_THROTTLE:
      std::cout << "RdKafka::Event::EVENT_THROTTLE "
        << event.broker_name() << std::endl;
      break;
    }
  }
};

// 生产者自定义分区策略回调：partitioner_cb
class HashPartitionerCb : public RdKafka::PartitionerCb {
public:
  // @brief 返回 topic 中使用 key 的分区，msg_opaque 置 NULL
  // @return 返回分区，(0, partition_cnt)
  int32_t partitioner_cb(const RdKafka::Topic* topic, const std::string* key,
    int32_t partition_cnt, void* msg_opaque) {
    char msg[128] = { 0 };
    // 用于自定义分区策略：这里用 hash。例：轮询方式：p_id++ % partition_cnt
    int32_t partition_id = generate_hash(key->c_str(), key->size()) % partition_cnt;
    // 输出：[topic][key][partition_cnt][partition_id]，例 [test][6419][2][1]
    sprintf_s(msg, "HashPartitionerCb:topic:[%s], key:[%s], partition_cnt:[%d], partition_id:[%d]",
      topic->name().c_str(), key->c_str(), partition_cnt, partition_id);
    std::cout << msg << std::endl;
    return partition_id;
  }

private:
  // 自定义哈希函数 
  static inline unsigned int generate_hash(const char* str, size_t len) {
    unsigned int hash = 5381;
    for (size_t i = 0; i < len; i++)
      hash = ((hash << 5) + hash) + str[i];
    return hash;
  }
};

class KafkaProducer {
public:
  /**
   * @brief KafkaProducer
   * @param brokers
   * @param topic
   * @param partition
   */
  explicit KafkaProducer(const std::string& brokers, const std::string& topic,
    int partition);
  /**
   * @brief push Message to Kafka
   * @param str, message data
   */
  void pushMessage(const std::string& str, const std::string& key);
  ~KafkaProducer();

protected:
  std::string m_brokers;          // Broker 列表，多个使用逗号分隔
  std::string m_topicStr;         // Topic 名称
  int m_partition;                // 分区
  RdKafka::Conf* m_config;        // Kafka Conf 对象
  RdKafka::Conf* m_topicConfig;   // Topic Conf 对象

  RdKafka::Topic* m_topic;              // Topic对象
  RdKafka::Producer* m_producer;        // Producer对象
  RdKafka::DeliveryReportCb* m_dr_cb;   // 设置传递回调
  RdKafka::EventCb* m_event_cb;         // 设置事件回调
  RdKafka::PartitionerCb* m_partitioner_cb; // 设置自定义分区回调
};

#endif // KAFKAPRODUCER_H
