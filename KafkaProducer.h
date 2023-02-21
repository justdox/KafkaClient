#ifndef KAFKAPRODUCER_H
#define KAFKAPRODUCER_H

#pragma once

#include <librdkafka/rdkafkacpp.h>
#include <iostream>
#include <string>

// ������Ͷ�ݱ���ص�
class ProducerDeliveryReportCb : public RdKafka::DeliveryReportCb {
public:
  void dr_cb(RdKafka::Message& message) {
    // ���ͳ���Ļص�
    if (message.err()) {
      std::cerr << "Message delivery failed: " << message.errstr() << std::endl;
    }
    // ���������Ļص�
    // Message delivered to topic test [2] at offset 4169
    else {
      std::cout << "Message delivered to topic " << message.topic_name()
        << " [" << message.partition() << "] at offset "
        << message.offset() << std::endl;
    }
  }
};

// �������¼��ص�����
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

// �������Զ���������Իص���partitioner_cb
class HashPartitionerCb : public RdKafka::PartitionerCb {
public:
  // @brief ���� topic ��ʹ�� key �ķ�����msg_opaque �� NULL
  // @return ���ط�����(0, partition_cnt)
  int32_t partitioner_cb(const RdKafka::Topic* topic, const std::string* key,
    int32_t partition_cnt, void* msg_opaque) {
    char msg[128] = { 0 };
    // �����Զ���������ԣ������� hash��������ѯ��ʽ��p_id++ % partition_cnt
    int32_t partition_id = generate_hash(key->c_str(), key->size()) % partition_cnt;
    // �����[topic][key][partition_cnt][partition_id]���� [test][6419][2][1]
    sprintf_s(msg, "HashPartitionerCb:topic:[%s], key:[%s], partition_cnt:[%d], partition_id:[%d]",
      topic->name().c_str(), key->c_str(), partition_cnt, partition_id);
    std::cout << msg << std::endl;
    return partition_id;
  }

private:
  // �Զ����ϣ���� 
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
  std::string m_brokers;          // Broker �б����ʹ�ö��ŷָ�
  std::string m_topicStr;         // Topic ����
  int m_partition;                // ����
  RdKafka::Conf* m_config;        // Kafka Conf ����
  RdKafka::Conf* m_topicConfig;   // Topic Conf ����

  RdKafka::Topic* m_topic;              // Topic����
  RdKafka::Producer* m_producer;        // Producer����
  RdKafka::DeliveryReportCb* m_dr_cb;   // ���ô��ݻص�
  RdKafka::EventCb* m_event_cb;         // �����¼��ص�
  RdKafka::PartitionerCb* m_partitioner_cb; // �����Զ�������ص�
};

#endif // KAFKAPRODUCER_H
