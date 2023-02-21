#ifndef KAFKACONSUMER_H
#define KAFKACONSUMER_H

#pragma once

#include <librdkafka/rdkafkacpp.h>
#include <iostream>
#include <stdio.h>
#include <string>
#include <vector>

// �����¼��ص�
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

// ��������������ƽ��ص�
// ע��ú�����ر� rdkafka ���Զ�������ֵ���ٷ���
class ConsumerRebalanceCb : public RdKafka::RebalanceCb {
private:
  // ��ӡ��ǰ��ȡ�ķ���
  static void printTopicPartition(const std::vector<RdKafka::TopicPartition*>& partitions)
  {
    for (unsigned int i = 0; i < partitions.size(); i++) {
      std::cerr << partitions[i]->topic() << "[" << partitions[i]->partition() << "], ";
    }
    std::cerr << "\n";
  }

public:
  // ����������ƽ��ص�
  void rebalance_cb(RdKafka::KafkaConsumer* consumer, RdKafka::ErrorCode err,
    std::vector<RdKafka::TopicPartition*>& partitions) {
    std::cerr << "RebalanceCb: " << RdKafka::err2str(err) << ": ";
    printTopicPartition(partitions);

    // ��������ɹ�
    if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
      // �����߶�����Щ����
      consumer->assign(partitions);
      // ��ȡ�������鱾�ζ��ĵķ����������������ڲ�ͬ��topic
      partition_count = (int)partitions.size();
    }
    // ��������ʧ��
    else {
      // ������ȡ���������еķ���
      consumer->unassign();
      // �����߶��ķ���������Ϊ0
      partition_count = 0;
    }
  }

private:
  int partition_count;    // �������鱾�ζ��ĵķ�������
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
