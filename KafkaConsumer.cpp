#include "KafkaConsumer.h"

// ����������
KafkaConsumer::KafkaConsumer(const std::string& brokers,
  const std::string& groupID,
  const std::vector<std::string>& topics,
  int partition) {
  m_brokers = brokers;
  m_groupID = groupID;
  m_topicVector = topics;
  m_partition = partition;

  std::string errorStr;
  RdKafka::Conf::ConfResult errorCode;

  // 1���������ö���
  // 1.1������ consumer conf ����
  m_config = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

  // ��Ҫ����1��ָ�� broker ��ַ�б�
  errorCode = m_config->set("bootstrap.servers", m_brokers, errorStr);
  if (errorCode != RdKafka::Conf::CONF_OK) {
    std::cout << "Conf set failed: " << errorStr << std::endl;
  }
  // ��Ҫ����2�������������� id
  errorCode = m_config->set("group.id", m_groupID, errorStr);
  if (errorCode != RdKafka::Conf::CONF_OK) {
    std::cout << "Conf set failed: " << errorStr << std::endl;
  }

  // �����¼��ص�
  m_event_cb = new ConsumerEventCb;
  errorCode = m_config->set("event_cb", m_event_cb, errorStr);
  if (errorCode != RdKafka::Conf::CONF_OK) {
    std::cout << "Conf set failed: " << errorStr << std::endl;
  }

  // ��������������ƽ��ص�
  m_rebalance_cb = new ConsumerRebalanceCb;
  errorCode = m_config->set("rebalance_cb", m_rebalance_cb, errorStr);
  if (errorCode != RdKafka::Conf::CONF_OK) {
    std::cout << "Conf set failed: " << errorStr << std::endl;
  }

  // �������ߵ��������β������ RD_KAFKA_RESP_ERR__PARTITION_EOF �¼�
  errorCode = m_config->set("enable.partition.eof", "false", errorStr);
  if (errorCode != RdKafka::Conf::CONF_OK) {
    std::cout << "Conf set failed: " << errorStr << std::endl;
  }

  // ÿ�������ȡ�����ݴ�С
  errorCode = m_config->set("max.partition.fetch.bytes", "1024000", errorStr);
  if (errorCode != RdKafka::Conf::CONF_OK) {
    std::cout << "Conf set failed: " << errorStr << std::endl;
  }

  // ���÷���������ԣ�range��roundrobin��cooperative-sticky
  errorCode = m_config->set("partition.assignment.strategy", "range", errorStr);
  if (errorCode != RdKafka::Conf::CONF_OK) {
    std::cout << "Conf set failed: " << errorStr << std::endl;
  }

  // ����̽�ʱʱ��
  errorCode = m_config->set("session.timeout.ms", "6000", errorStr);
  if (errorCode != RdKafka::Conf::CONF_OK) {
    std::cout << "Conf set failed: " << errorStr << std::endl;
  }

  // ����������
  errorCode = m_config->set("heartbeat.interval.ms", "2000", errorStr);
  if (errorCode != RdKafka::Conf::CONF_OK) {
    std::cout << "Conf set failed: " << errorStr << std::endl;
  }

  // 1.2������ topic conf ����
  m_topicConfig = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

  // ��Ҫ����3�������µ��������ߵ�������ʼλ�ã�latest �������µ����ݣ�earliest ��ͷ��ʼ����
  errorCode = m_topicConfig->set("auto.offset.reset", "latest", errorStr);
  if (errorCode != RdKafka::Conf::CONF_OK) {
    std::cout << "Topic Conf set failed: " << errorStr << std::endl;
  }

  // Ĭ�� topic ���ã������Զ����� topics
  errorCode = m_config->set("default_topic_conf", m_topicConfig, errorStr);
  if (errorCode != RdKafka::Conf::CONF_OK) {
    std::cout << "Conf set failed: " << errorStr << std::endl;
  }

  // 2������ Consumer ����
  m_consumer = RdKafka::KafkaConsumer::create(m_config, errorStr);
  if (m_consumer == NULL) {
    std::cout << "Create KafkaConsumer failed: " << errorStr << std::endl;
  }
  std::cout << "Created consumer " << m_consumer->name() << std::endl;
}

// ������Ϣ
void msg_consume(RdKafka::Message* msg, void* opaque) {
  switch (msg->err()) {
  case RdKafka::ERR__TIMED_OUT:
    // std::cerr << "Consumer error: " << msg->errstr() << std::endl; //
    // ��ʱ
    break;
  case RdKafka::ERR_NO_ERROR: // ����Ϣ����
    std::cout << " Message in-> topic:" << msg->topic_name()
      << ", partition:[" << msg->partition() << "] at offset "
      << msg->offset() << " key: " << msg->key()
      << " payload: " << (char*)msg->payload() << std::endl;
    break;
  default:
    std::cerr << "Consumer error: " << msg->errstr() << std::endl;
    break;
  }
}

// ��ȡ��Ϣ������
void KafkaConsumer::pullMessage() {
  // 1����������
  RdKafka::ErrorCode errorCode = m_consumer->subscribe(m_topicVector);
  if (errorCode != RdKafka::ERR_NO_ERROR) {
    std::cout << "subscribe failed: " << RdKafka::err2str(errorCode)
      << std::endl;
  }
  // 2����ȡ��������Ϣ
  while (true) {
    RdKafka::Message* msg = m_consumer->consume(1000); // 1000ms��ʱ
    // ������Ϣ
    msg_consume(msg, NULL);

    // �����ֶ��ύ
    // 1���첽�ύ���׶����ύ
    // m_consumer->commitAsync(); 
    delete msg;
  }
  // 2��ͬ���ύ��Consumer �ر�ǰ���ã��ȴ� broker ���ض�ȡ��Ϣ
  m_consumer->commitSync();
}

KafkaConsumer::~KafkaConsumer() {
  m_consumer->close();
  delete m_config;
  delete m_topicConfig;
  delete m_consumer;
  delete m_event_cb;
  delete m_rebalance_cb;
}
