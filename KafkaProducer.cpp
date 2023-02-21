#include "KafkaProducer.h"

// ����������
KafkaProducer::KafkaProducer(const std::string& brokers, const std::string& topic, int partition) {
  m_brokers = brokers;
  m_topicStr = topic;
  m_partition = partition;

  RdKafka::Conf::ConfResult errCode;      // ����������
  std::string errorStr;                   // ���ش�����Ϣ   

  // �������ö���
  // 1.1������ Kafka Conf ����
  m_config = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  if (m_config == NULL) {
    std::cout << "Create RdKafka Conf failed." << std::endl;
  }

  // ���� Broker ����       
  // ����Ҫ������ָ�� broker ��ַ�б���ʽ��host1:port1,host2:port2,...
  errCode = m_config->set("bootstrap.servers", m_brokers, errorStr);
  if (errCode != RdKafka::Conf::CONF_OK) {
    std::cout << "Conf set failed:" << errorStr << std::endl;
  }

  // ����������Ͷ�ݱ���ص�
  m_dr_cb = new ProducerDeliveryReportCb; // ����Ͷ�ݱ���ص�
  errCode = m_config->set("dr_cb", m_dr_cb, errorStr);    // �첽��ʽ��������
  if (errCode != RdKafka::Conf::CONF_OK) {
    std::cout << "Conf set failed:" << errorStr << std::endl;
  }

  // �����������¼��ص�
  m_event_cb = new ProducerEventCb; // �����������¼��ص�
  errCode = m_config->set("event_cb", m_event_cb, errorStr);
  if (errCode != RdKafka::Conf::CONF_OK) {
    std::cout << "Conf set failed:" << errorStr << std::endl;
  }

  // ��������ͳ�Ƽ��
  errCode = m_config->set("statistics.interval.ms", "10000", errorStr);
  if (errCode != RdKafka::Conf::CONF_OK) {
    std::cout << "Conf set failed:" << errorStr << std::endl;
  }

  // �����������Ϣ��С
  errCode = m_config->set("message.max.bytes", "10240000", errorStr);
  if (errCode != RdKafka::Conf::CONF_OK) {
    std::cout << "Conf set failed:" << errorStr << std::endl;
  }

  // 2������ Topic Conf ����
  m_topicConfig = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
  if (m_topicConfig == NULL) {
    std::cout << "Create RdKafka Topic Conf failed." << std::endl;
  }

  // �����������Զ���������Իص�
  m_partitioner_cb = new HashPartitionerCb; // �����Զ������Ͷ�ݻص�
  errCode = m_topicConfig->set("partitioner_cb", m_partitioner_cb, errorStr);
  if (errCode != RdKafka::Conf::CONF_OK) {
    std::cout << "Conf set failed:" << errorStr << std::endl;
  }

  // 2����������
  // 2.1������ Producer ���󣬿��Է�����ͬ������
  m_producer = RdKafka::Producer::create(m_config, errorStr);
  if (m_producer == NULL) {
    std::cout << "Create Producer failed:" << errorStr << std::endl;
  }

  // 2.2������ Topic ���󣬿��Դ��������ͬ�� topic ����
  m_topic = RdKafka::Topic::create(m_producer, m_topicStr, m_topicConfig, errorStr);
  // m_topic2 =RdKafka::Topic::create(m_producer, m_topicStr2, m_topicConfig2, errorStr);
  if (m_topic == NULL) {
    std::cout << "Create Topic failed:" << errorStr << std::endl;
  }
}

// ������Ϣ
void KafkaProducer::pushMessage(const std::string& str, const std::string& key) {
  int32_t len = str.length();
  void* payload = const_cast<void*>(static_cast<const void*>(str.data()));

  // produce �����������ͷ��͵�����Ϣ�� Broker
  // �������ʱ������ڲ����Զ����ϵ�ǰ��ʱ���
  RdKafka::ErrorCode errorCode = m_producer->produce(
    m_topic,                      // ָ�����͵�������
    RdKafka::Topic::PARTITION_UA, // ָ�����������ΪPARTITION_UA��ͨ��
    // partitioner_cb�Ļص�ѡ����ʵķ���
    RdKafka::Producer::RK_MSG_COPY, // ��Ϣ����
    payload,                        // ��Ϣ����
    len,                            // ��Ϣ����
    &key,                           // ��Ϣkey
    NULL // an optional application-provided per-message opaque pointer
         // that will be provided in the message delivery callback to let
         // the application reference a specific message
  );
  // ��ѯ����
  m_producer->poll(0);
  if (errorCode != RdKafka::ERR_NO_ERROR) {
    std::cerr << "Produce failed: " << RdKafka::err2str(errorCode)
      << std::endl;
    // kafka ���������ȴ� 100 ms
    if (errorCode == RdKafka::ERR__QUEUE_FULL) {
      m_producer->poll(100);
    }
  }
}

// ����������
KafkaProducer::~KafkaProducer() {
  while (m_producer->outq_len() > 0) {
    std::cerr << "Waiting for " << m_producer->outq_len() << std::endl;
    m_producer->flush(5000);
  }
  delete m_config;
  delete m_topicConfig;
  delete m_topic;
  delete m_producer;
  delete m_dr_cb;
  delete m_event_cb;
  delete m_partitioner_cb;
}
