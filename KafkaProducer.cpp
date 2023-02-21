#include "KafkaProducer.h"

// 构造生产者
KafkaProducer::KafkaProducer(const std::string& brokers, const std::string& topic, int partition) {
  m_brokers = brokers;
  m_topicStr = topic;
  m_partition = partition;

  RdKafka::Conf::ConfResult errCode;      // 创建错误码
  std::string errorStr;                   // 返回错误信息   

  // 创建配置对象
  // 1.1、创建 Kafka Conf 对象
  m_config = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  if (m_config == NULL) {
    std::cout << "Create RdKafka Conf failed." << std::endl;
  }

  // 设置 Broker 属性       
  // （必要参数）指定 broker 地址列表。格式：host1:port1,host2:port2,...
  errCode = m_config->set("bootstrap.servers", m_brokers, errorStr);
  if (errCode != RdKafka::Conf::CONF_OK) {
    std::cout << "Conf set failed:" << errorStr << std::endl;
  }

  // 设置生产者投递报告回调
  m_dr_cb = new ProducerDeliveryReportCb; // 创建投递报告回调
  errCode = m_config->set("dr_cb", m_dr_cb, errorStr);    // 异步方式发送数据
  if (errCode != RdKafka::Conf::CONF_OK) {
    std::cout << "Conf set failed:" << errorStr << std::endl;
  }

  // 设置生产者事件回调
  m_event_cb = new ProducerEventCb; // 创建生产者事件回调
  errCode = m_config->set("event_cb", m_event_cb, errorStr);
  if (errCode != RdKafka::Conf::CONF_OK) {
    std::cout << "Conf set failed:" << errorStr << std::endl;
  }

  // 设置数据统计间隔
  errCode = m_config->set("statistics.interval.ms", "10000", errorStr);
  if (errCode != RdKafka::Conf::CONF_OK) {
    std::cout << "Conf set failed:" << errorStr << std::endl;
  }

  // 设置最大发送消息大小
  errCode = m_config->set("message.max.bytes", "10240000", errorStr);
  if (errCode != RdKafka::Conf::CONF_OK) {
    std::cout << "Conf set failed:" << errorStr << std::endl;
  }

  // 2、创建 Topic Conf 对象
  m_topicConfig = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
  if (m_topicConfig == NULL) {
    std::cout << "Create RdKafka Topic Conf failed." << std::endl;
  }

  // 设置生产者自定义分区策略回调
  m_partitioner_cb = new HashPartitionerCb; // 创建自定义分区投递回调
  errCode = m_topicConfig->set("partitioner_cb", m_partitioner_cb, errorStr);
  if (errCode != RdKafka::Conf::CONF_OK) {
    std::cout << "Conf set failed:" << errorStr << std::endl;
  }

  // 2、创建对象
  // 2.1、创建 Producer 对象，可以发布不同的主题
  m_producer = RdKafka::Producer::create(m_config, errorStr);
  if (m_producer == NULL) {
    std::cout << "Create Producer failed:" << errorStr << std::endl;
  }

  // 2.2、创建 Topic 对象，可以创建多个不同的 topic 对象
  m_topic = RdKafka::Topic::create(m_producer, m_topicStr, m_topicConfig, errorStr);
  // m_topic2 =RdKafka::Topic::create(m_producer, m_topicStr2, m_topicConfig2, errorStr);
  if (m_topic == NULL) {
    std::cout << "Create Topic failed:" << errorStr << std::endl;
  }
}

// 发送消息
void KafkaProducer::pushMessage(const std::string& str, const std::string& key) {
  int32_t len = str.length();
  void* payload = const_cast<void*>(static_cast<const void*>(str.data()));

  // produce 方法，生产和发送单条消息到 Broker
  // 如果不加时间戳，内部会自动加上当前的时间戳
  RdKafka::ErrorCode errorCode = m_producer->produce(
    m_topic,                      // 指定发送到的主题
    RdKafka::Topic::PARTITION_UA, // 指定分区，如果为PARTITION_UA则通过
    // partitioner_cb的回调选择合适的分区
    RdKafka::Producer::RK_MSG_COPY, // 消息拷贝
    payload,                        // 消息本身
    len,                            // 消息长度
    &key,                           // 消息key
    NULL // an optional application-provided per-message opaque pointer
         // that will be provided in the message delivery callback to let
         // the application reference a specific message
  );
  // 轮询处理
  m_producer->poll(0);
  if (errorCode != RdKafka::ERR_NO_ERROR) {
    std::cerr << "Produce failed: " << RdKafka::err2str(errorCode)
      << std::endl;
    // kafka 队列满，等待 100 ms
    if (errorCode == RdKafka::ERR__QUEUE_FULL) {
      m_producer->poll(100);
    }
  }
}

// 析构生产者
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
