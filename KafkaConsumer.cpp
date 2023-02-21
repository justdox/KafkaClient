#include "KafkaConsumer.h"

// 构造消费者
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

  // 1、创建配置对象
  // 1.1、构造 consumer conf 对象
  m_config = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

  // 必要参数1：指定 broker 地址列表
  errorCode = m_config->set("bootstrap.servers", m_brokers, errorStr);
  if (errorCode != RdKafka::Conf::CONF_OK) {
    std::cout << "Conf set failed: " << errorStr << std::endl;
  }
  // 必要参数2：设置消费者组 id
  errorCode = m_config->set("group.id", m_groupID, errorStr);
  if (errorCode != RdKafka::Conf::CONF_OK) {
    std::cout << "Conf set failed: " << errorStr << std::endl;
  }

  // 设置事件回调
  m_event_cb = new ConsumerEventCb;
  errorCode = m_config->set("event_cb", m_event_cb, errorStr);
  if (errorCode != RdKafka::Conf::CONF_OK) {
    std::cout << "Conf set failed: " << errorStr << std::endl;
  }

  // 设置消费者组再平衡回调
  m_rebalance_cb = new ConsumerRebalanceCb;
  errorCode = m_config->set("rebalance_cb", m_rebalance_cb, errorStr);
  if (errorCode != RdKafka::Conf::CONF_OK) {
    std::cout << "Conf set failed: " << errorStr << std::endl;
  }

  // 当消费者到达分区结尾，发送 RD_KAFKA_RESP_ERR__PARTITION_EOF 事件
  errorCode = m_config->set("enable.partition.eof", "false", errorStr);
  if (errorCode != RdKafka::Conf::CONF_OK) {
    std::cout << "Conf set failed: " << errorStr << std::endl;
  }

  // 每次最大拉取的数据大小
  errorCode = m_config->set("max.partition.fetch.bytes", "1024000", errorStr);
  if (errorCode != RdKafka::Conf::CONF_OK) {
    std::cout << "Conf set failed: " << errorStr << std::endl;
  }

  // 设置分区分配策略：range、roundrobin、cooperative-sticky
  errorCode = m_config->set("partition.assignment.strategy", "range", errorStr);
  if (errorCode != RdKafka::Conf::CONF_OK) {
    std::cout << "Conf set failed: " << errorStr << std::endl;
  }

  // 心跳探活超时时间
  errorCode = m_config->set("session.timeout.ms", "6000", errorStr);
  if (errorCode != RdKafka::Conf::CONF_OK) {
    std::cout << "Conf set failed: " << errorStr << std::endl;
  }

  // 心跳保活间隔
  errorCode = m_config->set("heartbeat.interval.ms", "2000", errorStr);
  if (errorCode != RdKafka::Conf::CONF_OK) {
    std::cout << "Conf set failed: " << errorStr << std::endl;
  }

  // 1.2、创建 topic conf 对象
  m_topicConfig = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

  // 必要参数3：设置新到来消费者的消费起始位置，latest 消费最新的数据，earliest 从头开始消费
  errorCode = m_topicConfig->set("auto.offset.reset", "latest", errorStr);
  if (errorCode != RdKafka::Conf::CONF_OK) {
    std::cout << "Topic Conf set failed: " << errorStr << std::endl;
  }

  // 默认 topic 配置，用于自动订阅 topics
  errorCode = m_config->set("default_topic_conf", m_topicConfig, errorStr);
  if (errorCode != RdKafka::Conf::CONF_OK) {
    std::cout << "Conf set failed: " << errorStr << std::endl;
  }

  // 2、创建 Consumer 对象
  m_consumer = RdKafka::KafkaConsumer::create(m_config, errorStr);
  if (m_consumer == NULL) {
    std::cout << "Create KafkaConsumer failed: " << errorStr << std::endl;
  }
  std::cout << "Created consumer " << m_consumer->name() << std::endl;
}

// 消费消息
void msg_consume(RdKafka::Message* msg, void* opaque) {
  switch (msg->err()) {
  case RdKafka::ERR__TIMED_OUT:
    // std::cerr << "Consumer error: " << msg->errstr() << std::endl; //
    // 超时
    break;
  case RdKafka::ERR_NO_ERROR: // 有消息进来
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

// 拉取消息并消费
void KafkaConsumer::pullMessage() {
  // 1、订阅主题
  RdKafka::ErrorCode errorCode = m_consumer->subscribe(m_topicVector);
  if (errorCode != RdKafka::ERR_NO_ERROR) {
    std::cout << "subscribe failed: " << RdKafka::err2str(errorCode)
      << std::endl;
  }
  // 2、拉取并消费消息
  while (true) {
    RdKafka::Message* msg = m_consumer->consume(1000); // 1000ms超时
    // 消费消息
    msg_consume(msg, NULL);

    // 开启手动提交
    // 1、异步提交，阶段性提交
    // m_consumer->commitAsync(); 
    delete msg;
  }
  // 2、同步提交，Consumer 关闭前调用，等待 broker 返回读取消息
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
