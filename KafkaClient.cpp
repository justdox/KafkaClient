#include <iostream>

#include "KafkaProducer.h"
#include <vector>
#include <thread>

const auto brokers = "";
const auto topic = "";
const auto partition = 0;
const auto message = "";
const auto key = "";
const std::vector<std::string> topics = { topic };

void produce() {
  auto producer = new KafkaProducer(brokers, topic, partition);
  producer->pushMessage(message, key);
}

void consume() {

}

int main() {

  std::cout << "Hello C++!" << std::endl;

  std::thread $1(produce);
  std::thread $2(consume);

  return 0;
}