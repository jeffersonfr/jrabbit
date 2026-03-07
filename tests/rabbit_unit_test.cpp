#include "jrabbit/Rabbit.hpp"

#include <iostream>

#include <fmt/format.h>
#include <gtest/gtest.h>

constexpr auto RabbitHost = "rabbitmq.host";
constexpr auto RabbitPort = 5672;

struct Environment : public ::testing::Environment {
  Environment() = default;

  void SetUp() override {
  }

  void TearDown() override {
  }
};

struct jRabbitSuite : public ::testing::Test {
  jRabbitSuite() = default;

  void SetUp() override {
  }

  void TearDown() override {
  }
};

TEST_F(jRabbitSuite, ContextTest) {
  auto keys = std::pair{"test-key.pem", "test-cert.pem"};

  auto context = jrabbit::Context{}
      .host(RabbitHost)
      .port(RabbitPort)
      .timeout(std::chrono::seconds{1})
      .frame(8192)
      .user("test-user")
      .pass("test-pass")
      .virtual_host("my-virtual-host")
      .ssl_cacert("test-cacert.pem")
      .ssl_engine("pksc11")
      .ssl_verify_peer(true)
      .ssl_verify_hostname(true)
      .ssl_keys(keys);

  ASSERT_EQ(context.host(), RabbitHost);
  ASSERT_EQ(context.port(), RabbitPort);
  ASSERT_EQ(context.timeout(), std::chrono::seconds{1});
  ASSERT_EQ(context.frame(), 8192);
  ASSERT_EQ(context.user(), "test-user");
  ASSERT_EQ(context.pass(), "test-pass");
  ASSERT_EQ(context.virtual_host(), "my-virtual-host");
  ASSERT_EQ(context.ssl_cacert(), "test-cacert.pem");
  ASSERT_EQ(context.ssl_engine(), "pksc11");
  ASSERT_EQ(context.ssl_verify_peer(), true);
  ASSERT_EQ(context.ssl_verify_hostname(), true);
  ASSERT_EQ(context.ssl_keys().value(), keys);
}

TEST_F(jRabbitSuite, QueueTest) {
  auto queue = jrabbit::Queue{}
      .name("test-queue")
      .auto_delete(true)
      .durable(true)
      .exclusive(true)
      .passive(true);

  ASSERT_EQ(queue.name(), "test-queue");
  ASSERT_EQ(queue.auto_delete(), true);
  ASSERT_EQ(queue.durable(), true);
  ASSERT_EQ(queue.exclusive(), true);
  ASSERT_EQ(queue.passive(), true);
}

TEST_F(jRabbitSuite, ExchangeTest) {
  auto exchange = jrabbit::Exchange{}
      .name("test-exchange")
      .auto_delete(true)
      .durable(true)
      .passive(true)
      .type(jrabbit::Exchange::Type::DIRECT);

  ASSERT_EQ(exchange.name(), "test-exchange");
  ASSERT_EQ(exchange.auto_delete(), true);
  ASSERT_EQ(exchange.durable(), true);
  ASSERT_EQ(exchange.passive(), true);
  ASSERT_EQ(exchange.type(), jrabbit::Exchange::Type::DIRECT);
}

TEST_F(jRabbitSuite, LinearTest) {
  auto context = jrabbit::Context{}.host(RabbitHost).port(RabbitPort).timeout(std::chrono::seconds{1});
  auto state = jrabbit::RabbitMq::connect(context);

  if (!state) {
    std::println(std::cout, "Error: {}", state.error());

    FAIL();
  }

  auto channel = state->open(1);

  auto ex = jrabbit::Exchange{"exchange1"}.type(jrabbit::Exchange::Type::FANOUT);
  auto q1 = jrabbit::Queue{"queue1"};
  auto rk = jrabbit::RoutingKey{};

  // -- creating objects
  channel->declare_exchange(ex);
  channel->declare_queue(q1);

  // -- binding objects
  channel->bind(ex, q1, rk);

  // -- send messsage
  channel->publish(ex, jrabbit::Message{"message 1 ..."});

  std::vector<std::string> result, golden{
    "message 1 ...",
  };

  try {
    channel->subscribe([&](auto &&envelope) {
      result.push_back(std::string{envelope->data()});

      return false;
    }, q1, "tag1", std::chrono::seconds{1});
    channel->cancel("tag1");
  } catch (std::runtime_error &e) {
    std::println(std::cout, "Error: {}", e.what());
  }

  // -- unbinding objects
  channel->unbind(ex, q1);

  // -- delete objects
  channel->delete_queue(q1);
  channel->delete_exchange(ex);

  ASSERT_EQ(result, golden);
}

TEST_F(jRabbitSuite, FunctionalTest) {
  auto result = jrabbit::RabbitMq::connect(
        jrabbit::Context{}.host(RabbitHost).port(RabbitPort).timeout(std::chrono::seconds{1}))
      .and_then([](jrabbit::RabbitMq mq) -> std::expected<jrabbit::RabbitMq, std::string> {
        auto ex = jrabbit::Exchange{"exchange1"}.type(jrabbit::Exchange::Type::FANOUT);
        auto q1 = jrabbit::Queue{"queue1"};
        auto q2 = jrabbit::Queue{"queue2"};
        auto q3 = jrabbit::Queue{"queue3"};
        auto rk = jrabbit::RoutingKey{};

        auto channel = mq.open(1);

        // -- creating objects
        channel->declare_exchange(ex);
        channel->declare_queue(q1);

        // -- binding objects
        channel->bind(ex, q1);

        // -- send messsage
        channel->publish(ex, jrabbit::Message{"message 1 ..."});
        channel->publish(ex, jrabbit::Message{"message 2 ..."});
        channel->publish(ex, jrabbit::Message{"message 3 ..."});

        // -- reading q1
        std::vector<std::string> result, golden{
          "message 1 ...",
        };

        try {
          channel->subscribe([&](auto &&envelope) {
            result.push_back(std::string{envelope->data()});

            return false;
          }, q1, "tag1", std::chrono::seconds{1});
          channel->cancel("tag1");
        } catch (std::runtime_error &e) {
          std::println(std::cout, "Error: {}", e.what());
        }

        // -- unbinding objects
        channel->unbind(ex, q1);

        // -- delete objects
        channel->delete_queue(q1);
        channel->delete_exchange(ex);

        return std::move(mq);
      });

  if (!result) {
    std::println(std::cout, "Error: {}", result.error());

    FAIL();
  }
}

TEST_F(jRabbitSuite, FanoutTest) {
  auto context = jrabbit::Context{}.host(RabbitHost).port(RabbitPort).timeout(std::chrono::seconds{1});
  auto state = jrabbit::RabbitMq::connect(context);

  if (!state) {
    std::println(std::cout, "Error: {}", state.error());

    FAIL();
  }

  auto channel = state->open(1);

  auto ex = jrabbit::Exchange{"exchange1"}.type(jrabbit::Exchange::Type::FANOUT);
  auto q1 = jrabbit::Queue{"queue1"};
  auto q2 = jrabbit::Queue{"queue2"};
  auto q3 = jrabbit::Queue{"queue3"};

  // -- creating objects
  channel->declare_exchange(ex);
  channel->declare_queue(q1);
  channel->declare_queue(q2);
  channel->declare_queue(q3);

  // -- binding objects
  channel->bind(ex, q1);
  channel->bind(ex, q2);
  channel->bind(ex, q3);

  // -- send messsage
  channel->publish(ex, jrabbit::Message{"message 1 ..."});
  channel->publish(ex, jrabbit::Message{"message 2 ..."});
  channel->publish(ex, jrabbit::Message{"message 3 ..."});

  std::vector<std::string> result, golden{
    "message 1 ...",
    "message 1 ...",
    "message 1 ...",
    "message 2 ...",
    "message 3 ..."
  };

  std::string_view tag1{"tag1"};
  std::string_view tag2{"tag2"};
  std::string_view tag3{"tag3"};

  try {
    channel->subscribe([&](auto &&envelope) {
      result.push_back(std::string{envelope->data()});

      return false;
    }, q1, tag1, std::chrono::seconds{1});
    channel->cancel(tag1);

    channel->subscribe([&](auto &&envelope) {
      result.push_back(std::string{envelope->data()});

      return false;
    }, q2, tag2, std::chrono::seconds{1});
    channel->cancel(tag2);

    channel->subscribe([&](auto &&envelope) {
      result.push_back(std::string{envelope->data()});

      return true;
    }, q3, tag3, std::chrono::seconds{1});
    channel->cancel(tag3);
  } catch (std::exception &e) {
    std::cout << e.what() << std::endl;
  }

  // -- unbinding objects
  channel->unbind(ex, q1);
  channel->unbind(ex, q2);
  channel->unbind(ex, q3);

  // -- delete objects
  channel->delete_queue(q1);
  channel->delete_queue(q2);
  channel->delete_queue(q3);
  channel->delete_exchange(ex);

  ASSERT_EQ(result, golden);
}

TEST_F(jRabbitSuite, DirectTest) {
  auto context = jrabbit::Context{}.host(RabbitHost).port(RabbitPort).timeout(std::chrono::seconds{1});
  auto state = jrabbit::RabbitMq::connect(context);

  if (!state) {
    std::println(std::cout, "Error: {}", state.error());

    FAIL();
  }

  auto channel = state->open(1);

  auto ex = jrabbit::Exchange{"exchange1"}.type(jrabbit::Exchange::Type::DIRECT);
  auto q1 = jrabbit::Queue{"queue1"};
  auto q2 = jrabbit::Queue{"queue2"};
  auto q3 = jrabbit::Queue{"queue3"};
  auto rk1 = jrabbit::RoutingKey{"rk.log"};
  auto rk2 = jrabbit::RoutingKey{"rk.report"};
  auto rk3 = jrabbit::RoutingKey{"rk.mail"};

  // -- creating objects
  channel->declare_exchange(ex);
  channel->declare_queue(q1);
  channel->declare_queue(q2);
  channel->declare_queue(q3);

  // -- binding objects
  channel->bind(ex, q1, rk1);
  channel->bind(ex, q2, rk2);
  channel->bind(ex, q3, rk3);

  // -- send messsage
  channel->publish(ex, jrabbit::Message{"message 1 ..."}, rk1);
  channel->publish(ex, jrabbit::Message{"message 2 ..."}, rk2);
  channel->publish(ex, jrabbit::Message{"message 3 ..."}, rk3);

  std::vector<std::string> result, golden{
    "message 1 ...",
    "message 2 ...",
    "message 3 ..."
  };

  std::string_view tag1{"tag1"};
  std::string_view tag2{"tag2"};
  std::string_view tag3{"tag3"};

  try {
    channel->subscribe([&](auto &&envelope) {
      result.push_back(std::string{envelope->data()});

      return false;
    }, q1, tag1, std::chrono::seconds{1});
    channel->cancel(tag1);

    channel->subscribe([&](auto &&envelope) {
      result.push_back(std::string{envelope->data()});

      return false;
    }, q2, tag2, std::chrono::seconds{1});
    channel->cancel(tag2);

    channel->subscribe([&](auto &&envelope) {
      result.push_back(std::string{envelope->data()});

      return true;
    }, q3, tag3, std::chrono::seconds{1});
    channel->cancel(tag3);
  } catch (std::exception &e) {
    std::cout << e.what() << std::endl;
  }

  // -- unbinding objects
  channel->unbind(ex, q1);
  channel->unbind(ex, q2);
  channel->unbind(ex, q3);

  // -- delete objects
  channel->delete_queue(q1);
  channel->delete_queue(q2);
  channel->delete_queue(q3);
  channel->delete_exchange(ex);

  ASSERT_EQ(result, golden);
}

TEST_F(jRabbitSuite, TopicTest) {
  auto context = jrabbit::Context{}.host(RabbitHost).port(RabbitPort).timeout(std::chrono::seconds{1});
  auto state = jrabbit::RabbitMq::connect(context);

  if (!state) {
    std::println(std::cout, "Error: {}", state.error());

    FAIL();
  }

  auto channel = state->open(1);

  auto ex = jrabbit::Exchange{"exchange1"}.type(jrabbit::Exchange::Type::TOPIC);
  auto q1 = jrabbit::Queue{"queue1"};
  auto q2 = jrabbit::Queue{"queue2"};
  auto q3 = jrabbit::Queue{"queue3"};
  auto rk1 = jrabbit::RoutingKey{"rk.log"};
  auto rk2 = jrabbit::RoutingKey{"rk.#"};
  auto rk3 = jrabbit::RoutingKey{"rk.mail"};

  // -- creating objects
  channel->declare_exchange(ex);
  channel->declare_queue(q1);
  channel->declare_queue(q2);
  channel->declare_queue(q3);

  // -- binding objects
  channel->bind(ex, q1, rk1);
  channel->bind(ex, q2, rk2);
  channel->bind(ex, q3, rk3);

  // -- send messsage
  channel->publish(ex, jrabbit::Message{"message 1 ..."}, jrabbit::RoutingKey{"rk.test"});

  std::vector<std::string> result, golden{
    "message 1 ..."
  };

  std::string_view tag1{"tag1"};
  std::string_view tag2{"tag2"};
  std::string_view tag3{"tag3"};

  try {
    channel->subscribe([&](auto &&envelope) {
      result.push_back(std::string{envelope->data()});

      return false;
    }, q1, tag1, std::chrono::seconds{1});
    channel->cancel(tag1);
  } catch (std::exception &e) {
    std::cout << e.what() << std::endl;
  }

  try {
    channel->subscribe([&](auto &&envelope) {
      result.push_back(std::string{envelope->data()});

      return false;
    }, q2, tag2, std::chrono::seconds{1});
    channel->cancel(tag2);
  } catch (std::exception &e) {
    std::cout << e.what() << std::endl;
  }

  try {
    channel->subscribe([&](auto &&envelope) {
      result.push_back(std::string{envelope->data()});

      return false;
    }, q3, tag3, std::chrono::seconds{1});
    channel->cancel(tag3);
  } catch (std::exception &e) {
    std::cout << e.what() << std::endl;
  }

  // -- unbinding objects
  channel->unbind(ex, q1);
  channel->unbind(ex, q2);
  channel->unbind(ex, q3);

  // -- delete objects
  channel->delete_queue(q1);
  channel->delete_queue(q2);
  channel->delete_queue(q3);
  channel->delete_exchange(ex);

  ASSERT_EQ(result, golden);
}

TEST_F(jRabbitSuite, HeadersTest) {
  auto context = jrabbit::Context{}.host(RabbitHost).port(RabbitPort).timeout(std::chrono::seconds{1});
  auto state = jrabbit::RabbitMq::connect(context);

  if (!state) {
    std::println(std::cout, "Error: {}", state.error());

    FAIL();
  }

  auto channel = state->open(1);

  // normal queue
  auto ex = jrabbit::Exchange{"exchange1"}.type(jrabbit::Exchange::Type::HEADERS).durable(true);
  auto q1 = jrabbit::Queue{"queue1"}.durable(true);
  auto rk = jrabbit::RoutingKey{"teste"};
  auto params = jrabbit::Params{}
      .put_text("x-match", "all")
      .put_text("format", "pdf")
      .put_text("type", "report");

  channel->declare_exchange(ex);
  channel->declare_queue(q1);

  channel->bind(ex, q1, rk, params);

  // -- send messsage
  channel->publish(ex, jrabbit::Message{"message 1 ..."}, rk, jrabbit::Properties{}
                   .content_type("application/json")
                   .delivery_mode(jrabbit::Properties::DeliveryMode::Persistent)
                   .headers(jrabbit::Params{}
                     .put_text("format", "pdf")
                     .put_text("type", "log")));

  channel->publish(ex, jrabbit::Message{"message 2 ..."}, rk, jrabbit::Properties{}
                   .content_type("application/json")
                   .delivery_mode(jrabbit::Properties::DeliveryMode::Persistent)
                   .headers(jrabbit::Params{}
                     .put_text("format", "pdf")
                     .put_text("type", "report")));

  std::vector<std::string> result, golden{
    "message 2 ...",
  };

  std::string_view tag1{"tag1"};

  try {
    channel->subscribe([&](auto &&envelope) {
      result.push_back(std::string{envelope->data()});

      return false;
    }, q1, tag1, std::chrono::seconds{1});
    channel->cancel(tag1);
  } catch (std::exception &e) {
    std::cout << e.what() << std::endl;
  }

  // -- unbinding objects
  channel->unbind(ex, q1);

  // -- delete objects
  channel->delete_queue(q1);
  channel->delete_exchange(ex);

  ASSERT_EQ(result, golden);
}

TEST_F(jRabbitSuite, StreamTest) {
  auto context = jrabbit::Context{}.host(RabbitHost).port(RabbitPort).timeout(std::chrono::seconds{1});
  auto state = jrabbit::RabbitMq::connect(context);

  if (!state) {
    FAIL();
  }

  auto params = jrabbit::Params{}
      .put_text("x-queue-type", "stream");

  auto channel = state->open(1);

  auto ex = jrabbit::Exchange{"exchange1"}.type(jrabbit::Exchange::Type::FANOUT);
  auto q1 = jrabbit::Queue{"queue1"}.durable(true);
  auto rk = jrabbit::RoutingKey{};

  // -- creating objects
  channel->declare_exchange(ex);
  channel->declare_queue(q1, params);

  // -- binding objects
  channel->bind(ex, q1);

  // -- send messsage
  auto offset = jrabbit::Params{}
      .put_int32("x-stream-offset", 1); // print second published message

  channel->publish(ex, jrabbit::Message{"message 1 ..."});
  channel->publish(ex, jrabbit::Message{"message 2 ..."});
  channel->publish(ex, jrabbit::Message{"message 3 ..."});

  channel->qos(0, 100);

  std::vector<std::string> result, golden{
    "message 2 ...",
  };

  std::string_view tag1{"tag1"};

  try {
    channel->subscribe([&](auto &&envelope) {
      result.push_back(std::string{envelope->data()});

      return false;
    }, q1, tag1, std::chrono::seconds{1}, false, false, false, offset);
    channel->cancel(tag1);
  } catch (std::exception &e) {
    std::cout << e.what() << std::endl;
  }

  // -- unbinding objects
  channel->unbind(ex, q1);

  // -- delete objects
  channel->delete_queue(q1);
  channel->delete_exchange(ex);

  ASSERT_EQ(result, golden);
}

TEST_F(jRabbitSuite, PollTest) {
  auto context = jrabbit::Context{}.host(RabbitHost).port(RabbitPort).timeout(std::chrono::seconds{1});
  auto state = jrabbit::RabbitMq::connect(context);

  if (!state) {
    std::println(std::cout, "Error: {}", state.error());

    FAIL();
  }

  auto channel = state->open(1);

  auto ex = jrabbit::Exchange{"exchange1"}.type(jrabbit::Exchange::Type::FANOUT);
  auto q1 = jrabbit::Queue{"queue1"};
  auto rk = jrabbit::RoutingKey{};

  // -- creating objects
  channel->declare_exchange(ex);
  channel->declare_queue(q1);

  // -- binding objects
  channel->bind(ex, q1);

  // -- send messsage
  channel->publish(ex, jrabbit::Message{"message 1 ..."});

  std::vector<std::string> result, golden{
    "message 1 ...",
  };

  try {
    auto msg = channel->get(q1);

    if (msg) {
      result.push_back(std::string{msg.value()->data()});
    }
  } catch (std::runtime_error &e) {
    std::println(std::cout, "Error: {}", e.what());
  }

  // -- unbinding objects
  channel->unbind(ex, q1);

  // -- delete objects
  channel->delete_queue(q1);
  channel->delete_exchange(ex);

  ASSERT_EQ(result, golden);
}

TEST_F(jRabbitSuite, DeadLetterTest) {
  auto context = jrabbit::Context{}.host(RabbitHost).port(RabbitPort).timeout(std::chrono::seconds{1});
  auto state = jrabbit::RabbitMq::connect(context);

  if (!state) {
    std::println(std::cout, "Error: {}", state.error());

    FAIL();
  }

  auto channel = state->open(1);

  // dead ltter queue
  auto dlx_ex = jrabbit::Exchange{"dlx.exchange"}.type(jrabbit::Exchange::Type::DIRECT);
  auto dlx_q1 = jrabbit::Queue{"dlq.queue"}.durable(true);
  auto dlx_rk = jrabbit::RoutingKey{"dead"};

  channel->declare_exchange(dlx_ex);
  channel->declare_queue(dlx_q1);

  channel->bind(dlx_ex, dlx_q1, dlx_rk);

  // normal queue
  auto ex = jrabbit::Exchange{"exchange1"}.type(jrabbit::Exchange::Type::DIRECT);
  auto q1 = jrabbit::Queue{"queue1"};
  auto rk = jrabbit::RoutingKey{};
  auto params = jrabbit::Params{}
      .put_text("x-dead-letter-exchange", "dlx.exchange")
      .put_text("x-dead-letter-routing-key", "dead");

  channel->declare_exchange(ex);
  channel->declare_queue(q1, params);

  channel->bind(ex, q1);

  // -- send messsage
  channel->publish(ex, jrabbit::Message{"message 1 ..."});

  std::vector<std::string> result, golden{
    "message 1 ...",
  };

  std::string_view tag1{"tag1"};
  std::string_view dlx_tag1{"dlx_tag1"};

  try {
    channel->subscribe([&](auto &&envelope) {
      channel->reject(*envelope);

      return false;
    }, q1, tag1, std::chrono::seconds{1}, false, false);
    channel->cancel(tag1);

    channel->subscribe([&](auto &&envelope) {
      result.push_back(std::string{envelope->data()});

      return false;
    }, dlx_q1, dlx_tag1, std::chrono::seconds{1});
    channel->cancel(dlx_tag1);
  } catch (std::exception &e) {
    std::cout << e.what() << std::endl;
  }

  // -- unbinding objects
  channel->unbind(ex, q1);
  channel->unbind(dlx_ex, dlx_q1);

  // -- delete objects
  channel->delete_queue(q1);
  channel->delete_exchange(ex);
  channel->delete_queue(dlx_q1);
  channel->delete_exchange(dlx_ex);

  ASSERT_EQ(result, golden);
}

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);

  ::testing::AddGlobalTestEnvironment(new Environment{});

  return RUN_ALL_TESTS();
}
