#include "jrabbit/Rabbit.hpp"

#include <iostream>

#include <fmt/format.h>
#include <gtest/gtest.h>

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

const std::string RabbitServer = "rabbitmq.host";
const int RabbitPort = 5672;

TEST_F(jRabbitSuite, ProceduralTest) {
	auto context = jrabbit::Context{}.host(RabbitServer).port(RabbitPort).timeout(std::chrono::seconds{1});
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
	channel->publish(ex, jrabbit::Message{"testando mensagem 1 ..."});

	try {
		for (auto const &e: channel->consume(q1, jrabbit::RoutingKey{}, std::chrono::seconds{1})) {
			std::cout << "MSG: " << e->data() << std::endl;

			break;
		}
	} catch (std::runtime_error &e) {
		std::println(std::cout, "Error: {}", e.what());
	}

	// -- unbinding objects
	channel->unbind(ex, q1);

	// -- delete objects
	channel->delete_queue(q1);
	channel->delete_exchange(ex);
}

TEST_F(jRabbitSuite, MonadTest) {
	auto result = jrabbit::RabbitMq::connect(jrabbit::Context{}.host(RabbitServer).port(RabbitPort).timeout(std::chrono::seconds{1}))
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
				channel->declare_queue(q2);
				channel->declare_queue(q3);

				// -- binding objects
				channel->bind(ex, q1);
				channel->bind(ex, q2);
				channel->bind(ex, q3);

				// -- send messsage
				channel->publish(ex, jrabbit::Message{"testando mensagem 1 ..."});
				channel->publish(ex, jrabbit::Message{"testando mensagem 2 ..."});
				channel->publish(ex, jrabbit::Message{"testando mensagem 3 ..."});

				// -- reading q1
				try {
					for (auto const &e: channel->consume(q1, jrabbit::RoutingKey{}, std::chrono::seconds{1})) {
						std::cout << "MSG: " << e->data() << std::endl;

						break;
					}
				} catch (std::runtime_error &e) {
					std::println(std::cout, "Error: {}", e.what());
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

				return std::move(mq);
	});

	if (!result) {
		std::println(std::cout, "Error: {}", result.error());

		FAIL();
	}
}

TEST_F(jRabbitSuite, StreamTest) {
	auto context = jrabbit::Context{}.host(RabbitServer).port(RabbitPort).timeout(std::chrono::seconds{1});
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

	channel->publish(ex, jrabbit::Message{"testando stream 1 ..."});
	channel->publish(ex, jrabbit::Message{"testando stream 2 ..."});
	channel->publish(ex, jrabbit::Message{"testando stream 3 ..."});

	channel->qos(0, 100);

	try {
		for (auto const &e: channel->consume(q1, jrabbit::RoutingKey{}, std::chrono::seconds{1}, false, false, false, offset)) {
			std::cout << "MSG: " << e->data() << std::endl;

			break;
		}
	} catch (std::runtime_error &e) {
		std::println(std::cout, "Error: {}", e.what());
	}

	// -- unbinding objects
	channel->unbind(ex, q1);

	// -- delete objects
	channel->delete_queue(q1);
	channel->delete_exchange(ex);
}

TEST_F(jRabbitSuite, PollTest) {
	auto context = jrabbit::Context{}.host(RabbitServer).port(RabbitPort).timeout(std::chrono::seconds{1});
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
	channel->publish(ex, jrabbit::Message{"testando mensagem 1 ..."});

	try {
		auto msg = channel->get(q1);

		if (msg) {
			std::cout << "MSG: " << msg.value()->data() << std::endl;
		}
	} catch (std::runtime_error &e) {
		std::println(std::cout, "Error: {}", e.what());
	}

	// -- unbinding objects
	channel->unbind(ex, q1);

	// -- delete objects
	channel->delete_queue(q1);
	channel->delete_exchange(ex);
}

TEST_F(jRabbitSuite, DeadLetterTest) {
	auto context = jrabbit::Context{}.host(RabbitServer).port(RabbitPort).timeout(std::chrono::seconds{1});
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
	auto ex = jrabbit::Exchange{"exchange1"}.type(jrabbit::Exchange::Type::FANOUT);
	auto q1 = jrabbit::Queue{"queue1"};
	auto rk = jrabbit::RoutingKey{};
	auto params = jrabbit::Params{}
		.put_text("x-dead-letter-exchange", "dlx.exchange")
		.put_text("x-dead-letter-routing-key", "dead");

	channel->declare_exchange(ex);
	channel->declare_queue(q1, params);

	channel->bind(ex, q1);

	// -- send messsage
	channel->publish(ex, jrabbit::Message{"testando mensagem 1 ..."});

	try {
		for (auto const &e: channel->consume(q1, jrabbit::RoutingKey{}, std::chrono::seconds{1}, false, false, false)) {
			channel->reject(*e);

			break;
		}
	} catch (std::runtime_error &e) {
		std::println(std::cout, "Error: {}", e.what());
	}

	try {
		for (auto const &e: channel->consume(dlx_q1, dlx_rk, std::chrono::seconds{1})) {
			std::cout << "Dead Letter MSG: " << e->data() << std::endl;

			break;
		}
	} catch (std::runtime_error &e) {
		std::println(std::cout, "Error: {}", e.what());
	}

	// -- unbinding objects
	channel->unbind(ex, q1);
	channel->unbind(dlx_ex, dlx_q1);

	// -- delete objects
	channel->delete_queue(q1);
	channel->delete_exchange(ex);
	channel->delete_queue(dlx_q1);
	channel->delete_exchange(dlx_ex);
}

int main(int argc, char *argv[]) {
	::testing::InitGoogleTest(&argc, argv);

	::testing::AddGlobalTestEnvironment(new Environment{});

	return RUN_ALL_TESTS();
}
