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

TEST_F(jRabbitSuite, ProceduralTest) {
	auto context = jrabbit::Context{}.host("rabbit.jsys.zapto.org").port(5672).timeout(std::chrono::seconds{1});
	auto state = jrabbit::RabbitMq::connect(context);

	if (state) {
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

		for (auto const &e: channel->consume(q1, jrabbit::RoutingKey{}, std::chrono::seconds{1})) {
			std::cout << "MSG: " << e.data() << std::endl;

			break;
		}

		// -- unbinding objects
		channel->unbind(ex, q1);

		// -- delete objects
		channel->delete_queue(q1);
		channel->delete_exchange(ex);
	}
}

TEST_F(jRabbitSuite, MonadTest) {
	jrabbit::RabbitMq::connect(jrabbit::Context{}.host("rabbit.jsys.zapto.org").port(5672).timeout(std::chrono::seconds{1}))
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
						std::cout << "MSG: " << e.data() << std::endl;
					}
				} catch (std::runtime_error &e) {

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
}

int main(int argc, char *argv[]) {
	::testing::InitGoogleTest(&argc, argv);

	::testing::AddGlobalTestEnvironment(new Environment{});

	return RUN_ALL_TESTS();
}
