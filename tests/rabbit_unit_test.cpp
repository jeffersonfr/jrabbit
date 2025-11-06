#include "jrabbit/Rabbit.hpp"

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <iostream>

int main(int argc, char *argv[]) {
	jrabbit::RabbitMq::connect(jrabbit::Context{}.host("rabbit.jsys.zapto.org").port(5672).timeout(std::chrono::seconds{1}).channel(1))
		.and_then([](jrabbit::RabbitMq mq) -> std::expected<jrabbit::RabbitMq, std::string> {
				auto ex = jrabbit::Exchange{"exchange1"}.type(jrabbit::Exchange::Type::FANOUT);
				auto q1 = jrabbit::Queue{"queue1"};
				auto q2 = jrabbit::Queue{"queue2"};
				auto q3 = jrabbit::Queue{"queue3"};
				auto rk = jrabbit::RoutingKey{};

				// -- creating objects
				mq.declare_exchange(ex);

				mq.declare_queue(q1);
				// mq.declare_queue(q3);

				// -- binding objects
				mq.bind(ex, q1);
				// mq.bind(ex, q2);
				// mq.bind(ex, q3);

				// -- send messsage
				mq.publish(ex, jrabbit::Message{"testando mensagem 1 ..."});
				mq.publish(ex, jrabbit::Message{"testando mensagem 2 ..."});
				mq.publish(ex, jrabbit::Message{"testando mensagem 3 ..."});

				// -- reading q1
				try {
					for (auto const &e: mq.consume(q1, jrabbit::RoutingKey{}, std::chrono::seconds{1})) {
						std::cout << "MSG: " << e.data() << std::endl;
					}
				} catch (std::runtime_error &e) {

				}

				// -- unbinding objects
				mq.unbind(ex, q1);
				// mq.unbind(ex, q2);
				// mq.unbind(ex, q3);

				// -- delete objects
				mq.delete_queue(q1);
				// mq.delete_queue(q2);
				// mq.delete_queue(q3);

				mq.delete_exchange(ex);

				return std::move(mq);
		});

	return 0;
}
