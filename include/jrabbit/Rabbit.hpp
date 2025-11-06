#pragma once

#include <chrono>
#include <cstdio>
#include <expected>
#include <iostream>
#include <generator>

#include <rabbitmq-c/amqp.h>
#include <rabbitmq-c/tcp_socket.h>

namespace jrabbit {
  struct Context {
    Context() = default;

    Context &host(std::string const &value) {
      mHost = value;

      return *this;
    }

    [[nodiscard]] std::string const &host() const {
      return mHost;
    }

    Context &port(int value) {
      mPort = value;

      return *this;
    }

    [[nodiscard]] int port() const {
      return mPort;
    }

    Context &timeout(std::chrono::milliseconds value) {
      mTimeout = value;

      return *this;
    }

    [[nodiscard]] std::chrono::milliseconds timeout() const {
      return mTimeout;
    }

    Context &channel(int value) {
      mChannel = value;

      return *this;
    }

    [[nodiscard]] int channel() const {
      return mChannel;
    }

    Context &user(std::string const &value) {
      mUser = value;

      return *this;
    }

    [[nodiscard]] std::string const &user() const {
      return mUser;
    }

    Context &pass(std::string const &value) {
      mPass = value;

      return *this;
    }

    [[nodiscard]] std::string const &pass() const {
      return mPass;
    }

  private:
    std::string mHost{"localhost"};
    std::string mUser{"guest"};
    std::string mPass{"guest"};
    std::chrono::milliseconds mTimeout{1000};
    int mChannel{1u};
    int mPort{5672};
  };

  struct Exchange {
    enum class Type {
      FANOUT,
      DIRECT,
      TOPIC
    };

    Exchange() = default;

    explicit Exchange(std::string name)
      : mName{std::move(name)} {
    }

    Exchange &name(std::string const &value) {
      mName = value;

      return *this;
    }

    [[nodiscard]] std::string const &name() const {
      return mName;
    }

    Exchange &type(Type value) {
      mType = value;

      return *this;
    }

    [[nodiscard]] Type type() const {
      return mType;
    }

    Exchange &passive(bool value) {
      mPassive = value;

      return *this;
    }

    [[nodiscard]] bool passive() const {
      return mPassive;
    }

    Exchange &durable(bool value) {
      mDurable = value;

      return *this;
    }

    [[nodiscard]] bool durable() const {
      return mDurable;
    }

    Exchange &auto_delete(bool value) {
      mAutoDelete = value;

      return *this;
    }

    [[nodiscard]] bool auto_delete() const {
      return mAutoDelete;
    }

    Exchange &internal(bool value) {
      mInternal = value;

      return *this;
    }

    [[nodiscard]] bool internal() const {
      return mInternal;
    }

  private:
    std::string mName{};
    bool mPassive{};
    bool mDurable{};
    bool mAutoDelete{};
    bool mInternal{};
    Type mType{Type::FANOUT};
  };

  struct RoutingKey {
    RoutingKey() = default;

    explicit RoutingKey(std::string name)
      : mName{std::move(name)} {
    }

    RoutingKey &name(std::string const &value) {
      mName = value;

      return *this;
    }

    [[nodiscard]] std::string const &name() const {
      return mName;
    }

  private:
    std::string mName{};
  };

  struct Queue {
    Queue() = default;

    explicit Queue(std::string name)
      : mName{std::move(name)} {
    }

    Queue &name(std::string const &value) {
      mName = value;

      return *this;
    }

    [[nodiscard]] std::string const &name() const {
      return mName;
    }

    Queue &passive(bool value) {
      mPassive = value;

      return *this;
    }

    [[nodiscard]] bool passive() const {
      return mPassive;
    }

    Queue &durable(bool value) {
      mDurable = value;

      return *this;
    }

    [[nodiscard]] bool durable() const {
      return mDurable;
    }

    Queue &exclusive(bool value) {
      mExclusive = value;

      return *this;
    }

    [[nodiscard]] bool exclusive() const {
      return mExclusive;
    }

    Queue &auto_delete(bool value) {
      mAutoDelete = value;

      return *this;
    }

    [[nodiscard]] bool auto_delete() const {
      return mAutoDelete;
    }

  private:
    std::string mName{""};
    bool mPassive{};
    bool mDurable{};
    bool mExclusive{};
    bool mAutoDelete{};
  };

  struct Message {
    explicit Message(std::string message)
      : mMessage{std::move(message)} {
    }

    [[nodiscard]] std::string const &data() const {
      return mMessage;
    }

    Message &mandatory(bool value) {
      mMandatory = value;

      return *this;
    }

    [[nodiscard]] bool mandatory() const {
      return mMandatory;
    }

    Message &immediate(bool value) {
      mImmediate = value;

      return *this;
    }

    [[nodiscard]] bool immediate() const {
      return mImmediate;
    }

  private:
    std::string mMessage{};
    bool mMandatory{};
    bool mImmediate{};
  };

  struct Envelope {
    explicit Envelope(std::string message)
      : mMessage{std::move(message)} {
    }

    [[nodiscard]] std::string const &data() const {
      return mMessage;
    }

    Envelope &delivery_tag(uint64_t value) {
      mDeliveryTag = value;

      return *this;
    }

    [[nodiscard]] uint64_t delivery_tag() const {
      return mDeliveryTag;
    }

    Envelope &consumer_tag(std::string const &value) {
      mConsumerTag = value;

      return *this;
    }

    [[nodiscard]] std::string const &consumer_tag() const {
      return mConsumerTag;
    }

    Envelope &exchange(std::string const &value) {
      mExchange = value;

      return *this;
    }

    [[nodiscard]] std::string const &exchange() const {
      return mExchange;
    }

    Envelope &routing_key(std::string const &value) {
      mRoutingKey = value;

      return *this;
    }

    [[nodiscard]] std::string const &routing_key() const {
      return mRoutingKey;
    }

    Envelope &redelivered(bool value) {
      mRedelivered = value;

      return *this;
    }

    [[nodiscard]] bool redelivered() const {
      return mRedelivered;
    }

    Envelope &mandatory(bool value) {
      mMandatory = value;

      return *this;
    }

    [[nodiscard]] bool mandatory() const {
      return mMandatory;
    }

  private:
    std::string mMessage{};
    std::string mConsumerTag{};
    std::string mExchange{};
    std::string mRoutingKey{};
    uint64_t mDeliveryTag{};
    bool mRedelivered{};
    bool mMandatory{};
  };

  struct RabbitMq {
    [[nodiscard]] static std::expected<RabbitMq, std::string> connect(Context const &context) {
      try {
        return RabbitMq{context};
      } catch (const std::exception &e) {
        return std::unexpected{e.what()};
      }
    }

    RabbitMq(RabbitMq const &) = delete;

    RabbitMq(RabbitMq &&other) noexcept
      : mContext{std::move(other.mContext)}, mConnection{other.mConnection}, mSocket{other.mSocket} {
      other.mConnection = nullptr;
      other.mSocket = nullptr;
    }

    ~RabbitMq() {
      release();
    }

    RabbitMq &operator=(RabbitMq const &) = delete;

    RabbitMq &operator=(RabbitMq &&) = delete;

    void release() const {
      if (!mConnection or !mSocket) {
        return;
      }

      if (auto result = amqp_error(amqp_channel_close(mConnection, 1, AMQP_REPLY_SUCCESS)); result) {
        std::cerr << result.value() << std::endl;
      }

      if (auto result = amqp_error(amqp_connection_close(mConnection, AMQP_REPLY_SUCCESS)); result) {
        std::cerr << result.value() << std::endl;
      }

      if (auto result = amqp_destroy_connection(mConnection); result != AMQP_STATUS_OK) {
        std::cerr << amqp_error_string2(result) << std::endl;
      }
    }

    void declare_exchange(Exchange const &exchange) const {
      std::string exchangeType = "fanout";

      if (exchange.type() == Exchange::Type::DIRECT) {
        exchangeType = "direct";
      } else if (exchange.type() == Exchange::Type::TOPIC) {
        exchangeType = "topic";
      }

      amqp_exchange_declare(mConnection, mContext.channel(), amqp_cstring_bytes(exchange.name().c_str()),
                            amqp_cstring_bytes(exchangeType.c_str()), exchange.passive() ? 1 : 0,
                            exchange.durable() ? 1 : 0, exchange.auto_delete() ? 1 : 0, exchange.internal() ? 1 : 0,
                            amqp_empty_table);

      if (auto result = amqp_error(amqp_get_rpc_reply(mConnection)); result) {
        throw std::runtime_error(result.value());
      }
    }

    void delete_exchange(Exchange const &exchange, bool ifUnused = false) const {
      amqp_exchange_delete(mConnection, mContext.channel(), amqp_cstring_bytes(exchange.name().c_str()), ifUnused);

      if (auto result = amqp_error(amqp_get_rpc_reply(mConnection)); result) {
        throw std::runtime_error(result.value());
      }
    }

    void declare_queue(Queue const &queue) const {
      amqp_table_t args{
        .num_entries = 0,
        .entries = nullptr
      };

      amqp_queue_declare(mConnection, mContext.channel(), amqp_cstring_bytes(queue.name().c_str()),
                         queue.passive() ? 1 : 0,
                         queue.durable() ? 1 : 0, queue.exclusive() ? 1 : 0, queue.auto_delete() ? 1 : 0, args);

      if (auto result = amqp_error(amqp_get_rpc_reply(mConnection)); result) {
        throw std::runtime_error(result.value());
      }

      // amqp_release_buffers(mConnection);
    }

    void delete_queue(Queue const &queue, bool ifUnused = false, bool ifEmpty = false) const {
      amqp_queue_delete(mConnection, mContext.channel(), amqp_cstring_bytes(queue.name().c_str()), ifUnused ? 1 : 0,
                        ifEmpty ? 1 : 0);

      if (auto result = amqp_error(amqp_get_rpc_reply(mConnection)); result) {
        throw std::runtime_error(result.value());
      }
    }

    void purge_queue(Queue const &queue) const {
      amqp_queue_purge(mConnection, mContext.channel(), amqp_cstring_bytes(queue.name().c_str()));

      if (auto result = amqp_error(amqp_get_rpc_reply(mConnection)); result) {
        throw std::runtime_error(result.value());
      }
    }

    void bind(Exchange const &exchange, Queue const &queue, RoutingKey const &routingKey = {}) const {
      amqp_queue_bind(mConnection, mContext.channel(), amqp_cstring_bytes(queue.name().c_str()),
                      amqp_cstring_bytes(exchange.name().c_str()), amqp_cstring_bytes(routingKey.name().c_str()),
                      amqp_empty_table);

      if (auto result = amqp_error(amqp_get_rpc_reply(mConnection)); result) {
        throw std::runtime_error(result.value());
      }
    }

    void unbind(Exchange const &exchange, Queue const &queue, RoutingKey const &routingKey = {}) const {
      amqp_queue_unbind(mConnection, mContext.channel(), amqp_cstring_bytes(queue.name().c_str()),
                        amqp_cstring_bytes(exchange.name().c_str()), amqp_cstring_bytes(routingKey.name().c_str()),
                        amqp_empty_table);

      if (auto result = amqp_error(amqp_get_rpc_reply(mConnection)); result) {
        throw std::runtime_error(result.value());
      }
    }

    void publish(Exchange const &exchange, Message const &message, RoutingKey const &routingKey = {}) const {
      if (auto result = amqp_basic_publish(mConnection, 1, amqp_cstring_bytes(exchange.name().c_str()),
                                           amqp_cstring_bytes(routingKey.name().c_str()), message.mandatory(),
                                           message.immediate(), nullptr,
                                           amqp_cstring_bytes(message.data().c_str())); result != AMQP_STATUS_OK) {
        throw std::runtime_error{amqp_error_string2(result)};
      }

      if (auto result = amqp_error(amqp_get_rpc_reply(mConnection)); result) {
        throw std::runtime_error(result.value());
      }

      /*
       If we've done things correctly we can get one of 4 things back from the broker
        - basic.ack - our channel is in confirm mode, messsage was 'dealt with' by the broker
        - basic.nack - our channel is in confirm mode, queue has max-length set and is full, queue overflow stratege is reject-publish
        - basic.return then basic.ack - the message wasn't delievered, but was dealt with
        - channel.close - probably tried to publish to a non-existant exchange, in any case error!
        - connection.clsoe - something really bad happened
       */

      amqp_maybe_release_buffers_on_channel(mConnection, mContext.channel());
    }

    [[nodiscard]] std::generator<Envelope> consume(Queue const &queue, RoutingKey const &routingKey = {},
                                                   std::chrono::milliseconds timeout = {}, bool noLocal = {},
                                                   bool noAck = {true}, bool exclusive = {}) const {
      amqp_basic_consume(mConnection, mContext.channel(), amqp_cstring_bytes(queue.name().c_str()),
                         amqp_cstring_bytes(routingKey.name().c_str()), noLocal ? 1 : 0, noAck ? 1 : 0,
                         exclusive ? 1 : 0, amqp_empty_table);

      if (auto result = amqp_error(amqp_get_rpc_reply(mConnection)); result) {
        throw std::runtime_error(result.value());
      }

      struct timeval tval{
        .tv_sec = mContext.timeout().count() / 1000,
        .tv_usec = mContext.timeout().count() * 1000
      };
      amqp_frame_t frame;

      while (true) {
        amqp_envelope_t envelope;
        amqp_rpc_reply_t ret;

        amqp_maybe_release_buffers(mConnection);

        ret = amqp_consume_message(mConnection, &envelope, (timeout.count() > 0) ? &tval : nullptr, 0);

        // ... this code could be reduced, but there is a exception that might be treated when 'frame.payload.method.id == AMQP_BASIC_RETURN_METHOD'
        if (AMQP_RESPONSE_NORMAL == ret.reply_type) {
          auto msg = Envelope{{static_cast<char *>(envelope.message.body.bytes), envelope.message.body.len}}
          .delivery_tag((unsigned) envelope.delivery_tag)
          .consumer_tag({static_cast<char *>(envelope.consumer_tag.bytes), envelope.consumer_tag.len})
          .redelivered(envelope.redelivered)
          .exchange({static_cast<char *>(envelope.exchange.bytes), envelope.exchange.len})
          .routing_key({static_cast<char *>(envelope.routing_key.bytes), envelope.routing_key.len});

          amqp_destroy_envelope(&envelope);

          co_yield std::move(msg);
        }

        if (ret.reply_type == AMQP_RESPONSE_NONE) {
          co_return;
        }

        if (ret.reply_type == AMQP_RESPONSE_SERVER_EXCEPTION) {
          if (ret.reply.id == AMQP_CONNECTION_CLOSE_METHOD) {
            auto *context = static_cast<amqp_connection_close_t *>(ret.reply.decoded);
            auto sv = std::string_view{static_cast<char *>(context->reply_text.bytes), context->reply_text.len};

            throw std::runtime_error{std::format("server connection error {}, message: {}\n", context->reply_code, sv)};
          }

          if (ret.reply.id == AMQP_CHANNEL_CLOSE_METHOD) {
            auto *context = static_cast<amqp_channel_close_t *>(ret.reply.decoded);
            auto sv = std::string_view{static_cast<char *>(context->reply_text.bytes), context->reply_text.len};

            throw std::runtime_error{std::format("server channel error {}, message: {}\n", context->reply_code, sv)};
          }

          throw std::runtime_error{std::format("unknown server error, method id {}", ret.reply.id)};
        }

        if (ret.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION) {
          if (ret.library_error != AMQP_STATUS_UNEXPECTED_STATE) {
            throw std::runtime_error{amqp_error_string2(ret.library_error)};
          }

          if (AMQP_STATUS_OK != amqp_simple_wait_frame(mConnection, &frame)) {
            co_return;
          }

          if (frame.frame_type == AMQP_FRAME_METHOD) {
            if (frame.payload.method.id == AMQP_BASIC_ACK_METHOD) {
              // if we've turned publisher confirms on, and we've published a message here is a message being confirmed.
            } else if (frame.payload.method.id == AMQP_BASIC_RETURN_METHOD) {
              // if a published message couldn't be routed and the mandatory flag was set this is what would be
              // returned. The message then needs to be read.
              amqp_message_t message;

              if (auto result = amqp_error(amqp_read_message(mConnection, frame.channel, &message, 0)); result) {
                throw std::runtime_error(result.value());
              }

              auto msg = Envelope{{static_cast<char *>(message.body.bytes), message.body.len}}.mandatory(true);

              amqp_destroy_message(&message);

              co_yield std::move(msg);
            } else if (frame.payload.method.id == AMQP_CHANNEL_CLOSE_METHOD) {
              // a channel.close method happens when a channel exception occurs, this can happen by publishing to an
              // exchange that doesn't existfor example. In this case you would need to open another channel redeclare
              // any queues that were declared auto-delete, and restart any consumers that were attached to the previous
              // channel.
              co_return;
            } else if (frame.payload.method.id == AMQP_CONNECTION_CLOSE_METHOD) {
              // a connection.close method happens when a connection exception occurs, this can happen by trying to
              // use a channel that isn't open for example. In this case the whole connection must be restarted.
              co_return;
            } else {
              throw std::runtime_error{std::format("an unexpected method '{}' was received", frame.payload.method.id)};
            }
          }
        }
      }
    }

    void ack(Envelope const &envelope, bool multiple = {}) const {
      amqp_basic_ack(mConnection, mContext.channel(), envelope.delivery_tag(), multiple ? 1 : 0);

      if (auto result = amqp_error(amqp_get_rpc_reply(mConnection)); result) {
        throw std::runtime_error(result.value());
      }
    }

    void nack(Envelope const &envelope, bool requeue = {}, bool multiple = {}) const {
      amqp_basic_nack(mConnection, mContext.channel(), envelope.delivery_tag(), multiple ? 1 : 0, requeue ? 1 : 0);

      if (auto result = amqp_error(amqp_get_rpc_reply(mConnection)); result) {
        throw std::runtime_error(result.value());
      }
    }

    void reject(Envelope const &envelope, bool requeue = {}) const {
      amqp_basic_reject(mConnection, mContext.channel(), envelope.delivery_tag(), requeue ? 1 : 0);

      if (auto result = amqp_error(amqp_get_rpc_reply(mConnection)); result) {
        throw std::runtime_error(result.value());
      }
    }

    void recover(bool requeue = {}) const {
      amqp_basic_recover(mConnection, mContext.channel(), requeue ? 1 : 0);

      if (auto result = amqp_error(amqp_get_rpc_reply(mConnection)); result) {
        throw std::runtime_error(result.value());
      }

      amqp_maybe_release_buffers_on_channel(mConnection, mContext.channel());
    }

    void qos(const std::string &consumer_tag, uint32_t prefetchSize, uint16_t prefetchCount = {1},
             bool global = {}) const {
      amqp_basic_qos(mConnection, mContext.channel(), prefetchSize, prefetchCount, global ? 1 : 0);

      if (auto result = amqp_error(amqp_get_rpc_reply(mConnection)); result) {
        throw std::runtime_error(result.value());
      }

      amqp_maybe_release_buffers_on_channel(mConnection, mContext.channel());
    }

    void cancel(const std::string &consumerTag) const {
      amqp_basic_cancel(mConnection, mContext.channel(), amqp_cstring_bytes(consumerTag.c_str()));

      if (auto result = amqp_error(amqp_get_rpc_reply(mConnection)); result) {
        throw std::runtime_error(result.value());
      }

      amqp_maybe_release_buffers_on_channel(mConnection, mContext.channel());
    }

  private:
    Context mContext;
    amqp_connection_state_t mConnection{};
    amqp_socket_t *mSocket{nullptr};

    explicit RabbitMq(Context context)
      : mContext(std::move(context)) {
      mConnection = amqp_new_connection();
      mSocket = amqp_tcp_socket_new(mConnection);

      if (!mSocket) {
        throw std::runtime_error{"unable to connect to host"};
      }

      if (mContext.timeout().count() > 0) {
        struct timeval tval{
          .tv_sec = mContext.timeout().count() / 1000,
          .tv_usec = mContext.timeout().count() * 1000
        };

        if (amqp_socket_open_noblock(mSocket, mContext.host().c_str(), mContext.port(), &tval) != AMQP_STATUS_OK) {
          throw std::runtime_error{"unable to create socket connection"};
        }
      } else {
        if (amqp_socket_open(mSocket, mContext.host().c_str(), mContext.port()) != AMQP_STATUS_OK) {
          throw std::runtime_error{"unable to create socket connection"};
        }
      }

      if (auto result = amqp_error(amqp_login(mConnection, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN,
                                              mContext.user().c_str(),
                                              mContext.pass().c_str())); result) {
        throw std::runtime_error{result.value()};
      }

      auto channel = amqp_channel_open(mConnection, 1);

      if (channel == nullptr) {
        throw std::runtime_error{"unable to open/create a channel"};
      }

      if (auto result = amqp_error(amqp_get_rpc_reply(mConnection)); result) {
        throw std::runtime_error{result.value()};
      }
    }

    static std::optional<std::string> amqp_error(amqp_rpc_reply_t x) {
      if (x.reply_type == AMQP_RESPONSE_NONE) {
        return "missing rpc reply type";
      }

      if (x.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION) {
        return amqp_error_string2(x.library_error);
      }

      if (x.reply_type == AMQP_RESPONSE_SERVER_EXCEPTION) {
        if (x.reply.id == AMQP_CONNECTION_CLOSE_METHOD) {
          auto *context = static_cast<amqp_connection_close_t *>(x.reply.decoded);
          auto sv = std::string_view{static_cast<char *>(context->reply_text.bytes), context->reply_text.len};
          return std::format("server connection error {}, message: {}\n", context->reply_code, sv);
        }

        if (x.reply.id == AMQP_CHANNEL_CLOSE_METHOD) {
          auto *context = static_cast<amqp_channel_close_t *>(x.reply.decoded);
          auto sv = std::string_view{static_cast<char *>(context->reply_text.bytes), context->reply_text.len};
          return std::format("server channel error {}, message: {}\n", context->reply_code, sv);
        }

        return std::format("unknown server error, method id {}", x.reply.id);
      }

      return {};
    }
  };
}

