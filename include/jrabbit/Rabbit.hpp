#pragma once

#include <chrono>
#include <cstdio>
#include <expected>
#include <functional>
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

    Context &virtual_host(std::string const &value) {
      mVirtualHost = value;

      return *this;
    }

    [[nodiscard]] std::string const &virtual_host() const {
      return mVirtualHost;
    }

    Context &frame(int value) {
      mFrame = value;

      // [4096 .. 2 ^ 31 - 1]
      if (mFrame < 4096 or mFrame > 131072) {
        throw std::runtime_error{"invalid frame size range"};
      }

      return *this;
    }

    [[nodiscard]] int frame() const {
      return mFrame;
    }

  private:
    std::string mHost{"localhost"};
    std::string mUser{"guest"};
    std::string mPass{"guest"};
    std::string mVirtualHost{"/"};
    std::chrono::milliseconds mTimeout{1000};
    int mPort{5672};
    int mFrame{4096};
  };

  struct Params {
    friend class Channel;

    Params() = default;

    Params & put_void(char const *key) {
      ::amqp_table_entry_t entry;

      entry.key = amqp_cstring_bytes(key);
      entry.value.kind = AMQP_FIELD_KIND_VOID;

      mParams.push_back(entry);

      return *this;
    }

    Params & put_bool(char const *key, bool value) {
      ::amqp_table_entry_t entry;

      entry.key = amqp_cstring_bytes(key);
      entry.value.kind = AMQP_FIELD_KIND_BOOLEAN;
      entry.value.value.boolean = value;

      mParams.push_back(entry);

      return *this;
    }

    Params & put_int16(char const *key, int16_t value) {
      ::amqp_table_entry_t entry;

      entry.key = amqp_cstring_bytes(key);
      entry.value.kind = AMQP_FIELD_KIND_I16;
      entry.value.value.i16 = value;

      mParams.push_back(entry);

      return *this;
    }

    Params & put_int32(char const *key, int32_t value) {
      ::amqp_table_entry_t entry;

      entry.key = amqp_cstring_bytes(key);
      entry.value.kind = AMQP_FIELD_KIND_I32;
      entry.value.value.i32 = value;

      mParams.push_back(entry);

      return *this;
    }

    Params & put_int64(char const *key, int64_t value) {
      ::amqp_table_entry_t entry;

      entry.key = amqp_cstring_bytes(key);
      entry.value.kind = AMQP_FIELD_KIND_I64;
      entry.value.value.i64 = value;

      mParams.push_back(entry);

      return *this;
    }

    Params & put_uint8(char const *key, uint8_t value) {
      ::amqp_table_entry_t entry;

      entry.key = amqp_cstring_bytes(key);
      entry.value.kind = AMQP_FIELD_KIND_U8;
      entry.value.value.u8 = value;

      mParams.push_back(entry);

      return *this;
    }

    Params & put_uint16(char const *key, uint16_t value) {
      ::amqp_table_entry_t entry;

      entry.key = amqp_cstring_bytes(key);
      entry.value.kind = AMQP_FIELD_KIND_U16;
      entry.value.value.u16 = value;

      mParams.push_back(entry);

      return *this;
    }

    Params & put_uint32(char const *key, uint32_t value) {
      ::amqp_table_entry_t entry;

      entry.key = amqp_cstring_bytes(key);
      entry.value.kind = AMQP_FIELD_KIND_U32;
      entry.value.value.u32 = value;

      mParams.push_back(entry);

      return *this;
    }

    Params & put_uint64(char const *key, uint64_t value) {
      ::amqp_table_entry_t entry;

      entry.key = amqp_cstring_bytes(key);
      entry.value.kind = AMQP_FIELD_KIND_U64;
      entry.value.value.u64 = value;

      mParams.push_back(entry);

      return *this;
    }

    Params & put_float32(char const *key, float value) {
      ::amqp_table_entry_t entry;

      entry.key = amqp_cstring_bytes(key);
      entry.value.kind = AMQP_FIELD_KIND_F32;
      entry.value.value.f32 = value;

      mParams.push_back(entry);

      return *this;
    }

    Params & put_float64(char const *key, double value) {
      ::amqp_table_entry_t entry;

      entry.key = amqp_cstring_bytes(key);
      entry.value.kind = AMQP_FIELD_KIND_F64;
      entry.value.value.f64 = value;

      mParams.push_back(entry);

      return *this;
    }

    Params & put_text(char const *key, char const *value) {
      ::amqp_table_entry_t entry;

      entry.key = amqp_cstring_bytes(key);
      entry.value.kind = AMQP_FIELD_KIND_UTF8;
      entry.value.value.bytes = amqp_cstring_bytes(value);

      mParams.push_back(entry);

      return *this;
    }

    Params & put_bytes(char const *key, char const *value) {
      ::amqp_table_entry_t entry;

      entry.key = amqp_cstring_bytes(key);
      entry.value.kind = AMQP_FIELD_KIND_BYTES;
      entry.value.value.bytes = amqp_cstring_bytes(value);

      mParams.push_back(entry);

      return *this;
    }

    [[nodiscard]] amqp_table_t get_params() const {
      if (mParams.empty()) {
        return amqp_empty_table;
      }

      amqp_table_t table;

      table.entries = const_cast<::amqp_table_entry_t *>(mParams.data()),
      table.num_entries = static_cast<int>(mParams.size());

      return table;
    }

  private:
    std::vector<::amqp_table_entry_t> mParams;
  };

  struct Properties {
    enum class DeliveryMode {
      Persistent,
      NonPersistent
    };

    Properties()
      : mProperties{} {}

    Properties &content_type(std::string const &value) {
      mProperties._flags = AMQP_BASIC_CONTENT_TYPE_FLAG;
      mProperties.content_type = amqp_cstring_bytes(strdup("text/plain"));

      return *this;
    }

    Properties &delivery_mode(DeliveryMode mode) {
      mProperties._flags = AMQP_BASIC_DELIVERY_MODE_FLAG;

      if (mode == DeliveryMode::Persistent) {
        mProperties.delivery_mode = 1;
      } else {
        mProperties.delivery_mode = 2;
      }

      return *this;
    }

    Properties &headers(Params &&params) {
      mParams = std::move(params);

      mProperties._flags = AMQP_BASIC_HEADERS_FLAG;
      mProperties.headers = mParams.get_params();

      return *this;
    }

    [[nodiscard]] amqp_basic_properties_t const * get_properties() const {
      return &mProperties;
    }

  private:
    amqp_basic_properties_t mProperties;
    Params mParams;
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
    std::string mName{};
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

  struct RabbitMq;

  struct Channel {
    friend struct RabbitMq;

    ~Channel() {
      if (auto result = amqp_error(amqp_channel_close(mState, mChannel, AMQP_REPLY_SUCCESS)); result) {
        std::cerr << result.value() << std::endl;
      }
    }

    void declare_exchange(Exchange const &exchange, Params const &params = Params{}) const {
      std::string exchangeType = "fanout";

      if (exchange.type() == Exchange::Type::DIRECT) {
        exchangeType = "direct";
      } else if (exchange.type() == Exchange::Type::TOPIC) {
        exchangeType = "topic";
      }

      amqp_exchange_declare(mState, mChannel, amqp_cstring_bytes(exchange.name().c_str()),
                            amqp_cstring_bytes(exchangeType.c_str()), exchange.passive() ? 1 : 0,
                            exchange.durable() ? 1 : 0, exchange.auto_delete() ? 1 : 0, exchange.internal() ? 1 : 0,
                            params.get_params());

      if (auto result = amqp_error(amqp_get_rpc_reply(mState)); result) {
        throw std::runtime_error(result.value());
      }
    }

    void delete_exchange(Exchange const &exchange, bool ifUnused = false) const {
      amqp_exchange_delete(mState, mChannel, amqp_cstring_bytes(exchange.name().c_str()), ifUnused);

      if (auto result = amqp_error(amqp_get_rpc_reply(mState)); result) {
        throw std::runtime_error(result.value());
      }
    }

    void declare_queue(Queue const &queue, Params const &params = Params{}) const {
      amqp_queue_declare(mState, mChannel, amqp_cstring_bytes(queue.name().c_str()),
                         queue.passive() ? 1 : 0,
                         queue.durable() ? 1 : 0, queue.exclusive() ? 1 : 0, queue.auto_delete() ? 1 : 0, params.get_params());

      if (auto result = amqp_error(amqp_get_rpc_reply(mState)); result) {
        throw std::runtime_error(result.value());
      }

      // amqp_release_buffers(mState);
    }

    void delete_queue(Queue const &queue, bool ifUnused = false, bool ifEmpty = false) const {
      amqp_queue_delete(mState, mChannel, amqp_cstring_bytes(queue.name().c_str()), ifUnused ? 1 : 0,
                        ifEmpty ? 1 : 0);

      if (auto result = amqp_error(amqp_get_rpc_reply(mState)); result) {
        throw std::runtime_error(result.value());
      }
    }

    void purge_queue(Queue const &queue) const {
      amqp_queue_purge(mState, mChannel, amqp_cstring_bytes(queue.name().c_str()));

      if (auto result = amqp_error(amqp_get_rpc_reply(mState)); result) {
        throw std::runtime_error(result.value());
      }
    }

    void bind(Exchange const &exchange, Queue const &queue, RoutingKey const &routingKey = {}, Params const &params = Params{}) const {
      amqp_queue_bind(mState, mChannel, amqp_cstring_bytes(queue.name().c_str()),
                      amqp_cstring_bytes(exchange.name().c_str()), amqp_cstring_bytes(routingKey.name().c_str()),
                      params.get_params());

      if (auto result = amqp_error(amqp_get_rpc_reply(mState)); result) {
        throw std::runtime_error(result.value());
      }
    }

    void unbind(Exchange const &exchange, Queue const &queue, RoutingKey const &routingKey = {}, Params const &params = Params{}) const {
      amqp_queue_unbind(mState, mChannel, amqp_cstring_bytes(queue.name().c_str()),
                        amqp_cstring_bytes(exchange.name().c_str()), amqp_cstring_bytes(routingKey.name().c_str()),
                        params.get_params());

      if (auto result = amqp_error(amqp_get_rpc_reply(mState)); result) {
        throw std::runtime_error(result.value());
      }
    }

    void publish(Exchange const &exchange, Message const &message, RoutingKey const &routingKey = {}, Properties const &properties = {}) const {
      if (auto result = amqp_basic_publish(mState, 1, amqp_cstring_bytes(exchange.name().c_str()),
                                           amqp_cstring_bytes(routingKey.name().c_str()), message.mandatory(),
                                           message.immediate(), properties.get_properties(),
                                           amqp_cstring_bytes(message.data().c_str())); result != AMQP_STATUS_OK) {
        throw std::runtime_error{amqp_error_string2(result)};
      }

      if (auto result = amqp_error(amqp_get_rpc_reply(mState)); result) {
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

      amqp_maybe_release_buffers_on_channel(mState, mChannel);
    }

    [[nodiscard]] std::generator<Envelope> consume(Queue const &queue, RoutingKey const &routingKey = {},
                                                   std::chrono::milliseconds timeout = {}, bool noLocal = {},
                                                   bool noAck = {true}, bool exclusive = {}, Params const &params = Params{}) const {
      amqp_basic_consume(mState, mChannel, amqp_cstring_bytes(queue.name().c_str()),
                         amqp_cstring_bytes(routingKey.name().c_str()), noLocal ? 1 : 0, noAck ? 1 : 0,
                         exclusive ? 1 : 0, params.get_params());

      if (auto result = amqp_error(amqp_get_rpc_reply(mState)); result) {
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

        amqp_maybe_release_buffers_on_channel(mState, mChannel);

        ret = amqp_consume_message(mState, &envelope, (timeout.count() > 0) ? &tval : nullptr, 0);

        // ... this code could be reduced, but there is a exception that might be treated when 'frame.payload.method.id == AMQP_BASIC_RETURN_METHOD'
        if (AMQP_RESPONSE_NORMAL == ret.reply_type) {
          auto msg = Envelope{{static_cast<char *>(envelope.message.body.bytes), envelope.message.body.len}}
              .delivery_tag(envelope.delivery_tag)
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

          if (AMQP_STATUS_OK != amqp_simple_wait_frame(mState, &frame)) {
            co_return;
          }

          if (frame.frame_type == AMQP_FRAME_METHOD) {
            if (frame.payload.method.id == AMQP_BASIC_ACK_METHOD) {
              // if we've turned publisher confirms on, and we've published a message here is a message being confirmed.
            } else if (frame.payload.method.id == AMQP_BASIC_RETURN_METHOD) {
              // if a published message couldn't be routed and the mandatory flag was set this is what would be
              // returned. The message then needs to be read.
              amqp_message_t message;

              if (auto result = amqp_error(amqp_read_message(mState, frame.channel, &message, 0)); result) {
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
      amqp_basic_ack(mState, mChannel, envelope.delivery_tag(), multiple ? 1 : 0);

      if (auto result = amqp_error(amqp_get_rpc_reply(mState)); result) {
        throw std::runtime_error(result.value());
      }
    }

    void nack(Envelope const &envelope, bool requeue = {}, bool multiple = {}) const {
      amqp_basic_nack(mState, mChannel, envelope.delivery_tag(), multiple ? 1 : 0, requeue ? 1 : 0);

      if (auto result = amqp_error(amqp_get_rpc_reply(mState)); result) {
        throw std::runtime_error(result.value());
      }
    }

    void reject(Envelope const &envelope, bool requeue = {}) const {
      amqp_basic_reject(mState, mChannel, envelope.delivery_tag(), requeue ? 1 : 0);

      if (auto result = amqp_error(amqp_get_rpc_reply(mState)); result) {
        throw std::runtime_error(result.value());
      }
    }

    void recover(bool requeue = {}) const {
      amqp_basic_recover(mState, mChannel, requeue ? 1 : 0);

      if (auto result = amqp_error(amqp_get_rpc_reply(mState)); result) {
        throw std::runtime_error(result.value());
      }

      amqp_maybe_release_buffers_on_channel(mState, mChannel);
    }

    void qos(const std::string &consumer_tag, uint32_t prefetchSize, uint16_t prefetchCount = {1},
             bool global = {}) const {
      amqp_basic_qos(mState, mChannel, prefetchSize, prefetchCount, global ? 1 : 0);

      if (auto result = amqp_error(amqp_get_rpc_reply(mState)); result) {
        throw std::runtime_error(result.value());
      }

      amqp_maybe_release_buffers_on_channel(mState, mChannel);
    }

    void cancel(const std::string &consumerTag) const {
      amqp_basic_cancel(mState, mChannel, amqp_cstring_bytes(consumerTag.c_str()));

      if (auto result = amqp_error(amqp_get_rpc_reply(mState)); result) {
        throw std::runtime_error(result.value());
      }

      amqp_maybe_release_buffers_on_channel(mState, mChannel);
    }

    bool transaction(std::function<void(Channel &)> const &callback) {
      amqp_tx_select(mState, mChannel);
      amqp_get_rpc_reply(mState);

      try {
        callback(*this);
      } catch (const std::exception &e) {
        amqp_tx_rollback(mState, mChannel);

        if (auto result = amqp_error(amqp_get_rpc_reply(mState)); result) {
          throw std::runtime_error(result.value());
        }

        return false;
      }

      amqp_tx_commit(mState, mChannel);

      if (auto result = amqp_error(amqp_get_rpc_reply(mState)); result) {
        throw std::runtime_error(result.value());
      }

      return true;
    }

  private:
    Context &mContext;
    amqp_connection_state_t mState{};
    int mChannel{-1};

    Channel(Context &context, amqp_connection_state_t state, int channelId)
      : mContext{context}, mState{state}, mChannel{channelId} {
      auto channel = amqp_channel_open(mState, 1);

      if (channel == nullptr) {
        throw std::runtime_error{"unable to open/create a channel"};
      }

      if (auto result = amqp_error(amqp_get_rpc_reply(mState)); result) {
        throw std::runtime_error{result.value()};
      }
    }
  };

  struct RabbitMq {
    [[nodiscard]] static std::expected<RabbitMq, std::string> connect(jrabbit::Context const &context) {
      try {
        return RabbitMq{context};
      } catch (const std::exception &e) {
        return std::unexpected{e.what()};
      }
    }

    RabbitMq(RabbitMq const &) = delete;

    RabbitMq(RabbitMq &&other) noexcept
      : mContext{std::move(other.mContext)}, mState{other.mState}, mSocket{other.mSocket} {
      other.mState = nullptr;
      other.mSocket = nullptr;
    }

    ~RabbitMq() {
      if (mState != nullptr) {
        if (auto result = amqp_error(amqp_connection_close(mState, AMQP_REPLY_SUCCESS)); result) {
          std::cerr << result.value() << std::endl;
        }
      }

      if (mSocket != nullptr) {
        if (auto result = amqp_destroy_connection(mState); result != AMQP_STATUS_OK) {
          std::cerr << amqp_error_string2(result) << std::endl;
        }
      }
    }

    RabbitMq &operator=(RabbitMq const &) = delete;

    RabbitMq &operator=(RabbitMq &&) = delete;

    std::unique_ptr<Channel> open(int channel) {
      return std::unique_ptr<Channel>(new Channel{mContext, mState, channel});
    }

  private:
    Context mContext;
    amqp_connection_state_t mState{nullptr};
    amqp_socket_t *mSocket{nullptr};

    explicit RabbitMq(Context context)
      : mContext(std::move(context)) {
      mState = amqp_new_connection();
      mSocket = amqp_tcp_socket_new(mState);

      if (!mSocket) {
        amqp_connection_close(mState, AMQP_REPLY_SUCCESS);

        throw std::runtime_error{"unable to connect to host"};
      }

      if (mContext.timeout().count() > 0) {
        struct timeval tval{
          .tv_sec = mContext.timeout().count() / 1000,
          .tv_usec = mContext.timeout().count() * 1000
        };

        if (amqp_socket_open_noblock(mSocket, mContext.host().c_str(), mContext.port(), &tval) != AMQP_STATUS_OK) {
          amqp_connection_close(mState, AMQP_REPLY_SUCCESS);
          amqp_destroy_connection(mState);

          throw std::runtime_error{"unable to create socket connection"};
        }
      } else {
        if (amqp_socket_open(mSocket, mContext.host().c_str(), mContext.port()) != AMQP_STATUS_OK) {
          amqp_connection_close(mState, AMQP_REPLY_SUCCESS);
          amqp_destroy_connection(mState);

          throw std::runtime_error{"unable to create socket connection"};
        }
      }

      if (auto result = amqp_error(amqp_login(mState, mContext.virtual_host().c_str(), 0, mContext.frame(), 0,
                                              AMQP_SASL_METHOD_PLAIN, mContext.user().c_str(),
                                              mContext.pass().c_str())); result) {
        amqp_connection_close(mState, AMQP_REPLY_SUCCESS);
        amqp_destroy_connection(mState);

        throw std::runtime_error{result.value()};
      }
    }
  };
}
