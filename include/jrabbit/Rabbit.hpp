#pragma once

#include <chrono>
#include <cstdio>
#include <expected>
#include <functional>
#include <iostream>
#include <string_view>
#include <generator>
#include <unistd.h>

#include <rabbitmq-c/amqp.h>
#include <rabbitmq-c/tcp_socket.h>

#include "rabbitmq-c/ssl_socket.h"

namespace jrabbit {
  [[nodiscard]] static amqp_bytes_t amqp_string_view_bytes(std::string_view value) {
    amqp_bytes_t result;

    result.len = value.size();
    result.bytes = (void *)value.data();

    return result;
  }

  struct Params {
    friend class Channel;

    constexpr Params() = default;

    Params &put_void(std::string_view key) {
      ::amqp_table_entry_t entry;

      entry.key = amqp_string_view_bytes(key);
      entry.value.kind = AMQP_FIELD_KIND_VOID;

      mParams.push_back(entry);

      return *this;
    }

    Params &put_bool(std::string_view key, bool value) {
      ::amqp_table_entry_t entry;

      entry.key = amqp_string_view_bytes(key);
      entry.value.kind = AMQP_FIELD_KIND_BOOLEAN;
      entry.value.value.boolean = value;

      mParams.push_back(entry);

      return *this;
    }

    Params &put_int8(std::string_view key, int8_t value) {
      ::amqp_table_entry_t entry;

      entry.key = amqp_string_view_bytes(key);
      entry.value.kind = AMQP_FIELD_KIND_I8;
      entry.value.value.i8 = value;

      mParams.push_back(entry);

      return *this;
    }

    Params &put_int16(std::string_view key, int16_t value) {
      ::amqp_table_entry_t entry;

      entry.key = amqp_string_view_bytes(key);
      entry.value.kind = AMQP_FIELD_KIND_I16;
      entry.value.value.i16 = value;

      mParams.push_back(entry);

      return *this;
    }

    Params &put_int32(std::string_view key, int32_t value) {
      ::amqp_table_entry_t entry;

      entry.key = amqp_string_view_bytes(key);
      entry.value.kind = AMQP_FIELD_KIND_I32;
      entry.value.value.i32 = value;

      mParams.push_back(entry);

      return *this;
    }

    Params &put_int64(std::string_view key, int64_t value) {
      ::amqp_table_entry_t entry;

      entry.key = amqp_string_view_bytes(key);
      entry.value.kind = AMQP_FIELD_KIND_I64;
      entry.value.value.i64 = value;

      mParams.push_back(entry);

      return *this;
    }

    Params &put_uint8(std::string_view key, uint8_t value) {
      ::amqp_table_entry_t entry;

      entry.key = amqp_string_view_bytes(key);
      entry.value.kind = AMQP_FIELD_KIND_U8;
      entry.value.value.u8 = value;

      mParams.push_back(entry);

      return *this;
    }

    Params &put_uint16(std::string_view key, uint16_t value) {
      ::amqp_table_entry_t entry;

      entry.key = amqp_string_view_bytes(key);
      entry.value.kind = AMQP_FIELD_KIND_U16;
      entry.value.value.u16 = value;

      mParams.push_back(entry);

      return *this;
    }

    Params &put_uint32(std::string_view key, uint32_t value) {
      ::amqp_table_entry_t entry;

      entry.key = amqp_string_view_bytes(key);
      entry.value.kind = AMQP_FIELD_KIND_U32;
      entry.value.value.u32 = value;

      mParams.push_back(entry);

      return *this;
    }

    Params &put_uint64(std::string_view key, uint64_t value) {
      ::amqp_table_entry_t entry;

      entry.key = amqp_string_view_bytes(key);
      entry.value.kind = AMQP_FIELD_KIND_U64;
      entry.value.value.u64 = value;

      mParams.push_back(entry);

      return *this;
    }

    Params &put_float32(std::string_view key, float value) {
      ::amqp_table_entry_t entry;

      entry.key = amqp_string_view_bytes(key);
      entry.value.kind = AMQP_FIELD_KIND_F32;
      entry.value.value.f32 = value;

      mParams.push_back(entry);

      return *this;
    }

    Params &put_float64(std::string_view key, double value) {
      ::amqp_table_entry_t entry;

      entry.key = amqp_string_view_bytes(key);
      entry.value.kind = AMQP_FIELD_KIND_F64;
      entry.value.value.f64 = value;

      mParams.push_back(entry);

      return *this;
    }

    Params &put_text(std::string_view key, std::string_view value) {
      ::amqp_table_entry_t entry;

      entry.key = amqp_string_view_bytes(key);
      entry.value.kind = AMQP_FIELD_KIND_UTF8;
      entry.value.value.bytes = amqp_string_view_bytes(value);

      mParams.push_back(entry);

      return *this;
    }

    Params &put_bytes(std::string_view key, std::string_view value) {
      ::amqp_table_entry_t entry;

      entry.key = amqp_string_view_bytes(key);
      entry.value.kind = AMQP_FIELD_KIND_BYTES;
      entry.value.value.bytes = amqp_string_view_bytes(value);

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
      : mProperties{} {
    }

    explicit Properties(amqp_basic_properties_t properties)
      : mProperties{properties} {
    }

    [[nodiscard]] std::optional<std::string_view> content_type() const {
      if (mProperties._flags & AMQP_BASIC_CONTENT_TYPE_FLAG) {
        return std::string_view{static_cast<char *>(mProperties.content_type.bytes), mProperties.content_type.len};
      }

      return {};
    }

    Properties &content_type(std::string_view value) {
      mProperties._flags = AMQP_BASIC_CONTENT_TYPE_FLAG;
      mProperties.content_type = amqp_string_view_bytes(value);

      return *this;
    }

    [[nodiscard]] std::optional<std::string_view> encoding() const {
      if (mProperties._flags & AMQP_BASIC_CONTENT_ENCODING_FLAG) {
        return std::string_view{
          static_cast<char *>(mProperties.content_encoding.bytes), mProperties.content_encoding.len
        };
      }

      return {};
    }

    Properties &encoding(std::string_view value) {
      mProperties._flags = AMQP_BASIC_CONTENT_ENCODING_FLAG;
      mProperties.content_encoding = amqp_string_view_bytes(value);

      return *this;
    }

    [[nodiscard]] std::optional<DeliveryMode> delivery_mode() const {
      if (mProperties._flags & AMQP_BASIC_DELIVERY_MODE_FLAG) {
        return (mProperties.delivery_mode == 1) ? DeliveryMode::NonPersistent : DeliveryMode::Persistent;
      }

      return {};
    }

    Properties &delivery_mode(DeliveryMode value) {
      mProperties._flags = AMQP_BASIC_DELIVERY_MODE_FLAG;

      if (value == DeliveryMode::NonPersistent) {
        mProperties.delivery_mode = 1;
      } else {
        mProperties.delivery_mode = 2;
      }

      return *this;
    }

    [[nodiscard]] std::optional<uint8_t> priority() const {
      if (mProperties._flags & AMQP_BASIC_PRIORITY_FLAG) {
        return mProperties.priority;
      }

      return {};
    }

    Properties &priority(uint8_t value) {
      mProperties._flags = AMQP_BASIC_PRIORITY_FLAG;
      mProperties.priority = value;

      return *this;
    }

    [[nodiscard]] std::optional<std::string_view> reply_to() const {
      if (mProperties._flags & AMQP_BASIC_REPLY_TO_FLAG) {
        return std::string_view{static_cast<char *>(mProperties.reply_to.bytes), mProperties.reply_to.len};
      }

      return {};
    }

    Properties &reply_to(std::string_view value) {
      mProperties._flags = AMQP_BASIC_REPLY_TO_FLAG;
      mProperties.reply_to = amqp_string_view_bytes(value);

      return *this;
    }

    [[nodiscard]] std::optional<std::string_view> expiration() const {
      if (mProperties._flags & AMQP_BASIC_EXPIRATION_FLAG) {
        return std::string_view{static_cast<char *>(mProperties.expiration.bytes), mProperties.expiration.len};
      }

      return {};
    }

    Properties &expiration(std::string_view value) {
      mProperties._flags = AMQP_BASIC_EXPIRATION_FLAG;
      mProperties.expiration = amqp_string_view_bytes(value);

      return *this;
    }

    [[nodiscard]] std::optional<std::string_view> message_id() const {
      if (mProperties._flags & AMQP_BASIC_MESSAGE_ID_FLAG) {
        return std::string_view{static_cast<char *>(mProperties.message_id.bytes), mProperties.message_id.len};
      }

      return {};
    }

    Properties &message_id(std::string_view value) {
      mProperties._flags = AMQP_BASIC_MESSAGE_ID_FLAG;
      mProperties.message_id = amqp_string_view_bytes(value);

      return *this;
    }

    [[nodiscard]] std::optional<std::string_view> application_id() const {
      if (mProperties._flags & AMQP_BASIC_APP_ID_FLAG) {
        return std::string_view{static_cast<char *>(mProperties.app_id.bytes), mProperties.app_id.len};
      }

      return {};
    }

    Properties &application_id(std::string_view value) {
      mProperties._flags = AMQP_BASIC_APP_ID_FLAG;
      mProperties.app_id = amqp_string_view_bytes(value);

      return *this;
    }

    [[nodiscard]] std::optional<std::string_view> cluster_id() const {
      if (mProperties._flags & AMQP_BASIC_CLUSTER_ID_FLAG) {
        return std::string_view{static_cast<char *>(mProperties.cluster_id.bytes), mProperties.cluster_id.len};
      }

      return {};
    }

    Properties &cluster_id(std::string_view value) {
      mProperties._flags = AMQP_BASIC_CLUSTER_ID_FLAG;
      mProperties.cluster_id = amqp_string_view_bytes(value);

      return *this;
    }

    [[nodiscard]] std::optional<std::string_view> user_id() const {
      if (mProperties._flags & AMQP_BASIC_USER_ID_FLAG) {
        return std::string_view{static_cast<char *>(mProperties.user_id.bytes), mProperties.user_id.len};
      }

      return {};
    }

    Properties &user_id(std::string_view value) {
      mProperties._flags = AMQP_BASIC_USER_ID_FLAG;
      mProperties.user_id = amqp_string_view_bytes(value);

      return *this;
    }

    [[nodiscard]] std::optional<uint64_t> timestamp() const {
      if (mProperties._flags & AMQP_BASIC_MESSAGE_ID_FLAG) {
        return mProperties.timestamp;
      }

      return {};
    }

    Properties &timestamp(std::chrono::milliseconds value) {
      mProperties._flags = AMQP_BASIC_TIMESTAMP_FLAG;
      mProperties.timestamp = value.count();

      return *this;
    }

    [[nodiscard]] std::optional<std::string_view> type() const {
      if (mProperties._flags & AMQP_BASIC_TYPE_FLAG) {
        return std::string_view{static_cast<char *>(mProperties.type.bytes), mProperties.type.len};
      }

      return {};
    }

    Properties &type(std::string_view value) {
      mProperties._flags = AMQP_BASIC_TYPE_FLAG;
      mProperties.type = amqp_string_view_bytes(value);

      return *this;
    }

    [[nodiscard]] std::optional<std::string_view> correlation_id() const {
      if (mProperties._flags & AMQP_BASIC_CORRELATION_ID_FLAG) {
        return std::string_view{static_cast<char *>(mProperties.correlation_id.bytes), mProperties.correlation_id.len};
      }

      return {};
    }

    Properties &correlation_id(std::string_view value) {
      mProperties._flags = AMQP_BASIC_CORRELATION_ID_FLAG;
      mProperties.correlation_id = amqp_string_view_bytes(value);

      return *this;
    }

    [[nodiscard]] std::optional<Params> headers() const {
      if (mProperties._flags & AMQP_BASIC_HEADERS_FLAG) {
        auto params = Params{};

        for (int i = 0; i < mProperties.headers.num_entries; i++) {
          auto entry = mProperties.headers.entries[i];

          if (entry.value.kind == AMQP_FIELD_KIND_VOID) {
            params.put_void({static_cast<char *>(entry.key.bytes), entry.key.len});
          } else if (entry.value.kind == AMQP_FIELD_KIND_BOOLEAN) {
            params.put_bool({static_cast<char *>(entry.key.bytes), entry.key.len}, entry.value.value.boolean);
          } else if (entry.value.kind == AMQP_FIELD_KIND_I8) {
            params.put_int8({static_cast<char *>(entry.key.bytes), entry.key.len}, entry.value.value.i8);
          } else if (entry.value.kind == AMQP_FIELD_KIND_I16) {
            params.put_int16({static_cast<char *>(entry.key.bytes), entry.key.len}, entry.value.value.i16);
          } else if (entry.value.kind == AMQP_FIELD_KIND_I32) {
            params.put_int32({static_cast<char *>(entry.key.bytes), entry.key.len}, entry.value.value.i32);
          } else if (entry.value.kind == AMQP_FIELD_KIND_I64) {
            params.put_int64({static_cast<char *>(entry.key.bytes), entry.key.len}, entry.value.value.i64);
          } else if (entry.value.kind == AMQP_FIELD_KIND_U8) {
            params.put_uint8({static_cast<char *>(entry.key.bytes), entry.key.len}, entry.value.value.u8);
          } else if (entry.value.kind == AMQP_FIELD_KIND_U16) {
            params.put_uint16({static_cast<char *>(entry.key.bytes), entry.key.len}, entry.value.value.u16);
          } else if (entry.value.kind == AMQP_FIELD_KIND_U32) {
            params.put_uint32({static_cast<char *>(entry.key.bytes), entry.key.len}, entry.value.value.u32);
          } else if (entry.value.kind == AMQP_FIELD_KIND_U64) {
            params.put_uint64({static_cast<char *>(entry.key.bytes), entry.key.len}, entry.value.value.u64);
          } else if (entry.value.kind == AMQP_FIELD_KIND_F32) {
            params.put_float32({static_cast<char *>(entry.key.bytes), entry.key.len}, entry.value.value.f32);
          } else if (entry.value.kind == AMQP_FIELD_KIND_F64) {
            params.put_float64({static_cast<char *>(entry.key.bytes), entry.key.len}, entry.value.value.f64);
          } else if (entry.value.kind == AMQP_FIELD_KIND_UTF8) {
            params.put_text({static_cast<char *>(entry.key.bytes), entry.key.len}, {
                              static_cast<char *>(entry.value.value.bytes.bytes), entry.value.value.bytes.len
                            });
          } else if (entry.value.kind == AMQP_FIELD_KIND_BYTES) {
            params.put_bytes({static_cast<char *>(entry.key.bytes), entry.key.len}, {
                               static_cast<char *>(entry.value.value.bytes.bytes), entry.value.value.bytes.len
                             });
          }
        }
      }

      return {};
    }

    Properties &headers(Params const &params) {
      mProperties._flags = AMQP_BASIC_HEADERS_FLAG;
      mProperties.headers = params.get_params();

      return *this;
    }

    [[nodiscard]] amqp_basic_properties_t const *get_properties() const {
      return &mProperties;
    }

  private:
    amqp_basic_properties_t mProperties;
  };

  struct Context {
    Context() = default;

    Context &ssl_cacert(std::string const &value) {
      mSslCacert = value;

      return *this;
    }

    [[nodiscard]] std::string const &ssl_cacert() const {
      return mSslCacert;
    }

    Context &ssl_engine(std::string const &value) {
      mSslEngine = value;

      return *this;
    }

    [[nodiscard]] std::string const &ssl_engine() const {
      return mSslEngine;
    }

    Context &ssl_verify_peer(bool value) {
      mSslVerifyPeer = value;

      return *this;
    }

    [[nodiscard]] bool ssl_verify_peer() const {
      return mSslVerifyPeer;
    }

    Context &ssl_verify_hostname(bool value) {
      mSslVerifyHostname = value;

      return *this;
    }

    [[nodiscard]] bool ssl_verify_hostname() const {
      return mSslVerifyHostname;
    }

    Context &ssl_keys(std::pair<std::string, std::string> value) {
      mSslKeys = value;

      return *this;
    }

    [[nodiscard]] std::optional<std::pair<std::string, std::string> > ssl_keys() const {
      return mSslKeys;
    }

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
      if (mFrame < 4096 or mFrame > AMQP_DEFAULT_FRAME_SIZE) {
        throw std::runtime_error{"invalid frame size range"};
      }

      return *this;
    }

    [[nodiscard]] int frame() const {
      return mFrame;
    }

    Context &properties(Params params) {
      mParams = params;

      return *this;
    }

    [[nodiscard]] std::optional<Params> properties() {
      return mParams;
    }

  private:
    std::string mSslCacert;
    std::string mSslEngine;
    std::optional<std::pair<std::string, std::string> > mSslKeys;
    std::optional<Params> mParams;
    std::string mHost{"localhost"};
    std::string mUser{"guest"};
    std::string mPass{"guest"};
    std::string mVirtualHost{"/"};
    std::chrono::milliseconds mTimeout{1000};
    int mPort{5672};
    int mFrame{4096};
    bool mSslVerifyPeer{};
    bool mSslVerifyHostname{};
  };

  struct Exchange {
    enum class Type {
      FANOUT,
      DIRECT,
      TOPIC,
      HEADERS,
    };

    Exchange() = default;

    explicit Exchange(std::string_view name)
      : mName{name} {
    }

    Exchange &name(std::string_view const &value) {
      mName = value;

      return *this;
    }

    [[nodiscard]] std::string_view const &name() const {
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
    std::string_view mName{""};
    bool mPassive{};
    bool mDurable{};
    bool mAutoDelete{};
    bool mInternal{};
    Type mType{Type::FANOUT};
  };

  struct RoutingKey {
    RoutingKey() = default;

    explicit RoutingKey(std::string_view name)
      : mName{name} {
    }

    RoutingKey &name(std::string_view const &value) {
      mName = value;

      return *this;
    }

    [[nodiscard]] std::string_view const &name() const {
      return mName;
    }

  private:
    std::string_view mName{""};
  };

  struct Queue {
    Queue() = default;

    explicit Queue(std::string_view name)
      : mName{std::move(name)} {
    }

    Queue &name(std::string_view const &value) {
      mName = value;

      return *this;
    }

    [[nodiscard]] std::string_view const &name() const {
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
    std::string_view mName{""};
    bool mPassive{};
    bool mDurable{};
    bool mExclusive{};
    bool mAutoDelete{};
  };

  struct Message {
    explicit Message(std::string_view message)
      : mMessage{message} {
    }

    [[nodiscard]] std::string_view data() const {
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
    std::string_view mMessage{""};
    bool mMandatory{};
    bool mImmediate{};
  };

  struct Envelope {
    virtual ~Envelope() = default;

    [[nodiscard]] virtual std::string_view data() const = 0;

    [[nodiscard]] virtual int channel() const = 0;

    [[nodiscard]] virtual uint64_t delivery_tag() const = 0;

    [[nodiscard]] virtual std::string_view consumer_tag() const = 0;

    [[nodiscard]] virtual std::string_view exchange() const = 0;

    [[nodiscard]] virtual std::string_view routing_key() const = 0;

    [[nodiscard]] virtual bool redelivered() const = 0;

    [[nodiscard]] virtual Properties properties() const = 0;

  protected:
    Envelope() = default;
  };

  struct RabbitMqEnvelopeWrapper : public Envelope {
    explicit RabbitMqEnvelopeWrapper(amqp_envelope_t envelope)
      : Envelope{}, mEnvelope{envelope} {
    }

    ~RabbitMqEnvelopeWrapper() override {
      amqp_destroy_envelope(&mEnvelope);
    }

    [[nodiscard]] std::string_view data() const override {
      return {static_cast<char *>(mEnvelope.message.body.bytes), mEnvelope.message.body.len};
    }

    [[nodiscard]] int channel() const override {
      return mEnvelope.channel;
    }

    [[nodiscard]] uint64_t delivery_tag() const override {
      return mEnvelope.delivery_tag;
    }

    [[nodiscard]] std::string_view consumer_tag() const override {
      return {static_cast<char *>(mEnvelope.consumer_tag.bytes), mEnvelope.consumer_tag.len};
    }

    [[nodiscard]] std::string_view exchange() const override {
      return {static_cast<char *>(mEnvelope.exchange.bytes), mEnvelope.exchange.len};
    }

    [[nodiscard]] std::string_view routing_key() const override {
      return {static_cast<char *>(mEnvelope.routing_key.bytes), mEnvelope.routing_key.len};
    }

    [[nodiscard]] bool redelivered() const override {
      return mEnvelope.redelivered;
    }

    [[nodiscard]] Properties properties() const override {
      return Properties{mEnvelope.message.properties};
    }

  private:
    amqp_envelope_t mEnvelope;
  };

  struct RabbitMqMessasgeWrapper : public Envelope {
    explicit RabbitMqMessasgeWrapper(amqp_message_t message, amqp_basic_get_ok_t *reply, int channel)
      : Envelope{}, mMessage{message}, mReply{reply}, mChannel{channel} {
    }

    ~RabbitMqMessasgeWrapper() override {
      amqp_destroy_message(&mMessage);
    }

    [[nodiscard]] std::string_view data() const override {
      return {static_cast<char *>(mMessage.body.bytes), mMessage.body.len};
    }

    [[nodiscard]] int channel() const override {
      return mChannel;
    }

    [[nodiscard]] uint64_t delivery_tag() const override {
      if (!mReply) {
        return 0;
      }

      return mReply->delivery_tag;
    }

    [[nodiscard]] std::string_view consumer_tag() const override {
      return {""};
    }

    [[nodiscard]] std::string_view exchange() const override {
      if (!mReply) {
        return {""};
      }

      return {static_cast<char *>(mReply->exchange.bytes), mReply->exchange.len};
    }

    [[nodiscard]] std::string_view routing_key() const override {
      if (!mReply) {
        return {""};
      }

      return {static_cast<char *>(mReply->routing_key.bytes), mReply->routing_key.len};
    }

    [[nodiscard]] bool redelivered() const override {
      if (!mReply) {
        return false;
      }

      return mReply->redelivered;
    }

    [[nodiscard]] Properties properties() const override {
      return Properties{mMessage.properties};
    }

  private:
    amqp_message_t mMessage;
    amqp_basic_get_ok_t *mReply{nullptr};
    int mChannel{-1};
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

    void declare_exchange(Exchange const &exchange, Params const &params = StaticParams) const {
      std::string_view exchangeType = "fanout";

      if (exchange.type() == Exchange::Type::DIRECT) {
        exchangeType = "direct";
      } else if (exchange.type() == Exchange::Type::TOPIC) {
        exchangeType = "topic";
      } else if (exchange.type() == Exchange::Type::HEADERS) {
        exchangeType = "headers";
      }

      amqp_exchange_declare(mState, mChannel, amqp_string_view_bytes(exchange.name()),
                            amqp_string_view_bytes(exchangeType), exchange.passive() ? 1 : 0,
                            exchange.durable() ? 1 : 0, exchange.auto_delete() ? 1 : 0, exchange.internal() ? 1 : 0,
                            params.get_params());

      if (auto result = amqp_error(amqp_get_rpc_reply(mState)); result) {
        throw std::runtime_error(result.value());
      }
    }

    void delete_exchange(Exchange const &exchange, bool ifUnused = false) const {
      amqp_exchange_delete(mState, mChannel, amqp_string_view_bytes(exchange.name()), ifUnused);

      if (auto result = amqp_error(amqp_get_rpc_reply(mState)); result) {
        throw std::runtime_error(result.value());
      }
    }

    void declare_queue(Queue const &queue, Params const &params = StaticParams) const {
      amqp_queue_declare(mState, mChannel, amqp_string_view_bytes(queue.name()),
                         queue.passive() ? 1 : 0,
                         queue.durable() ? 1 : 0, queue.exclusive() ? 1 : 0, queue.auto_delete() ? 1 : 0,
                         params.get_params());

      if (auto result = amqp_error(amqp_get_rpc_reply(mState)); result) {
        throw std::runtime_error(result.value());
      }

      // amqp_release_buffers(mState);
    }

    void delete_queue(Queue const &queue, bool ifUnused = false, bool ifEmpty = false) const {
      amqp_queue_delete(mState, mChannel, amqp_string_view_bytes(queue.name()), ifUnused ? 1 : 0,
                        ifEmpty ? 1 : 0);

      if (auto result = amqp_error(amqp_get_rpc_reply(mState)); result) {
        throw std::runtime_error(result.value());
      }
    }

    void purge_queue(Queue const &queue) const {
      amqp_queue_purge(mState, mChannel, amqp_string_view_bytes(queue.name()));

      if (auto result = amqp_error(amqp_get_rpc_reply(mState)); result) {
        throw std::runtime_error(result.value());
      }
    }

    void bind(Exchange const &exchange, Queue const &queue, RoutingKey const &routingKey = StaticRoutingKey,
              Params const &params = StaticParams) const {
      amqp_queue_bind(mState, mChannel, amqp_string_view_bytes(queue.name()),
                      amqp_string_view_bytes(exchange.name()), amqp_string_view_bytes(routingKey.name()),
                      params.get_params());

      if (auto result = amqp_error(amqp_get_rpc_reply(mState)); result) {
        throw std::runtime_error(result.value());
      }
    }

    void unbind(Exchange const &exchange, Queue const &queue, RoutingKey const &routingKey = StaticRoutingKey,
                Params const &params = StaticParams) const {
      amqp_queue_unbind(mState, mChannel, amqp_string_view_bytes(queue.name()),
                        amqp_string_view_bytes(exchange.name()), amqp_string_view_bytes(routingKey.name()),
                        params.get_params());

      if (auto result = amqp_error(amqp_get_rpc_reply(mState)); result) {
        throw std::runtime_error(result.value());
      }
    }

    void publish(Exchange const &exchange, Message const &message, RoutingKey const &routingKey = StaticRoutingKey,
                 Properties const &properties = {}) const {
      if (auto result = amqp_basic_publish(mState, 1, amqp_string_view_bytes(exchange.name()),
                                           amqp_string_view_bytes(routingKey.name()), message.mandatory(),
                                           message.immediate(), properties.get_properties(),
                                           amqp_string_view_bytes(message.data())); result != AMQP_STATUS_OK) {
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

    /*
    [[nodiscard]] bool wait_for_frame(std::chrono::milliseconds ms) const {
      if (amqp_frames_enqueued(mState) == AMQP_STATUS_OK and amqp_data_in_buffer(mState) == AMQP_STATUS_OK) {
        amqp_frame_t frame;

        amqp_maybe_release_buffers(mState);

        struct timeval timeout{
          .tv_sec = mContext.timeout().count() / 1000,
          .tv_usec = mContext.timeout().count() * 1000
        };

        return amqp_simple_wait_frame_noblock(mState, &frame, &timeout) == AMQP_STATUS_OK;
      }

      return false;
    }
    */

    [[nodiscard]] std::optional<std::unique_ptr<Envelope> > get(Queue const &queue, bool noAck = {true}) const {
      amqp_rpc_reply_t reply = amqp_basic_get(mState, mChannel, amqp_string_view_bytes(queue.name()), noAck ? 1 : 0);

      if (auto result = amqp_error(reply); result) {
        throw std::runtime_error(result.value());
      }

      if (reply.reply.id == AMQP_BASIC_GET_EMPTY_METHOD) {
        return {};
      }

      if (reply.reply.id == AMQP_BASIC_GET_OK_METHOD) {
        auto *msg = static_cast<amqp_basic_get_ok_t *>(reply.reply.decoded);

        // The actual message content needs to be read separately in C
        amqp_message_t message;

        amqp_rpc_reply_t msg_reply = amqp_read_message(mState, mChannel, &message, 0);

        if (msg_reply.reply_type == AMQP_RESPONSE_NORMAL) {
          return std::make_unique<RabbitMqMessasgeWrapper>(message, msg, -1);
        }
      }

      return {};
    }

    void subscribe(std::function<bool(std::unique_ptr<Envelope>)> callback, Queue const &queue, std::string_view consumerTag,
                 std::chrono::milliseconds timeout = {}, bool noLocal = {},
                 bool noAck = {true}, bool exclusive = {}, Params const &params = StaticParams) const {
      amqp_basic_consume(mState, mChannel, amqp_string_view_bytes(queue.name()),
                         amqp_string_view_bytes(consumerTag), noLocal ? 1 : 0, noAck ? 1 : 0,
                         exclusive ? 1 : 0, params.get_params());

      if (auto result = amqp_error(amqp_get_rpc_reply(mState)); result) {
        throw std::runtime_error(result.value());
      }

      while (true) {
        if (auto result = consume(timeout); result) {
          if ((consumerTag.empty() or result.value()->consumer_tag() == consumerTag) && !callback(std::move(result.value()))) {
            return;
          }
        }
      }
    }

    [[nodiscard]] std::generator<std::unique_ptr<Envelope>> subscribe(int count, Queue const &queue, std::string_view consumerTag,
             std::chrono::milliseconds timeout = {}, bool noLocal = {},
             bool noAck = {true}, bool exclusive = {}, Params const &params = StaticParams) const {
      amqp_basic_consume(mState, mChannel, amqp_string_view_bytes(queue.name()),
                         amqp_string_view_bytes(consumerTag), noLocal ? 1 : 0, noAck ? 1 : 0,
                         exclusive ? 1 : 0, params.get_params());

      if (auto result = amqp_error(amqp_get_rpc_reply(mState)); result) {
        throw std::runtime_error(result.value());
      }

      while (count-- > 0) {
        if (auto result = consume(timeout); result) {
          if ((consumerTag.empty() or result.value()->consumer_tag() == consumerTag)) {
            co_yield std::move(result.value());
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

    void qos(uint32_t prefetchSize, uint16_t prefetchCount = {1},
             bool global = {}) const {
      amqp_basic_qos(mState, mChannel, prefetchSize, prefetchCount, global ? 1 : 0);

      if (auto result = amqp_error(amqp_get_rpc_reply(mState)); result) {
        throw std::runtime_error(result.value());
      }

      amqp_maybe_release_buffers_on_channel(mState, mChannel);
    }

    void cancel(const std::string_view consumerTag) const {
      amqp_basic_cancel(mState, mChannel, amqp_string_view_bytes(consumerTag));

      if (auto result = amqp_error(amqp_get_rpc_reply(mState)); result) {
        throw std::runtime_error(result.value());
      }

      amqp_maybe_release_buffers_on_channel(mState, mChannel);
    }

    void confirm_select() const {
      amqp_confirm_select(mState, mChannel);

      if (auto result = amqp_error(amqp_get_rpc_reply(mState)); result) {
        throw std::runtime_error(result.value());
      }
    }

    [[nodiscard]] std::pair<std::uint64_t, std::string> wait_ack(std::chrono::microseconds ms) const {
      amqp_publisher_confirm_t confirm;
      amqp_rpc_reply_t ret;

      amqp_maybe_release_buffers(mState);

      if (ms.count() > 0) {
        struct timeval timeout{
          .tv_sec = mContext.timeout().count() / 1000,
          .tv_usec = mContext.timeout().count() * 1000
        };

        ret = amqp_publisher_confirm_wait(mState, &timeout, &confirm);
      } else {
        ret = amqp_publisher_confirm_wait(mState, nullptr, &confirm);
      }

      if (auto result = amqp_error(ret); result) {
        throw std::runtime_error(result.value());
      }

      std::string method = amqp_method_name(confirm.method);

      if (confirm.method == AMQP_BASIC_ACK_METHOD) {
        return std::pair{confirm.payload.ack.delivery_tag, method};
      } else if (confirm.method == AMQP_BASIC_NACK_METHOD) {
        return std::pair{confirm.payload.ack.delivery_tag, method};
      } else if (confirm.method == AMQP_BASIC_REJECT_METHOD) {
        return std::pair{confirm.payload.ack.delivery_tag, method};
      } else {
        throw std::runtime_error{std::format("unexpected method {}", method)};
      }
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
    inline static constexpr auto const StaticParams = Params{};
    inline static constexpr auto const StaticRoutingKey = RoutingKey{};

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

    [[nodiscard]] std::optional<std::unique_ptr<Envelope> > consume(std::chrono::milliseconds timeout = {}) const {
      amqp_envelope_t envelope;
      struct timeval tval{
        .tv_sec = mContext.timeout().count() / 1000,
        .tv_usec = mContext.timeout().count() * 1000
      };
      amqp_frame_t frame;

      amqp_maybe_release_buffers(mState);

      auto reply = amqp_consume_message(mState, &envelope, &tval, 0);

      if (auto result = amqp_error(reply); result) {
        throw std::runtime_error(result.value());
      }

      if (AMQP_RESPONSE_NORMAL == reply.reply_type) {
        return std::make_unique<RabbitMqEnvelopeWrapper>(envelope);
      }

      if (reply.reply_type == AMQP_RESPONSE_NONE) {
        return {};
      }

      if (reply.reply_type == AMQP_RESPONSE_SERVER_EXCEPTION) {
        if (reply.reply.id == AMQP_CONNECTION_CLOSE_METHOD) {
          auto *context = static_cast<amqp_connection_close_t *>(reply.reply.decoded);
          auto sv = std::string_view{static_cast<char *>(context->reply_text.bytes), context->reply_text.len};

          throw std::runtime_error{std::format("server connection error {}, message: {}\n", context->reply_code, sv)};
        }

        if (reply.reply.id == AMQP_CHANNEL_CLOSE_METHOD) {
          auto *context = static_cast<amqp_channel_close_t *>(reply.reply.decoded);
          auto sv = std::string_view{static_cast<char *>(context->reply_text.bytes), context->reply_text.len};

          throw std::runtime_error{std::format("server channel error {}, message: {}\n", context->reply_code, sv)};
        }

        throw std::runtime_error{std::format("unknown server error, method id {}", reply.reply.id)};
      }

      if (reply.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION) {
        if (reply.library_error != AMQP_STATUS_UNEXPECTED_STATE) {
          throw std::runtime_error{amqp_error_string2(reply.library_error)};
        }

        if (AMQP_STATUS_OK != amqp_simple_wait_frame(mState, &frame)) {
          return {};
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

            return std::make_unique<RabbitMqMessasgeWrapper>(message, nullptr, frame.channel);
          } else if (frame.payload.method.id == AMQP_CHANNEL_CLOSE_METHOD) {
            // a channel.close method happens when a channel exception occurs, this can happen by publishing to an
            // exchange that doesn't existfor example. In this case you would need to open another channel redeclare
            // any queues that were declared auto-delete, and restart any consumers that were attached to the previous
            // channel.
            return {};
          } else if (frame.payload.method.id == AMQP_CONNECTION_CLOSE_METHOD) {
            // a connection.close method happens when a connection exception occurs, this can happen by trying to
            // use a channel that isn't open for example. In this case the whole connection must be restarted.
            return {};
          } else {
            throw std::runtime_error{std::format("an unexpected method '{}' was received", frame.payload.method.id)};
          }
        }
      }

      return {};
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

    [[nodiscard]] static std::string get_version() {
      return {amqp_version()};
    }

    [[nodiscard]] int get_max_channels() const {
      return amqp_get_channel_max(mState);
    }

    [[nodiscard]] int get_max_frame_size() const {
      return amqp_get_frame_max(mState);
    }

    [[nodiscard]] std::chrono::seconds get_heartbeat_timeout() const {
      return std::chrono::seconds{amqp_get_heartbeat(mState)};
    }

  private:
    Context mContext;
    amqp_connection_state_t mState{nullptr};
    amqp_socket_t *mSocket{nullptr};

    explicit RabbitMq(Context context)
      : mContext(std::move(context)) {
      mState = amqp_new_connection();

      auto cacert = mContext.ssl_cacert();

      if (!cacert.empty()) {
        mSocket = amqp_ssl_socket_new(mState);
      } else {
        mSocket = amqp_tcp_socket_new(mState);
      }

      if (!mSocket) {
        amqp_connection_close(mState, AMQP_REPLY_SUCCESS);

        throw std::runtime_error{"unable to initialize connection"};
      }

      if (!cacert.empty()) {
        amqp_ssl_socket_set_verify_peer(mSocket, 0);
        amqp_ssl_socket_set_verify_hostname(mSocket, 0);

        amqp_ssl_socket_set_cacert(mSocket, cacert.c_str());

        if (auto engine = mContext.ssl_engine(); !engine.empty()) {
          amqp_set_ssl_engine(engine.c_str());
        }

        if (mContext.ssl_verify_peer()) {
          amqp_ssl_socket_set_verify_peer(mSocket, 1);
        }

        if (mContext.ssl_verify_hostname()) {
          amqp_ssl_socket_set_verify_hostname(mSocket, 1);
        }

        if (auto keys = mContext.ssl_keys(); keys.has_value()) {
          auto key_pair = keys.value();

          amqp_ssl_socket_set_key(mSocket, key_pair.first.c_str(), key_pair.second.c_str());
        }
      }

      if (mContext.timeout().count() > 0) {
        struct timeval tval{
          .tv_sec = mContext.timeout().count() / 1000,
          .tv_usec = mContext.timeout().count() * 1000
        };

        if (amqp_socket_open_noblock(mSocket, mContext.host().data(), mContext.port(), &tval) != AMQP_STATUS_OK) {
          amqp_connection_close(mState, AMQP_REPLY_SUCCESS);
          amqp_destroy_connection(mState);

          throw std::runtime_error{"connection timeout"};
        }
      } else {
        if (amqp_socket_open(mSocket, mContext.host().data(), mContext.port()) != AMQP_STATUS_OK) {
          amqp_connection_close(mState, AMQP_REPLY_SUCCESS);
          amqp_destroy_connection(mState);

          throw std::runtime_error{"connection error"};
        }
      }

      if (!mContext.properties()) {
        if (auto result = amqp_error(amqp_login(mState, mContext.virtual_host().data(), 0, mContext.frame(), 0,
                                                AMQP_SASL_METHOD_PLAIN, mContext.user().data(),
                                                mContext.pass().data())); result) {
          amqp_connection_close(mState, AMQP_REPLY_SUCCESS);
          amqp_destroy_connection(mState);

          throw std::runtime_error{result.value()};
        }
      } else {
        auto params = mContext.properties()->get_params();

        if (auto result = amqp_error(amqp_login_with_properties(mState, mContext.virtual_host().data(), 0,
                                                                mContext.frame(), 0,
                                                                &params, AMQP_SASL_METHOD_PLAIN, mContext.user().data(),
                                                                mContext.pass().data())); result) {
          amqp_connection_close(mState, AMQP_REPLY_SUCCESS);
          amqp_destroy_connection(mState);

          throw std::runtime_error{result.value()};
        }
      }
    }
  };
}
