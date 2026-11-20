#include "nexstream/processor/infrastructure/json_codec.hpp"
#include <nlohmann/json.hpp>
#include <cmath>
#include <stdexcept>
namespace nexstream::processor::infrastructure {
namespace {bool identifier(const std::string& v){return !v.empty()&&v.size()<=128;}}
domain::Event parse_event(std::string_view text) {
 if(text.size()>65536)throw std::invalid_argument("event size");
 auto j=nlohmann::json::parse(text,[](int depth,nlohmann::json::parse_event_t,nlohmann::json&){if(depth>4)throw std::invalid_argument("JSON nesting");return true;}); if(!j.is_object()||!j.at("schema_version").is_number_integer()||j.at("schema_version")!=1)throw std::invalid_argument("version");
 for(auto key:{"sample_timestamp_ms","memory_used_bytes","memory_total_bytes"})if(!j.at(key).is_number_integer())throw std::invalid_argument("integer type");
 if(j.at("memory_used_bytes")<0||j.at("memory_total_bytes")<=0)throw std::invalid_argument("memory sign");
 domain::Event e{j.at("host_id").get<std::string>(),j.at("message_id").get<std::string>(),j.at("sample_timestamp_ms").get<std::int64_t>(),j.at("cpu_usage_percent").get<double>(),j.at("memory_used_bytes").get<std::uint64_t>(),j.at("memory_total_bytes").get<std::uint64_t>()};
 if(!identifier(e.host_id)||!identifier(e.message_id)||e.sampled_at_ms<=0||!std::isfinite(e.cpu)||e.cpu<0||e.cpu>100||e.used>e.total)throw std::invalid_argument("event semantics");
 return e;
}
std::string encode_alert(const domain::Alert& a) {return nlohmann::json{{"schema_version",1},{"alert_id",domain::alert_identity(a)},{"host_id",a.host_id},{"message_id",a.message_id},{"sample_timestamp_ms",a.sampled_at_ms},{"metric",a.metric},{"observed_percent",a.observed},{"threshold_percent",a.threshold},{"consecutive_samples",a.consecutive}}.dump();}
std::string encode_dlq(std::string_view reason,std::string_view topic,int partition,std::int64_t offset){return nlohmann::json{{"schema_version",1},{"reason",reason},{"source_topic",topic},{"source_partition",partition},{"source_offset",offset}}.dump();}
}
