#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <deque>
namespace nexstream::processor::domain {
struct Event {std::string host_id,message_id; std::int64_t sampled_at_ms; double cpu; std::uint64_t used,total;};
struct Alert {std::string host_id,message_id,metric;std::int64_t sampled_at_ms;double observed,threshold;unsigned consecutive;};
inline std::string alert_identity(const Alert& a) {return std::to_string(a.host_id.size())+":"+a.host_id+std::to_string(a.message_id.size())+":"+a.message_id+":"+a.metric;}
struct Config {double cpu=90,ram=90;unsigned consecutive=3;std::size_t max_hosts=10000,max_ids_per_host=128;};
// Single-threaded state. Bounded recent identity dedup; restore checkpoint before polling after restart.
class Evaluator {
public:
 explicit Evaluator(Config config={});
 std::vector<Alert> evaluate(const Event& event);
 std::string checkpoint() const;
 void restore(const std::string& text);
private:
 struct State {unsigned cpu=0,ram=0;std::int64_t timestamp=0;std::unordered_set<std::string> seen;std::deque<std::string> order;};
 Config config_;std::unordered_map<std::string,State> states_;
};
}
