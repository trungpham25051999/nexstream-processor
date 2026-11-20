#include "nexstream/processor/domain/evaluator.hpp"
#include "nexstream/processor/application/partition_state.hpp"
#include "nexstream/processor/infrastructure/json_codec.hpp"
#include <iostream>
#include <stdexcept>
using namespace nexstream::processor;
void check(bool v){if(!v)throw std::runtime_error("assertion failed");}
int main(){try{
domain::Evaluator evaluator({90,90,2,1,4});domain::Event e{"host","one",1,95,95,100};
application::PartitionState partition({90,90,2,1,4},1);
check(partition.process(0,e).replayed);auto second=e;second.message_id="second";second.sampled_at_ms=2;
auto fresh=partition.process(1,second);check(!fresh.replayed&&fresh.alerts.size()==2);
bool gap=false;try{partition.process(3,second);}catch(...){gap=true;}check(gap);
application::PartitionState invalid_replay({90,90,2,1,4},1);check(invalid_replay.process(0,std::nullopt).replayed);
bool replay_bound=false;try{application::PartitionState too_large({},100001);}catch(...){replay_bound=true;}check(replay_bound);
check(evaluator.evaluate(e).empty());check(evaluator.evaluate(e).empty());e.message_id="two";e.sampled_at_ms=2;
domain::Evaluator recovered({90,90,2,1,4});recovered.restore(evaluator.checkpoint());check(recovered.evaluate(e).size()==2);
auto alerts=evaluator.evaluate(e);check(alerts.size()==2);check(infrastructure::encode_alert(alerts[0]).find("4:host3:two:cpu")!=std::string::npos);
auto left=alerts[0],right=alerts[0];left.host_id="a:b";left.message_id="c";right.host_id="a";right.message_id="b:c";check(domain::alert_identity(left)!=domain::alert_identity(right));
e.message_id="three";e.sampled_at_ms=3;e.cpu=1;e.used=1;check(evaluator.evaluate(e).empty());
e.message_id="four";e.sampled_at_ms=4;e.cpu=99;check(evaluator.evaluate(e).empty());
e.host_id="another";bool bounded=false;try{evaluator.evaluate(e);}catch(...){bounded=true;}check(bounded);
auto parsed=infrastructure::parse_event(R"({"schema_version":1,"host_id":"h","message_id":"m","sample_timestamp_ms":1,"cpu_usage_percent":50,"memory_used_bytes":10,"memory_total_bytes":20})");check(parsed.total==20);
for(auto text:{"{}","bad",R"({"schema_version":2})","[[[[[[0]]]]]]"}){bool invalid=false;try{infrastructure::parse_event(text);}catch(...){invalid=true;}check(invalid);}
std::cout<<"processor assertions PASS\n";
}catch(const std::exception& e){std::cerr<<e.what()<<'\n';return 1;}}
