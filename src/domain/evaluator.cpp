#include "nexstream/processor/domain/evaluator.hpp"
#include <cmath>
#include <stdexcept>
#include <sstream>
#include <iomanip>
namespace nexstream::processor::domain {
Evaluator::Evaluator(Config c):config_(c) {if(!std::isfinite(c.cpu)||!std::isfinite(c.ram)||c.cpu<0||c.cpu>100||c.ram<0||c.ram>100||!c.consecutive||!c.max_hosts||!c.max_ids_per_host) throw std::invalid_argument("threshold config");}
std::string Evaluator::checkpoint() const {
 std::ostringstream out;
 out<<std::setprecision(17)<<config_.cpu<<' '<<config_.ram<<' '<<config_.consecutive<<' '<<states_.size()<<'\n';
 for(const auto& [host,s]:states_) {out<<std::quoted(host)<<' '<<s.cpu<<' '<<s.ram<<' '<<s.timestamp<<' '<<s.order.size();for(const auto& id:s.order)out<<' '<<std::quoted(id);out<<'\n';}
 return out.str();
}
void Evaluator::restore(const std::string& text) {
 std::istringstream in(text); double cpu,ram;unsigned count;size_t hosts;
 if(!(in>>cpu>>ram>>count>>hosts)||cpu!=config_.cpu||ram!=config_.ram||count!=config_.consecutive||hosts>config_.max_hosts)throw std::runtime_error("checkpoint config mismatch");
 std::unordered_map<std::string,State> restored;
 for(size_t i=0;i<hosts;++i){std::string host;State s;size_t ids;
  if(!(in>>std::quoted(host)>>s.cpu>>s.ram>>s.timestamp>>ids)||host.size()>128||s.cpu>count||s.ram>count||ids>config_.max_ids_per_host)throw std::runtime_error("checkpoint invalid");
  for(size_t j=0;j<ids;++j){std::string id;if(!(in>>std::quoted(id))||id.size()>128)throw std::runtime_error("checkpoint id");s.seen.insert(id);s.order.push_back(id);}
  restored.emplace(std::move(host),std::move(s));
 }
 in>>std::ws;if(!in.eof())throw std::runtime_error("checkpoint trailing data");states_=std::move(restored);
}
std::vector<Alert> Evaluator::evaluate(const Event& e) {
 if(!states_.contains(e.host_id)&&states_.size()>=config_.max_hosts) throw std::runtime_error("host capacity exhausted");
 auto& s=states_[e.host_id];
 if(s.seen.contains(e.message_id)||e.sampled_at_ms<s.timestamp) return {};
 s.seen.insert(e.message_id);s.order.push_back(e.message_id);
 if(s.order.size()>config_.max_ids_per_host){s.seen.erase(s.order.front());s.order.pop_front();}
 s.timestamp=e.sampled_at_ms;double ram=100.0*static_cast<double>(e.used)/static_cast<double>(e.total);
 s.cpu=e.cpu>=config_.cpu?(s.cpu<config_.consecutive?s.cpu+1:s.cpu):0;
 s.ram=ram>=config_.ram?(s.ram<config_.consecutive?s.ram+1:s.ram):0;
 std::vector<Alert> alerts;
 if(s.cpu==config_.consecutive) alerts.push_back({e.host_id,e.message_id,"cpu",e.sampled_at_ms,e.cpu,config_.cpu,config_.consecutive});
 if(s.ram==config_.consecutive) alerts.push_back({e.host_id,e.message_id,"ram",e.sampled_at_ms,ram,config_.ram,config_.consecutive});
 return alerts;
}
}
