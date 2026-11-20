#include "nexstream/processor/domain/evaluator.hpp"
#include "nexstream/processor/application/partition_state.hpp"
#include "nexstream/processor/infrastructure/kafka_worker.hpp"
#include "nexstream/processor/infrastructure/json_codec.hpp"
#include <librdkafka/rdkafka.h>
#include <memory>
#include <iostream>
#include <csignal>
#include <optional>
#include <fstream>
#include <filesystem>
#include <fcntl.h>
#include <unistd.h>
#include <sys/file.h>
#include <stdexcept>
#include <map>
#include <charconv>
#include <array>
namespace {
volatile std::sig_atomic_t stopped=0;
void stop(int){stopped=1;}
void set(rd_kafka_conf_t* c,const char* k,const std::string& v){char error[512];if(rd_kafka_conf_set(c,k,v.c_str(),error,sizeof(error))!=RD_KAFKA_CONF_OK)throw std::runtime_error(error);}
struct Delivery {bool done=false,success=false;};
void delivered(rd_kafka_t*,const rd_kafka_message_t* m,void*) {std::unique_ptr<std::shared_ptr<Delivery>> owner(static_cast<std::shared_ptr<Delivery>*>(m->_private));(*owner)->success=!m->err;(*owner)->done=true;}
using KafkaQueue=std::unique_ptr<rd_kafka_queue_t,decltype(&rd_kafka_queue_destroy)>;
void destroy_kafka(rd_kafka_t* handle){
 if(rd_kafka_type(handle)==RD_KAFKA_CONSUMER){
  KafkaQueue queue(rd_kafka_queue_new(handle),rd_kafka_queue_destroy);
  if(queue){
   std::unique_ptr<rd_kafka_error_t,decltype(&rd_kafka_error_destroy)> error(rd_kafka_consumer_close_queue(handle,queue.get()),rd_kafka_error_destroy);
   const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(6);
   if(!error)while(!rd_kafka_consumer_closed(handle)&&std::chrono::steady_clock::now()<deadline)(void)rd_kafka_queue_poll_callback(queue.get(),100);
  }
  queue.reset();
  rd_kafka_destroy_flags(handle,RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE);
 }else rd_kafka_destroy(handle);
}
using Kafka=std::unique_ptr<rd_kafka_t,decltype(&destroy_kafka)>;
struct Checkpoint {
 std::string path;int lock;
 explicit Checkpoint(std::string p):path(std::move(p)),lock(open((path+".lock").c_str(),O_CREAT|O_RDWR|O_CLOEXEC,0600)) {if(lock<0||flock(lock,LOCK_EX|LOCK_NB)<0){if(lock>=0)close(lock);throw std::runtime_error("checkpoint lock");}}
 ~Checkpoint(){if(lock>=0)close(lock);}
 Checkpoint(const Checkpoint&)=delete;
 Checkpoint& operator=(const Checkpoint&)=delete;
 void save(const std::string& text){if(text.size()>512*1024)throw std::runtime_error("checkpoint quota");std::string tmp=path+".tmp";int fd=open(tmp.c_str(),O_CREAT|O_TRUNC|O_WRONLY|O_CLOEXEC,0600);if(fd<0)throw std::runtime_error("checkpoint open");size_t pos=0;while(pos<text.size()){auto n=write(fd,text.data()+pos,text.size()-pos);if(n<0&&errno==EINTR)continue;if(n<=0){close(fd);throw std::runtime_error("checkpoint write");}pos+=static_cast<size_t>(n);}int result=fsync(fd);close(fd);if(result||rename(tmp.c_str(),path.c_str()))throw std::runtime_error("checkpoint sync");auto parent=std::filesystem::path(path).parent_path();int dir=open(parent.empty()?".":parent.c_str(),O_RDONLY|O_DIRECTORY|O_CLOEXEC);if(dir<0)throw std::runtime_error("checkpoint directory");result=fsync(dir);close(dir);if(result)throw std::runtime_error("checkpoint directory sync");}
};
struct Ownership {
 nexstream::processor::domain::Config config;
 std::map<std::pair<std::string,int>,nexstream::processor::application::PartitionState> partitions;
 std::string failure;
 std::string fingerprint;
 bool awaiting_assignment{true};
 std::chrono::steady_clock::time_point assignment_deadline{std::chrono::steady_clock::now()+std::chrono::seconds(30)};
};
void rebalance(rd_kafka_t* kafka,rd_kafka_resp_err_t reason,rd_kafka_topic_partition_list_t* assigned,void* opaque) noexcept {
 auto& owner=*static_cast<Ownership*>(opaque);
 try {
  owner.partitions.clear();
  if(reason!=RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS){if(!owner.awaiting_assignment){owner.awaiting_assignment=true;owner.assignment_deadline=std::chrono::steady_clock::now()+std::chrono::seconds(30);}rd_kafka_assign(kafka,nullptr);std::cerr<<"REVOKED all partitions\n";return;}
  owner.awaiting_assignment=false;
  if(assigned->cnt==0){if(rd_kafka_assign(kafka,assigned))throw std::runtime_error("idle assignment failed");std::cerr<<"ASSIGNED idle\n";return;}
  if(assigned->cnt>16)throw std::runtime_error("partition ownership bound exceeded");
  if(rd_kafka_committed(kafka,assigned,3000))throw std::runtime_error("cannot resolve committed offsets");
  for(int i=0;i<assigned->cnt;++i){auto& p=assigned->elems[i];int64_t low=0,high=0;
   if(p.err||rd_kafka_query_watermark_offsets(kafka,p.topic,p.partition,&low,&high,3000)||low!=0)throw std::runtime_error("recovery requires retained history from offset zero");
   auto boundary=p.offset==RD_KAFKA_OFFSET_INVALID?0:p.offset;
   if(boundary<0||boundary>high)throw std::runtime_error("invalid committed recovery boundary");
   if(boundary>0&&(!p.metadata||std::string_view(static_cast<const char*>(p.metadata),p.metadata_size)!=owner.fingerprint))throw std::runtime_error("consumer group algorithm/config metadata mismatch; reviewed migration required");
   owner.partitions.try_emplace({p.topic,p.partition},owner.config,boundary);p.offset=0;
   std::cerr<<"ASSIGNED "<<p.topic<<' '<<p.partition<<" replay_to="<<boundary<<'\n';
  }
  if(rd_kafka_assign(kafka,assigned))throw std::runtime_error("partition assignment failed");
 }catch(const std::exception& e){owner.failure=e.what();rd_kafka_assign(kafka,nullptr);}
}
Kafka create(rd_kafka_type_t type,const std::string& broker,const std::string& group,Ownership* ownership=nullptr) {
 std::unique_ptr<rd_kafka_conf_t,decltype(&rd_kafka_conf_destroy)> conf(rd_kafka_conf_new(),rd_kafka_conf_destroy);
 set(conf.get(),"bootstrap.servers",broker);
 set(conf.get(),"socket.timeout.ms","3000");
 if(type==RD_KAFKA_CONSUMER){
  rd_kafka_conf_set_opaque(conf.get(),ownership);rd_kafka_conf_set_rebalance_cb(conf.get(),rebalance);
  rd_kafka_conf_set_events(conf.get(),RD_KAFKA_EVENT_OFFSET_COMMIT);
  set(conf.get(),"session.timeout.ms","6000");set(conf.get(),"heartbeat.interval.ms","2000");
  set(conf.get(),"max.poll.interval.ms","60000");set(conf.get(),"partition.assignment.strategy","range");
  set(conf.get(),"fetch.max.bytes","1048576");set(conf.get(),"receive.message.max.bytes","2097152");
 }
 if(type==RD_KAFKA_CONSUMER){set(conf.get(),"group.id",group);set(conf.get(),"enable.auto.commit","false");set(conf.get(),"enable.auto.offset.store","false");set(conf.get(),"auto.offset.reset","earliest");set(conf.get(),"queued.max.messages.kbytes","1024");set(conf.get(),"fetch.message.max.bytes","65536");}
 else {set(conf.get(),"enable.idempotence","true");set(conf.get(),"acks","all");set(conf.get(),"message.timeout.ms","5000");set(conf.get(),"queue.buffering.max.kbytes","1024");set(conf.get(),"queue.buffering.max.messages","100");rd_kafka_conf_set_dr_msg_cb(conf.get(),delivered);}
 char error[512];auto* raw=rd_kafka_new(type,conf.get(),error,sizeof(error));if(!raw)throw std::runtime_error(error);conf.release();return Kafka(raw,destroy_kafka);
}
bool publish(rd_kafka_t* producer,const std::string& topic,const std::string& key,std::string payload) {
 auto delivery=std::make_shared<Delivery>();
 auto owner=std::make_unique<std::shared_ptr<Delivery>>(delivery);
 std::array<rd_kafka_vu_t,5> values{};
 values[0].vtype=RD_KAFKA_VTYPE_TOPIC;values[0].u.cstr=topic.c_str();
 values[1].vtype=RD_KAFKA_VTYPE_KEY;values[1].u.mem={const_cast<char*>(key.data()),key.size()};
 values[2].vtype=RD_KAFKA_VTYPE_VALUE;values[2].u.mem={payload.data(),payload.size()};
 values[3].vtype=RD_KAFKA_VTYPE_MSGFLAGS;values[3].u.i=RD_KAFKA_MSG_F_COPY;
 values[4].vtype=RD_KAFKA_VTYPE_OPAQUE;values[4].u.ptr=owner.get();
 std::unique_ptr<rd_kafka_error_t,decltype(&rd_kafka_error_destroy)> error(rd_kafka_produceva(producer,values.data(),values.size()),rd_kafka_error_destroy);
 if(error)return false;
 owner.release();
 // This worker dispatches callbacks. Correlation is independently owned until delivery completion.
 auto result=rd_kafka_flush(producer,6000);
 if(result){rd_kafka_purge(producer,RD_KAFKA_PURGE_F_QUEUE|RD_KAFKA_PURGE_F_INFLIGHT);rd_kafka_flush(producer,1000);}
 return delivery->done&&delivery->success;
}
bool commit_confirmed(rd_kafka_t* consumer,const rd_kafka_topic_partition_list_t& offsets){
 KafkaQueue queue(rd_kafka_queue_new(consumer),rd_kafka_queue_destroy);
 if(!queue||rd_kafka_commit_queue(consumer,&offsets,queue.get(),nullptr,nullptr))return false;
 std::unique_ptr<rd_kafka_event_t,decltype(&rd_kafka_event_destroy)> event(rd_kafka_queue_poll(queue.get(),3000),rd_kafka_event_destroy);
 if(!event||rd_kafka_event_type(event.get())!=RD_KAFKA_EVENT_OFFSET_COMMIT||rd_kafka_event_error(event.get()))return false;
 const auto* result=rd_kafka_event_topic_partition_list(event.get());
 if(!result||result->cnt!=offsets.cnt)return false;
 for(int i=0;i<result->cnt;++i)if(result->elems[i].err)return false;
 return true;
}
}
int nexstream::processor::infrastructure::run_processor(int argc,char** argv){
 try {
  std::signal(SIGINT,stop);std::signal(SIGTERM,stop);
  std::string broker=argc>1?argv[1]:"127.0.0.1:19092",group=argc>2?argv[2]:"nexstream.processor.v1";
  if(group.empty()||group.size()>128||broker.empty()||broker.size()>4096)throw std::invalid_argument("group/bootstrap bound");
  for(unsigned char c:group)if(c<32||c==127)throw std::invalid_argument("control character in group id");
  nexstream::processor::domain::Config config;
  auto number=[]<typename T>(const char* text,T& target){std::string_view input(text);auto [end,error]=std::from_chars(input.data(),input.data()+input.size(),target);if(error!=std::errc{}||end!=input.data()+input.size())throw std::invalid_argument("invalid numeric configuration");};
  if(argc>3)number(argv[3],config.cpu);
  if(argc>4)number(argv[4],config.ram);
  if(argc>5)number(argv[5],config.consecutive);
  config.max_hosts=128;config.max_ids_per_host=16;
  Ownership ownership{config,{}, {}, "nexstream-replay-v1:"};
  constexpr char digits[]="0123456789abcdef";
  for(unsigned char c:nexstream::processor::domain::Evaluator(config).checkpoint()){ownership.fingerprint+=digits[c>>4];ownership.fingerprint+=digits[c&15];}
  // Local files are diagnostics only. Kafka committed prefix is the recovery authority.
  Checkpoint checkpoint(argc>6?argv[6]:"processor-state.checkpoint");
  auto producer=create(RD_KAFKA_PRODUCER,broker,group),consumer=create(RD_KAFKA_CONSUMER,broker,group,&ownership);
  rd_kafka_poll_set_consumer(consumer.get());
  std::unique_ptr<rd_kafka_topic_partition_list_t,decltype(&rd_kafka_topic_partition_list_destroy)> topics(rd_kafka_topic_partition_list_new(1),rd_kafka_topic_partition_list_destroy);
  rd_kafka_topic_partition_list_add(topics.get(),"telemetry.events.v1",RD_KAFKA_PARTITION_UA);
  if(rd_kafka_subscribe(consumer.get(),topics.get()))throw std::runtime_error("subscribe");
  int code=0;
  auto next_probe=std::chrono::steady_clock::now()+std::chrono::seconds(5);
  std::optional<std::chrono::steady_clock::time_point> outage_deadline;
  while(!stopped){
   std::unique_ptr<rd_kafka_message_t,decltype(&rd_kafka_message_destroy)> message(rd_kafka_consumer_poll(consumer.get(),200),rd_kafka_message_destroy);
   if(std::chrono::steady_clock::now()>=next_probe){
    const rd_kafka_metadata_t* raw=nullptr;
    auto error=rd_kafka_metadata(consumer.get(),0,nullptr,&raw,500);
    std::unique_ptr<const rd_kafka_metadata_t,decltype(&rd_kafka_metadata_destroy)> metadata(raw,rd_kafka_metadata_destroy);
    bool healthy=!error;
    if(healthy)for(int i=0;i<metadata->topic_cnt;++i)if(metadata->topics[i].err)healthy=false;
    if(healthy)for(auto& [key,state]:ownership.partitions){int64_t low=0,high=0;auto watermark_error=rd_kafka_query_watermark_offsets(consumer.get(),key.first.c_str(),key.second,&low,&high,500);if(watermark_error){healthy=false;continue;}if(low>state.next_offset())throw std::runtime_error("source retention overtook processing offset");state.observe_backlog(high);}
    const auto now=std::chrono::steady_clock::now();
    if(healthy)outage_deadline.reset();
    else if(!outage_deadline)outage_deadline=now+std::chrono::seconds(30);
    else if(now>=*outage_deadline)throw std::runtime_error("broker reconnect budget exceeded");
    next_probe=now+std::chrono::seconds(5);
   }
   if(!ownership.failure.empty())throw std::runtime_error(ownership.failure);
   if(ownership.awaiting_assignment&&std::chrono::steady_clock::now()>=ownership.assignment_deadline)throw std::runtime_error("consumer assignment budget exceeded");
   for(const auto& [key,state]:ownership.partitions){(void)key;state.check_deadline();}
   if(!message)continue;
   if(message->err){if(message->err==RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE||message->err==RD_KAFKA_RESP_ERR__BAD_MSG||message->err==RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED||message->err==RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED||message->err==RD_KAFKA_RESP_ERR__FATAL||message->err==RD_KAFKA_RESP_ERR__AUTHENTICATION||message->err==RD_KAFKA_RESP_ERR__MAX_POLL_EXCEEDED)throw std::runtime_error("fatal/bounded Kafka input failure");continue;}
   const std::string topic=rd_kafka_topic_name(message->rkt);
   auto found=ownership.partitions.find({topic,message->partition});
   if(found==ownership.partitions.end())throw std::runtime_error("record from unowned partition");
   auto& state=found->second;
   bool success=true;
   std::optional<nexstream::processor::domain::Event> event;
   try{event=nexstream::processor::infrastructure::parse_event(std::string_view(message->payload?static_cast<const char*>(message->payload):"",message->len));}
   catch(const std::bad_alloc&){throw;}
   catch(const std::exception&){event.reset();}
   if(event&&(!message->key||std::string_view(static_cast<const char*>(message->key),message->key_len)!=event->host_id))event.reset();
   auto result=state.process(message->offset,event);
   if(result.replayed)continue;
   if(!event)success=publish(producer.get(),"telemetry.dlq.v1","invalid",nexstream::processor::infrastructure::encode_dlq("invalid telemetry v1",topic,message->partition,message->offset));
   if(event){
    for(const auto& alert:result.alerts)if(!publish(producer.get(),"telemetry.alerts.v1",alert.host_id,nexstream::processor::infrastructure::encode_alert(alert))){success=false;break;}
   }
   if(!success){code=2;break;}
   if(rd_kafka_assignment_lost(consumer.get())){code=2;break;}
   checkpoint.save(group+"\n"+topic+"\n"+std::to_string(message->partition)+" "+std::to_string(state.next_offset())+"\n"+state.snapshot());
   rd_kafka_topic_partition_t offset{};offset.topic=const_cast<char*>(topic.c_str());offset.partition=message->partition;offset.offset=message->offset+1;
   // Borrow immutable metadata during enqueue; Kafka copies the request into its owned operation.
   offset.metadata=ownership.fingerprint.data();offset.metadata_size=ownership.fingerprint.size();
   rd_kafka_topic_partition_list_t offsets{};offsets.cnt=1;offsets.size=1;offsets.elems=&offset;
   if(!commit_confirmed(consumer.get(),offsets)){code=2;break;}
  }
  return code;
 }catch(const std::exception& e){std::cerr<<e.what()<<'\n';return 1;}
}
