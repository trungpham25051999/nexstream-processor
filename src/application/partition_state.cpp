#include "nexstream/processor/application/partition_state.hpp"
#include <stdexcept>
namespace nexstream::processor::application {
PartitionState::PartitionState(domain::Config config, std::int64_t committed_next)
    : evaluator_(config), committed_next_(committed_next),
      deadline_(std::chrono::steady_clock::now() + std::chrono::seconds(30)) {
    if (committed_next < 0 || committed_next > 100000)
        throw std::runtime_error("recovery prefix exceeds 100000-record bound");
}
void PartitionState::check_deadline() const {
    if (recovering() && std::chrono::steady_clock::now() > deadline_)
        throw std::runtime_error("partition recovery deadline exceeded");
}
void PartitionState::observe_backlog(std::int64_t high_watermark) {
    if(high_watermark<=next_offset_){progress_deadline_.reset();return;}
    const auto now=std::chrono::steady_clock::now();
    if(!progress_deadline_||observed_offset_!=next_offset_){observed_offset_=next_offset_;progress_deadline_=now+std::chrono::seconds(30);}
    else if(now>=*progress_deadline_)throw std::runtime_error("partition input progress budget exceeded");
}
PartitionResult PartitionState::process(std::int64_t offset, const std::optional<domain::Event>& event) {
    check_deadline();
    if (offset != next_offset_) throw std::runtime_error("source offset gap: full uncompacted history required");
    const bool replay = recovering();
    auto alerts = event ? evaluator_.evaluate(*event) : std::vector<domain::Alert>{};
    ++next_offset_;
    if (replay) alerts.clear();
    return {replay, std::move(alerts)};
}
}
