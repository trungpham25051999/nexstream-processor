#pragma once
#include "nexstream/processor/domain/evaluator.hpp"
#include <chrono>
#include <optional>

namespace nexstream::processor::application {
struct PartitionResult {
    bool replayed;
    std::vector<domain::Alert> alerts;
};
// One owner/polling thread. Reconstruct the committed prefix before producing new output.
class PartitionState final {
public:
    PartitionState(domain::Config config, std::int64_t committed_next);
    PartitionResult process(std::int64_t offset, const std::optional<domain::Event>& event);
    bool recovering() const noexcept { return next_offset_ < committed_next_; }
    void check_deadline() const;
    void observe_backlog(std::int64_t high_watermark);
    std::int64_t next_offset() const noexcept { return next_offset_; }
    std::string snapshot() const { return evaluator_.checkpoint(); }
private:
    domain::Evaluator evaluator_;
    std::int64_t committed_next_;
    std::int64_t next_offset_{0};
    std::chrono::steady_clock::time_point deadline_;
    std::int64_t observed_offset_{-1};
    std::optional<std::chrono::steady_clock::time_point> progress_deadline_;
};
}
