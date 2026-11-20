#pragma once
#include "nexstream/processor/domain/evaluator.hpp"
#include <string_view>
namespace nexstream::processor::infrastructure {
domain::Event parse_event(std::string_view text);
std::string encode_alert(const domain::Alert& alert);
std::string encode_dlq(std::string_view reason,std::string_view topic,int partition,std::int64_t offset);
}
