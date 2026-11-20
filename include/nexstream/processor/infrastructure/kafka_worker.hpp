#pragma once
namespace nexstream::processor::infrastructure {
// Single-threaded polling worker; transport callbacks are dispatched by this worker.
int run_processor(int argc,char** argv);
}
