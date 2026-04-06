#pragma once

#include <cstddef>
#include <cstdint>

#include "scheduler/scheduler.h"

namespace hybrid {

std::uint64_t run_recursive_fibonacci(Scheduler& scheduler, int n);
std::uint64_t run_parallel_merge_sort(Scheduler& scheduler, std::size_t element_count);
double run_matrix_multiplication(Scheduler& scheduler, std::size_t matrix_size);

}  // namespace hybrid
