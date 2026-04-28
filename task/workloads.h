#pragma once

#include <cstddef>
#include <cstdint>

#include "scheduler/scheduler.h"

namespace hybrid {

std::uint64_t run_recursive_fibonacci(Scheduler& scheduler, int n);
std::uint64_t run_parallel_merge_sort(Scheduler& scheduler, std::size_t element_count);
double run_matrix_multiplication(Scheduler& scheduler, std::size_t matrix_size);

std::uint64_t run_openmp_recursive_fibonacci(int n, std::size_t thread_count);
std::uint64_t run_openmp_parallel_merge_sort(std::size_t element_count, std::size_t thread_count);
double run_openmp_matrix_multiplication(std::size_t matrix_size, std::size_t thread_count);

}  // namespace hybrid
