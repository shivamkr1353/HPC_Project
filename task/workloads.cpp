#include "task/workloads.h"

#include <algorithm>
#include <future>
#include <numeric>
#include <random>
#include <utility>
#include <vector>

namespace hybrid {
namespace {

std::uint64_t sequential_fibonacci(int n) {
    if (n < 2) {
        return static_cast<std::uint64_t>(n);
    }
    return sequential_fibonacci(n - 1) + sequential_fibonacci(n - 2);
}

std::size_t estimate_fibonacci_cost(int n) {
    const int clamped = std::max(0, std::min(n, 20));
    return static_cast<std::size_t>(1ULL << clamped);
}

std::uint64_t fibonacci_impl(Scheduler& scheduler, int n, std::size_t depth) {
    constexpr int kSequentialCutoff = 18;
    constexpr std::size_t kDepthCutoff = 12;

    if (n < 2) {
        return static_cast<std::uint64_t>(n);
    }
    if (n <= kSequentialCutoff || depth >= kDepthCutoff) {
        return sequential_fibonacci(n);
    }

    const TaskMetadata left_metadata{
        estimate_fibonacci_cost(n - 1),
        depth + 1,
        scheduler.current_locality_group(),
    };

    auto left_future = scheduler.submit(
        [&scheduler, n, depth]() { return fibonacci_impl(scheduler, n - 1, depth + 1); },
        left_metadata);
    const std::uint64_t right_value = fibonacci_impl(scheduler, n - 2, depth + 1);
    return scheduler.await(left_future) + right_value;
}

std::uint64_t rolling_hash(const std::vector<int>& values) {
    std::uint64_t hash = 1469598103934665603ULL;
    const std::size_t stride = std::max<std::size_t>(1, values.size() / 64);
    for (std::size_t index = 0; index < values.size(); index += stride) {
        hash ^= static_cast<std::uint64_t>(values[index]);
        hash *= 1099511628211ULL;
    }
    hash ^= static_cast<std::uint64_t>(values.back());
    hash *= 1099511628211ULL;
    return hash;
}

}  // namespace

std::uint64_t run_recursive_fibonacci(Scheduler& scheduler, int n) {
    constexpr int kPartitionCutoff = 22;

    std::vector<std::pair<int, std::size_t>> frontier{{n, 0}};
    while (frontier.size() < scheduler.worker_count()) {
        auto largest = std::max_element(frontier.begin(),
                                        frontier.end(),
                                        [](const auto& lhs, const auto& rhs) {
                                            return lhs.first < rhs.first;
                                        });
        if (largest == frontier.end() || largest->first <= kPartitionCutoff) {
            break;
        }

        const int value = largest->first;
        const std::size_t depth = largest->second;
        frontier.erase(largest);
        frontier.push_back({value - 1, depth + 1});
        frontier.push_back({value - 2, depth + 1});
    }

    std::vector<std::future<std::uint64_t>> futures;
    futures.reserve(frontier.size());
    for (const auto& [value, depth] : frontier) {
        futures.push_back(scheduler.submit(
            [&scheduler, value, depth]() { return fibonacci_impl(scheduler, value, depth); },
            TaskMetadata{estimate_fibonacci_cost(value), depth, -1}));
    }

    std::uint64_t result = 0;
    for (auto& future : futures) {
        result += scheduler.await(future);
    }
    return result;
}

std::uint64_t run_parallel_merge_sort(Scheduler& scheduler, std::size_t element_count) {
    std::mt19937 generator(42);
    std::uniform_int_distribution<int> distribution(0, 1'000'000);

    std::vector<int> data(element_count);
    for (auto& value : data) {
        value = distribution(generator);
    }

    std::vector<int> buffer(element_count);
    const std::size_t chunk_count =
        std::max<std::size_t>(1, std::min<std::size_t>(scheduler.worker_count() * 2,
                                                        std::max<std::size_t>(1, element_count / 2048)));
    const std::size_t chunk_size = (element_count + chunk_count - 1) / chunk_count;

    std::vector<std::future<void>> futures;
    futures.reserve(chunk_count);
    for (std::size_t start = 0; start < element_count; start += chunk_size) {
        const std::size_t end = std::min(start + chunk_size, element_count);
        futures.push_back(scheduler.submit(
            [&data, start, end]() { std::sort(data.begin() + start, data.begin() + end); },
            TaskMetadata{end - start, 0, -1}));
    }
    for (auto& future : futures) {
        scheduler.await(future);
    }

    std::size_t merge_width = chunk_size;
    std::size_t level = 1;
    while (merge_width < element_count) {
        futures.clear();
        auto* source = &data;
        auto* destination = &buffer;

        for (std::size_t start = 0; start < element_count; start += merge_width * 2) {
            const std::size_t middle = std::min(start + merge_width, element_count);
            const std::size_t end = std::min(start + merge_width * 2, element_count);
            futures.push_back(scheduler.submit(
                [source, destination, start, middle, end]() {
                    std::merge(source->begin() + static_cast<long long>(start),
                               source->begin() + static_cast<long long>(middle),
                               source->begin() + static_cast<long long>(middle),
                               source->begin() + static_cast<long long>(end),
                               destination->begin() + static_cast<long long>(start));
                },
                TaskMetadata{std::max<std::size_t>(1, end - start), level, -1}));
        }

        for (auto& future : futures) {
            scheduler.await(future);
        }

        data.swap(buffer);
        merge_width *= 2;
        ++level;
    }

    return rolling_hash(data);
}

double run_matrix_multiplication(Scheduler& scheduler, std::size_t matrix_size) {
    const std::size_t cell_count = matrix_size * matrix_size;
    std::vector<double> matrix_a(cell_count);
    std::vector<double> matrix_b(cell_count);
    std::vector<double> matrix_c(cell_count, 0.0);

    for (std::size_t row = 0; row < matrix_size; ++row) {
        for (std::size_t column = 0; column < matrix_size; ++column) {
            matrix_a[row * matrix_size + column] =
                static_cast<double>(((row + 1) * (column + 3)) % 19) / 19.0;
            matrix_b[row * matrix_size + column] =
                static_cast<double>(((row * 7) + (column * 5) + 11) % 23) / 23.0;
        }
    }

    const std::size_t row_block =
        std::max<std::size_t>(8, matrix_size / std::max<std::size_t>(1, scheduler.worker_count() * 2));
    constexpr std::size_t kTile = 32;

    std::vector<std::future<void>> futures;
    for (std::size_t row_start = 0; row_start < matrix_size; row_start += row_block) {
        const std::size_t row_end = std::min(row_start + row_block, matrix_size);
        futures.push_back(scheduler.submit(
            [&, row_start, row_end]() {
                for (std::size_t ii = row_start; ii < row_end; ii += kTile) {
                    const std::size_t ii_end = std::min(ii + kTile, row_end);
                    for (std::size_t kk = 0; kk < matrix_size; kk += kTile) {
                        const std::size_t kk_end = std::min(kk + kTile, matrix_size);
                        for (std::size_t jj = 0; jj < matrix_size; jj += kTile) {
                            const std::size_t jj_end = std::min(jj + kTile, matrix_size);
                            for (std::size_t i = ii; i < ii_end; ++i) {
                                for (std::size_t k = kk; k < kk_end; ++k) {
                                    const double a_value = matrix_a[i * matrix_size + k];
                                    for (std::size_t j = jj; j < jj_end; ++j) {
                                        matrix_c[i * matrix_size + j] +=
                                            a_value * matrix_b[k * matrix_size + j];
                                    }
                                }
                            }
                        }
                    }
                }
            },
            TaskMetadata{std::max<std::size_t>(1, (row_end - row_start) * matrix_size), 0, -1}));
    }

    for (auto& future : futures) {
        scheduler.await(future);
    }

    double checksum = 0.0;
    for (std::size_t row = 0; row < matrix_size; ++row) {
        checksum += matrix_c[row * matrix_size + (row % matrix_size)];
    }
    return checksum;
}

}  // namespace hybrid
