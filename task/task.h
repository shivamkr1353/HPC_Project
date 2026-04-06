#pragma once

#include <cstddef>
#include <functional>

namespace hybrid {

enum class SchedulingMode {
    Sequential,
    Static,
    WorkStealing,
    AdaptiveHybrid
};

struct TaskMetadata {
    std::size_t estimated_cost{1};
    std::size_t recursion_depth{0};
    int locality_group{-1};
};

struct Task {
    std::function<void()> execute;
    TaskMetadata metadata;
};

}  // namespace hybrid
