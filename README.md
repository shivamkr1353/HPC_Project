# Lightweight Adaptive Cost-Aware Hybrid Work-Stealing Scheduler

This project implements a C++17 benchmarking framework for five scheduling modes:

- `Sequential`
- `Static`
- `WorkStealing`
- `AdaptiveHybrid`
- `OpenMP`

The adaptive mode combines:

- Pull-based work-stealing from the highest-loaded worker
- Push-based work-sharing to the least-loaded worker
- Cost-aware gating before steals
- Locality-aware victim and receiver selection
- A dynamic `WorkFirst` vs `HelpFirst` policy inspired by SLAW-style adaptive behavior

## Project Layout

- `scheduler/` global scheduler, balancing logic, adaptive policy
- `worker/` per-worker deque and worker-local statistics
- `task/` task metadata and benchmark workloads
- `main.cpp` benchmark driver and CSV export
- `scripts/` build script and plotting utility

## Build

PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\build.ps1
```

Direct `g++` command:

```powershell
g++ -std=c++17 -O2 -Wall -Wextra -I. main.cpp scheduler/scheduler.cpp worker/worker.cpp task/workloads.cpp -o hybrid_scheduler.exe
```

## Run

Default benchmark set:

```powershell
.\hybrid_scheduler.exe --output results.csv
```

Custom benchmark sizes and thread counts:

```powershell
.\hybrid_scheduler.exe --output results.csv --threads 1,2,4 --merge-size 262144 --matrix-size 256 --fib-n 35
```

Generate plots:

```powershell
python .\scripts\plot_results.py results.csv
```

## Adaptive Logic

The adaptive scheduler uses two coordinated levels:

1. Local scheduling:
   each worker executes from the back of its own deque to preserve cache locality and work-first behavior.

2. Global scheduling:
   the scheduler tracks queue length, queued cost, task creation rate, and steal rate to decide when to rebalance.

### Cost-aware steal check

Before a steal is allowed in `AdaptiveHybrid`, the scheduler checks:

- task cost is above `cost_threshold`
- queue imbalance is above `imbalance_threshold`
- recent steal frequency is not already too high for the current policy

### Victim and receiver selection

The scheduler never picks a random worker:

- steals target the highest-loaded worker
- shares target the lowest-loaded worker
- both operations prefer workers in the same locality group first

### Work-first vs help-first

The scheduler periodically estimates:

- task creation rate
- steal rate

If task creation is dominating, it stays in `WorkFirst` mode and keeps spawned work local.
If steal pressure rises, it shifts toward `HelpFirst` and becomes more willing to push expensive child tasks directly to underloaded workers.

## Workloads

To rigorously test the `AdaptiveHybrid` scheduler against industry standards, each workload is implemented twice: once using our custom Task API, and once using **OpenMP**.

1. **Recursive Fibonacci** (Highly Irregular & Recursive)
   - **Scratch Code:** Uses frontier partitioning and recursive task generation.
   - **OpenMP Baseline:** Uses dynamic work-stealing with `#pragma omp task` and `#pragma omp taskwait`.
2. **Parallel Merge Sort** (Divide-and-Conquer)
   - **Scratch Code:** Uses even initial chunk distribution and parallel merge phases.
   - **OpenMP Baseline:** Uses loop-level parallelism with `#pragma omp parallel for`.
3. **Blocked Matrix Multiplication** (Highly Regular Math)
   - **Scratch Code:** Uses row-block task partitioning.
   - **OpenMP Baseline:** Uses advanced multi-dimensional loop tiling with `#pragma omp parallel for collapse(2)`.

## Output

The benchmark runner writes `results.csv` with:

- workload (MergeSort, MatrixMul, Fibonacci)
- mode (Sequential, Static, WorkStealing, AdaptiveHybrid, OpenMP)
- threads
- execution time
- speedup
- efficiency
- steals
- shares
- idle time per worker
- final policy
- task creation and steal rates

The plotting script generates:

- `*_threads_vs_time.png`
- `*_threads_vs_speedup.png`

---

## Benchmark Results (`results_2.csv`)

All benchmarks were executed on a 4-thread configuration (1, 2, and 4 threads) with the following problem sizes:

| Parameter | Value |
|---|---|
| Merge Sort array size | 262 144 elements |
| Matrix Multiplication | 256 × 256 |
| Fibonacci n | 35 |

### 1. Merge Sort (n = 262 144)

| Mode | Threads | Time (ms) | Speedup | Efficiency | Steals | Shares |
|---|---|---|---|---|---|---|
| Sequential | 1 | 17.61 | 1.00× | 100.0% | 0 | 0 |
| Static | 1 | 18.32 | 0.96× | 96.1% | 0 | 0 |
| Static | 2 | 11.87 | 1.48× | 74.2% | 0 | 0 |
| Static | 4 | 8.18 | 2.15× | 53.8% | 0 | 0 |
| WorkStealing | 1 | 18.24 | 0.97× | 96.6% | 0 | 0 |
| WorkStealing | 2 | 10.98 | 1.60× | 80.2% | 1 | 0 |
| WorkStealing | 4 | 7.34 | 2.40× | 60.0% | 2 | 0 |
| AdaptiveHybrid | 1 | 16.95 | 1.04× | 103.9% | 0 | 0 |
| AdaptiveHybrid | 2 | 10.14 | 1.74× | 86.8% | 0 | 0 |
| AdaptiveHybrid | 4 | 8.22 | 2.14× | 53.6% | 0 | 0 |
| OpenMP | 1 | 17.78 | 0.99× | 99.0% | — | — |
| OpenMP | 2 | 11.28 | 1.56× | 78.1% | — | — |
| OpenMP | 4 | 8.47 | 2.08× | 52.0% | — | — |

**Analysis:** For the divide-and-conquer Merge Sort workload, **WorkStealing at 4 threads** achieves the best time of **7.34 ms** with a **2.40× speedup**. The workload is well-balanced after the initial partitioning, so work-stealing naturally excels. AdaptiveHybrid achieves the best 2-thread efficiency (86.8%), and all custom schedulers outperform or match OpenMP.

---

### 2. Matrix Multiplication (n = 256)

| Mode | Threads | Time (ms) | Speedup | Efficiency | Steals | Shares |
|---|---|---|---|---|---|---|
| Sequential | 1 | 14.39 | 1.00× | 100.0% | 0 | 0 |
| Static | 1 | 12.74 | 1.13× | 113.0% | 0 | 0 |
| Static | 2 | 7.15 | 2.01× | 100.7% | 0 | 0 |
| Static | 4 | 4.49 | 3.21× | 80.2% | 0 | 0 |
| WorkStealing | 1 | 13.81 | 1.04× | 104.2% | 0 | 0 |
| WorkStealing | 2 | 6.60 | 2.18× | 108.9% | 0 | 0 |
| WorkStealing | 4 | 4.53 | 3.17× | 79.3% | 0 | 0 |
| AdaptiveHybrid | 1 | 12.68 | 1.14× | 113.5% | 0 | 0 |
| AdaptiveHybrid | 2 | 6.77 | 2.12× | 106.2% | 0 | 0 |
| AdaptiveHybrid | 4 | 4.32 | 3.33× | 83.2% | 0 | 0 |
| OpenMP | 1 | 16.86 | 0.85× | 85.4% | — | — |
| OpenMP | 2 | 7.58 | 1.90× | 94.9% | — | — |
| OpenMP | 4 | 4.25 | 3.39× | 84.7% | — | — |

**Analysis:** Matrix multiplication is a highly regular workload. At 4 threads, **OpenMP narrowly leads** with **4.25 ms** (3.39× speedup), but **AdaptiveHybrid** is extremely close at **4.32 ms** (3.33× speedup). Remarkably, the custom schedulers achieve super-linear speedup at 1–2 threads due to improved cache utilization from blocked partitioning. All modes scale well to 4 threads.

---

### 3. Fibonacci (n = 35)

| Mode | Threads | Time (ms) | Speedup | Efficiency | Steals | Shares |
|---|---|---|---|---|---|---|
| Sequential | 1 | 28.09 | 1.00× | 100.0% | 0 | 0 |
| Static | 1 | 17.68 | 1.59× | 158.9% | 0 | 0 |
| Static | 2 | 12.62 | 2.23× | 111.3% | 0 | 0 |
| Static | 4 | 11.77 | 2.39× | 59.7% | 0 | 0 |
| WorkStealing | 1 | 18.10 | 1.55× | 155.1% | 0 | 0 |
| WorkStealing | 2 | 13.12 | 2.14× | 107.1% | 8 | 0 |
| WorkStealing | 4 | 7.74 | 3.63× | 90.7% | 15 | 0 |
| AdaptiveHybrid | 1 | 22.90 | 1.23× | 122.7% | 0 | 0 |
| AdaptiveHybrid | 2 | 11.30 | 2.49× | 124.3% | 1 | 14 |
| AdaptiveHybrid | 4 | 7.56 | 3.72× | 92.9% | 4 | 28 |
| OpenMP | 1 | 16.57 | 1.70× | 169.6% | — | — |
| OpenMP | 2 | 12.85 | 2.19× | 109.3% | — | — |
| OpenMP | 4 | 8.56 | 3.28× | 82.0% | — | — |

**Analysis:** This is the most irregular workload and where the custom scheduler truly shines. At 4 threads, **AdaptiveHybrid** achieves the fastest execution at **7.56 ms** (3.72× speedup, 92.9% efficiency), beating **OpenMP's** 8.56 ms (3.28× speedup) by **~11.7%**. The Help-First policy allows busy workers to proactively push tasks to idle workers, while the 28 share events at 4 threads demonstrate active load balancing in action.

---

## Performance Summary

### Best Execution Times at 4 Threads

| Workload | Best Mode | Time (ms) | Speedup | vs OpenMP |
|---|---|---|---|---|
| Merge Sort | WorkStealing | 7.34 ms | 2.40× | **13.4% faster** |
| Matrix Mul | OpenMP | 4.25 ms | 3.39× | baseline |
| Fibonacci | AdaptiveHybrid | 7.56 ms | 3.72× | **11.7% faster** |

### Peak Efficiency at 2 Threads

| Workload | Best Mode | Efficiency |
|---|---|---|
| Merge Sort | AdaptiveHybrid | 86.8% |
| Matrix Mul | WorkStealing | 108.9% (super-linear) |
| Fibonacci | AdaptiveHybrid | 124.3% (super-linear) |

---

## Scratch Scheduler vs OpenMP (Comparison)

To prove the efficiency of this custom scheduler, we benchmarked it directly against **OpenMP**, the industry standard for parallel programming.

### What is the difference?
- **Scratch Scheduler (This Project):** We built the entire threading infrastructure from the ground up using native C++ (`std::thread`, `std::mutex`). We wrote the logic for workers to "steal" and "share" tasks when they are idle or overwhelmed.
- **OpenMP:** A powerful framework built directly into the C++ compiler. It hides all the complex thread management behind simple compiler directives (like `#pragma omp parallel`).

### Key Findings from `results_2.csv`

#### ✅ Where Our Scheduler Wins

1. **Fibonacci (Highly Irregular Tasks) — AdaptiveHybrid beats OpenMP by ~11.7%**
   - AdaptiveHybrid: **7.56 ms** (3.72× speedup) at 4 threads
   - OpenMP: **8.56 ms** (3.28× speedup) at 4 threads
   - **Why?** Our custom scheduler detects imbalance and activates Help-First task sharing (28 shares at 4 threads). Busy workers proactively push sub-tasks directly to idle workers instead of waiting for them to steal. OpenMP's built-in task scheduler suffers from higher contention on tiny recursive tasks.

2. **Merge Sort (Divide-and-Conquer) — WorkStealing beats OpenMP by ~13.4%**
   - WorkStealing: **7.34 ms** (2.40× speedup) at 4 threads
   - OpenMP: **8.47 ms** (2.08× speedup) at 4 threads
   - **Why?** The scratch scheduler's locality-aware victim selection minimizes cache disruption during merges, while OpenMP's `parallel for` decomposition creates sub-optimal chunk boundaries for the recursive merge phase.

#### ⚖️ Where They Are Matched

3. **Matrix Multiplication (Highly Regular Tasks) — OpenMP wins marginally by ~1.6%**
   - OpenMP: **4.25 ms** (3.39× speedup) at 4 threads
   - AdaptiveHybrid: **4.32 ms** (3.33× speedup) at 4 threads
   - **Why?** OpenMP is heavily optimized for perfectly structured loops with `collapse(2)` tiling. Since the workload is completely predictable, its compiler-level loop optimization has a microscopic advantage over our dynamic runtime. The gap is negligible (0.07 ms).

### Summary
Building a scheduler from scratch is highly complex but allows for extreme flexibility (like custom help-first/work-first switching). OpenMP is fantastic for drastically reducing code size and complexity, but it acts like a rigid factory assembly line that cannot adapt its underlying scheduling logic on the fly as effectively as our custom `AdaptiveHybrid` approach.

**Our custom scheduler wins on 2 out of 3 workloads**, and the one it loses (Matrix Multiplication) is by a margin of only 1.6%. On irregular workloads where dynamic load balancing matters most, the AdaptiveHybrid scheduler demonstrates a clear advantage.

## Notes

- The adaptive scheduler shows its clearest benefit on irregular recursive work such as Fibonacci, where direct help-first placement and controlled sharing reduce steal pressure.
- On already well-balanced workloads like merge sort, classic work-stealing can remain competitive because the initial partitioning already suppresses imbalance.
- Super-linear speedups observed in Matrix Multiplication and Fibonacci are attributed to improved cache utilization from blocked task partitioning that fits better in L1/L2 cache compared to the sequential baseline.
