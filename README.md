# Lightweight Adaptive Cost-Aware Hybrid Work-Stealing Scheduler

This project implements a C++17 benchmarking framework for four scheduling modes:

- `Sequential`
- `Static`
- `WorkStealing`
- `AdaptiveHybrid`

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

- Recursive Fibonacci with frontier partitioning and recursive task generation
- Parallel merge sort with even initial chunk distribution and parallel merge phases
- Blocked matrix multiplication with row-block task partitioning

## Output

The benchmark runner writes `results.csv` with:

- workload
- mode
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

## Notes

- The adaptive scheduler shows its clearest benefit on irregular recursive work such as Fibonacci, where direct help-first placement and controlled sharing reduce steal pressure.
- On already well-balanced workloads like merge sort, classic work-stealing can remain competitive because the initial partitioning already suppresses imbalance.
