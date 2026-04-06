from __future__ import annotations

import csv
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt


def load_rows(csv_path: Path) -> list[dict[str, str]]:
    with csv_path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def group_by_workload(rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[row["workload"]].append(row)
    return grouped


def plot_metric(rows: list[dict[str, str]], workload: str, metric: str, ylabel: str, output_path: Path) -> None:
    grouped: dict[str, list[tuple[int, float]]] = defaultdict(list)
    for row in rows:
        grouped[row["mode"]].append((int(row["threads"]), float(row[metric])))

    plt.figure(figsize=(9, 5))
    for mode, points in sorted(grouped.items()):
        points.sort(key=lambda item: item[0])
        x_values = [thread_count for thread_count, _ in points]
        y_values = [value for _, value in points]
        plt.plot(x_values, y_values, marker="o", linewidth=2, label=mode)

    plt.title(f"{workload}: Threads vs {ylabel}")
    plt.xlabel("Threads")
    plt.ylabel(ylabel)
    plt.grid(True, linestyle="--", alpha=0.35)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=160)
    plt.close()


def main() -> int:
    csv_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("results.csv")
    rows = load_rows(csv_path)
    grouped = group_by_workload(rows)

    output_dir = csv_path.parent
    for workload, workload_rows in grouped.items():
        safe_name = workload.lower().replace(" ", "_")
        plot_metric(
            workload_rows,
            workload,
            "execution_ms",
            "Execution Time (ms)",
            output_dir / f"{safe_name}_threads_vs_time.png",
        )
        plot_metric(
            workload_rows,
            workload,
            "speedup",
            "Speedup",
            output_dir / f"{safe_name}_threads_vs_speedup.png",
        )

    print(f"Plots saved next to {csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
