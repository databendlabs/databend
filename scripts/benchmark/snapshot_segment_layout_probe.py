#!/usr/bin/env python3
"""Model the in-memory segment-layout algorithm; no Databend server is used.

Counts logical summary reads with the segment cache DISABLED, including summaries
read twice by binary probing and subsequent segment pruning. This is not a Rust
integration benchmark or a wall-clock estimate. Input ranges are the simulated
storage, NOT metadata retained by the layout index. Learning IO is reported
separately from query IO. Nothing is sorted or rewritten in the input snapshot.

Run: python3 scripts/benchmark/snapshot_segment_layout_probe.py
"""

import argparse
import bisect
from collections import Counter
from dataclasses import dataclass
from itertools import pairwise

MAX_RUNS = 128  # Matches SnapshotSegmentLayout.


@dataclass
class Run:
    start: int
    end: int
    first: tuple[int, int]
    last: tuple[int, int]

    def descending(self):
        return any(a > b for a, b in zip(self.first, self.last))

    def bounds(self):
        return (
            (self.last[0], self.first[1])
            if self.descending()
            else (self.first[0], self.last[1])
        )

    def joins(self, other):
        endpoints = (self.first, self.last, other.first, other.last)
        ascending = all(a[0] <= b[0] and a[1] <= b[1] for a, b in pairwise(endpoints))
        descending = all(a[0] >= b[0] and a[1] >= b[1] for a, b in pairwise(endpoints))
        return self.end == other.start and (ascending or descending)

    def join(self, other):
        self.end, self.last = other.end, other.last


def learn(segments, indices):
    """Keep only endpoints of observed, contiguous monotone runs; preserve holes."""
    runs = []
    reads = 0
    for index in indices:
        reads += 1
        if runs is None:
            continue
        position = bisect.bisect_right([run.start for run in runs], index)
        if position and index < runs[position - 1].end:
            continue
        if segments[index] is None:
            continue
        run = Run(index, index + 1, segments[index], segments[index])
        if position and runs[position - 1].joins(run):
            position -= 1
            previous = runs.pop(position)
            previous.join(run)
            run = previous
        if position < len(runs) and run.joins(runs[position]):
            run.join(runs.pop(position))
        runs.insert(position, run)
        if len(runs) > MAX_RUNS:
            runs = None  # Disable learning, not the original metadata scan.
    return runs, reads


def query(segments, runs, lower, upper):
    reads = Counter()
    probes_count = 0

    def read(index):
        reads[index] += 1
        return segments[index]

    def keep(bounds):
        return bounds is None or (bounds[0] <= upper and lower <= bounds[1])

    candidates = []
    covered = 0
    for run in runs or []:
        candidates.extend(range(covered, run.start))
        covered = run.end
        minimum, maximum = run.bounds()
        if not keep((minimum, maximum)):
            continue
        start, end = run.start, run.end
        probes = {}
        for prefix in (True, False):
            low, high = start, end
            while low < high:
                mid = low + (high - low) // 2
                if mid not in probes:
                    probes[mid] = read(mid)
                    probes_count += 1
                column = probes[mid]
                bounds = (
                    (minimum, column[1])
                    if prefix != run.descending()
                    else (column[0], maximum)
                )
                if keep(bounds) == prefix:
                    high = mid
                else:
                    low = mid + 1
            if prefix:
                start = low
            else:
                end = low
        candidates.extend(range(start, end))
    candidates.extend(range(covered, len(segments)))
    # Never mask duplicate candidate segments by silently deduplicating them.
    assert len(candidates) == len(set(candidates)), "duplicate candidate segments"
    matched = [index for index in candidates if keep(read(index))]
    expected = [index for index, bounds in enumerate(segments) if keep(bounds)]
    assert matched == expected, (
        "pruning lost matching segments or changed snapshot order"
    )
    total = sum(reads.values())
    return (
        len(candidates),
        probes_count,
        total,
        len(reads),
        total - len(reads),
        len(matched),
    )


def interleaved_scan(count, workers):
    """Round-robin completion of contiguous worker batches (not a timing model)."""
    batch, remainder = divmod(count, workers)
    for offset in range(batch + bool(remainder)):
        for worker in range(workers):
            start = worker * batch + min(worker, remainder)
            if offset < batch + (worker < remainder):
                yield start + offset


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--segments", type=int, default=9028)
    parser.add_argument(
        "--matches",
        type=int,
        default=43,
        help="query width; exact matches depend on overlap",
    )
    args = parser.parse_args()
    count, width = args.segments, args.matches
    if count < 256 or not 1 <= width <= count // 4:
        parser.error("need segments >= 256 and 1 <= matches <= segments/4")
    ordered = [(index, index) for index in range(count)]
    lower = count // 2 - width // 2
    # Shuffle the order of disjoint sorted chunks, not their contents. This
    # models multiple locally ordered outputs in an unordered snapshot list.
    chunks = [ordered[i * count // 43 : (i + 1) * count // 43] for i in range(43)]
    shuffled = [bounds for i in range(43) for bounds in chunks[(i * 19) % 43]]
    repeated = [(i % (count // 43), i % (count // 43)) for i in range(count)]
    scenarios = [
        ("cold: no evidence", ordered, [], lower),
        ("ascending: 50% observed", ordered, range(count // 4, count * 3 // 4), lower),
        ("ascending: all observed", ordered, range(count), lower),
        ("descending: all observed", ordered[::-1], range(count), lower),
        ("64 interleaved worker batches", ordered, interleaved_scan(count, 64), lower),
        (
            "256 interleaved worker batches",
            ordered,
            interleaved_scan(count, 256),
            lower,
        ),
        ("43 disjoint sorted chunks", shuffled, range(count), lower),
        (
            "overlapping ranges (+10)",
            [(i, i + 10) for i in range(count)],
            range(count),
            lower,
        ),
        (
            "43 overlapping append runs",
            repeated,
            range(count),
            (count // 43 - width) // 2,
        ),
        (
            "too fragmented: disabled",
            [(i, count * 2 - i) for i in range(count)],
            range(count),
            0,
        ),
    ]
    print(
        "MODEL ONLY; segment cache disabled; all correctness/uniqueness assertions enabled"
    )
    print(
        f"snapshot={count} segments, query width={width}, directory limit={MAX_RUNS} runs"
    )
    print(
        f"{'scenario':31} {'learn':>6} {'runs':>5} {'cand':>6} {'probe':>6} "
        f"{'reads':>6} {'unique':>6} {'repeat':>6} {'match':>6} {'reduction':>10}"
    )
    for name, segments, observed, query_lower in scenarios:
        runs, learning_reads = learn(segments, observed)
        result = query(segments, runs, query_lower, query_lower + width - 1)
        candidates, probes, total, unique, repeated_reads, matched = result
        run_count = "off" if runs is None else str(len(runs))
        print(
            f"{name:31} {learning_reads:6} {run_count:>5} {candidates:6} {probes:6} "
            f"{total:6} {unique:6} {repeated_reads:6} {matched:6} "
            f"{1 - total / count:9.2%}"
        )
    print("learn=one-time learning reads; reads=probe+candidates, NOT learn+query")
    print(
        "repeat=probe metadata read again during normal pruning; candidates themselves are unique"
    )
    print(
        "Partial evidence never excludes unread holes; overlapping append runs can match many segments."
    )


if __name__ == "__main__":
    main()
