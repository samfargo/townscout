#!/usr/bin/env python3
"""
Parse serial logs for telemetry summaries.

Usage:
  python scripts/analyze_telemetry.py logs/remote_runs/<run>-serial.log \
    [--after "starting make d_anchor_category"] [--until "uploaded build.log"]
"""
from __future__ import annotations

import argparse
import re
import statistics
from dataclasses import dataclass, field
from typing import List, Pattern, Tuple

CPU_REGEX: Pattern[str] = re.compile(
    r"\[telemetry\]\[cpu] .*?usr=(?P<usr>\d+\.\d+)%.*?"
    r"sys=(?P<sys>\d+\.\d+)%.*?wa=(?P<wa>\d+\.\d+)%.*?"
    r"idle=(?P<idle>\d+\.\d+)%"
)
VMSTAT_REGEX: Pattern[str] = re.compile(
    r"\[telemetry\]\[vmstat] .*?runq=(?P<runq>\d+)\s+blocked=(?P<blocked>\d+)"
    r"\s+free=(?P<free>\d+)\s+idle=(?P<idle>\d+)%\s+wa=(?P<wa>\d+)%"
)


@dataclass
class CpuSample:
    usr: float
    sys: float
    wa: float
    idle: float


@dataclass
class VmstatSample:
    runq: int
    blocked: int
    free: int
    idle: int
    wa: int


@dataclass
class TelemetryAggregates:
    cpu_samples: List[CpuSample] = field(default_factory=list)
    vmstat_samples: List[VmstatSample] = field(default_factory=list)

    def summary(self) -> str:
        parts: List[str] = []
        if self.cpu_samples:
            usr = statistics.mean(s.usr for s in self.cpu_samples)
            sys = statistics.mean(s.sys for s in self.cpu_samples)
            wa = statistics.mean(s.wa for s in self.cpu_samples)
            idle = statistics.mean(s.idle for s in self.cpu_samples)
            parts.append(
                f"CPU samples={len(self.cpu_samples)} "
                f"usr={usr:.2f}% sys={sys:.2f}% wa={wa:.2f}% idle={idle:.2f}%"
            )
        if self.vmstat_samples:
            runq = statistics.mean(s.runq for s in self.vmstat_samples)
            blocked = statistics.mean(s.blocked for s in self.vmstat_samples)
            free = statistics.mean(s.free for s in self.vmstat_samples)
            idle = statistics.mean(s.idle for s in self.vmstat_samples)
            wa = statistics.mean(s.wa for s in self.vmstat_samples)
            parts.append(
                f"vmstat samples={len(self.vmstat_samples)} "
                f"runq={runq:.2f} blocked={blocked:.2f} "
                f"free={free:.0f} idle={idle:.2f}% wa={wa:.2f}%"
            )
        return "\n".join(parts) if parts else "No telemetry samples captured."


def capture_lines(
    lines: List[str], start_token: str | None, end_token: str | None
) -> List[str]:
    capturing = start_token is None
    captured: List[str] = []
    for line in lines:
        if not capturing and start_token and start_token in line:
            capturing = True
        if capturing:
            captured.append(line)
            if end_token and end_token in line:
                break
    return captured


def parse_samples(lines: List[str]) -> TelemetryAggregates:
    agg = TelemetryAggregates()
    for line in lines:
        if "[telemetry][cpu]" in line:
            match = CPU_REGEX.search(line)
            if match:
                agg.cpu_samples.append(
                    CpuSample(
                        usr=float(match.group("usr")),
                        sys=float(match.group("sys")),
                        wa=float(match.group("wa")),
                        idle=float(match.group("idle")),
                    )
                )
        elif "[telemetry][vmstat]" in line:
            match = VMSTAT_REGEX.search(line)
            if match:
                agg.vmstat_samples.append(
                    VmstatSample(
                        runq=int(match.group("runq")),
                        blocked=int(match.group("blocked")),
                        free=int(match.group("free")),
                        idle=int(match.group("idle")),
                        wa=int(match.group("wa")),
                    )
                )
    return agg


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize telemetry from serial logs.")
    parser.add_argument("log_path", help="Path to logs/remote_runs/<run>-serial.log")
    parser.add_argument(
        "--after",
        help="Only start collecting after the first line containing this token.",
    )
    parser.add_argument(
        "--until",
        help="Stop collecting after the first line containing this token (inclusive).",
    )
    args = parser.parse_args()

    with open(args.log_path, "r", encoding="utf-8", errors="ignore") as fh:
        lines = fh.readlines()

    scoped_lines = capture_lines(lines, args.after, args.until)
    agg = parse_samples(scoped_lines)
    print(agg.summary())


if __name__ == "__main__":
    main()
