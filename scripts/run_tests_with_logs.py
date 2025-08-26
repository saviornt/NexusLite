#!/usr/bin/env python3
import datetime
import subprocess
import sys
from pathlib import Path
from typing import List, Tuple, Optional, Sequence, Dict, Any

ROOT = Path(__file__).resolve().parents[1]
LOG_ROOT = ROOT / "test_logs"

# Utilities

def ts() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

def run(cmd: Sequence[str], cwd: Optional[Path] = None) -> Tuple[int, str, float]:
    start: datetime.datetime = datetime.datetime.now()
    proc = subprocess.Popen(
        list(cmd),
        cwd=str(cwd or ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        shell=False,
        text=True,
    )
    out, _ = proc.communicate()
    end: datetime.datetime = datetime.datetime.now()
    duration: float = (end - start).total_seconds()
    return proc.returncode, out, duration

def write_log(base_dir: Path, name: str, header: str, content: str) -> Path:
    base_dir.mkdir(parents=True, exist_ok=True)
    path = base_dir / f"{name}.log"
    with path.open("w", encoding="utf-8") as f:
        f.write(header)
        if not header.endswith("\n"):
            f.write("\n")
        f.write(content)
    return path

# Parse test names using `-- --list` output

def list_tests_for_target(args: Sequence[str]) -> List[str]:
    # args are cargo args like ["--lib", "--bins"] or ["--test", "integration_tests"]
    cmd: List[str] = ["cargo", "test", "-q"] + list(args) + ["--", "--list"]
    code, out, _ = run(cmd)
    if code != 0:
        return []
    names: List[str] = []
    for line in out.splitlines():
        line = line.strip()
        # Formats observed:
        #  - name: test
        #  - module::name: test
        #  - some benches may appear; keep only ": test"
        if line.endswith(": test"):
            name = line[:-len(": test")]
            names.append(name)
    return names

def run_each_test(args: Sequence[str], names: Sequence[str]) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    for name in names:
        # --exact to avoid substring matches; --nocapture to get output if needed
        cmd_parts: List[str] = ["cargo", "test", "-q"]
        cmd_parts += list(args)
        cmd_parts += [name, "--", "--exact", "--nocapture"]
        code, out, dur = run(cmd_parts)
        results.append({"name": name, "code": code, "duration": dur, "output": out})
    return results


def format_results(title: str, results: Sequence[Dict[str, Any]], extra_header: Optional[str] = None) -> str:
    lines: List[str] = []
    if extra_header:
        lines.append(extra_header)
    lines.append(f"{title} - total tests: {len(results)}")
    passed = sum(1 for r in results if r["code"] == 0)
    failed = len(results) - passed
    total_time = sum(r["duration"] for r in results)
    lines.append(f"Summary: passed={passed} failed={failed} duration={total_time:.2f}s")
    lines.append("")
    lines.append("Per-test results:")
    for r in results:
        status = "ok" if r["code"] == 0 else "FAIL"
        lines.append(f"- {r['name']}: {status} ({r['duration']:.3f}s)")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    stamp = ts()
    out_dir = LOG_ROOT / stamp

    # Unit tests (lib + bins)
    unit_names = list_tests_for_target(["--lib", "--bins"])
    unit_results = run_each_test(["--lib", "--bins"], unit_names) if unit_names else []
    unit_header = f"Command: cargo test -q --lib --bins (individually)\nStarted: {stamp}"
    write_log(out_dir, "unit_tests", unit_header, format_results("Unit Tests", unit_results))

    # Integration tests target
    int_names = list_tests_for_target(["--test", "integration_tests"])
    int_results = run_each_test(["--test", "integration_tests"], int_names) if int_names else []
    write_log(out_dir, "integration_tests", f"Command: cargo test -q --test integration_tests (individually)\nStarted: {stamp}", format_results("Integration Tests", int_results))

    # Property tests target
    prop_names = list_tests_for_target(["--test", "prop_tests"])
    prop_results = run_each_test(["--test", "prop_tests"], prop_names) if prop_names else []
    write_log(out_dir, "property_tests", f"Command: cargo test -q --test prop_tests (individually)\nStarted: {stamp}", format_results("Property Tests", prop_results))

    # Clippy
    clippy_cmd = [
        "cargo", "clippy", "--all-targets", "--",
        "-W", "clippy::pedantic", "-W", "clippy::nursery", "-W", "clippy::perf",
        "-W", "clippy::correctness", "-W", "clippy::suspicious", "-W", "clippy::style",
        "-W", "clippy::complexity", "-W", "clippy::restriction"
    ]
    clippy_code, clippy_out, clippy_dur = run(clippy_cmd)
    clippy_header = f"Command: {' '.join(clippy_cmd)}\nDuration: {clippy_dur:.2f}s\nResult: {'ok' if clippy_code==0 else 'FAIL'}"
    write_log(out_dir, "clippy", clippy_header, clippy_out)

    # Cargo fmt check
    fmt_cmd = ["cargo", "fmt", "--all", "--", "--check"]
    fmt_code, fmt_out, fmt_dur = run(fmt_cmd)
    fmt_header = f"Command: {' '.join(fmt_cmd)}\nDuration: {fmt_dur:.2f}s\nResult: {'ok' if fmt_code==0 else 'FAIL'}"
    write_log(out_dir, "fmt", fmt_header, fmt_out)

    print(f"Logs written to: {out_dir}")

if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except KeyboardInterrupt:
        sys.exit(130)
