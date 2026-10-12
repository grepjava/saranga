"""Normalizes the perf suite's output (`output`, from stdin) or a --json
report (`json <file>`).

Output: rates, latencies and timings become N and runs of spaces one
space, except in the message totals; each command's lines are sorted and
made unique, as the number of one-second reports varies. The JVM's notice
that it has no microsecond clock is dropped (Saranga has one).

JSON: each sample's members, with the type of each value.
"""

import json
import re
import sys

# A measurement too fast for the clock is infinite: Java prints the sign as
# `?` where the console's code page has no infinity.
# DecimalFormat("###.##") drops a zero integer part: ".5".
NUMBER = re.compile(r"-?(\d+(\.\d+)?|(?<![\d.])\.\d+)(E-?\d+)?|∞|Infinity|(?<![^ ])\?(?![^ ])")
TOTAL = re.compile(r"^--- total (sent|completed|received):")
ANSI = re.compile(r"\x1b\[[0-9;]*m")
# The JVM's notices, and what only Artemis's --verbose prints.
DROPPED = ("Microseconds wall-clock time not available",)
DROPPED_PREFIXES = ("WARNING: ", "Executing org.apache.activemq.artemis.cli.", "Home::")


def line(s, exact_totals):
    s = s.rstrip("\r\n")
    if exact_totals and TOTAL.match(s):
        return re.sub(r" +", " ", s).rstrip()
    return re.sub(r" +", " ", NUMBER.sub("N", s)).rstrip()


def counted(header):
    """A run with a message count, whose totals are exact."""
    m = re.search(r"--message-count (\d+)", header)
    return bool(m) and m.group(1) != "0"


def output():
    sections = []
    for raw in sys.stdin:
        raw = ANSI.sub("", raw.rstrip("\r\n"))
        raw = re.sub(r"^Usage: (artemis|saranga) help", "Usage: CLI help", raw)
        if raw.startswith(DROPPED_PREFIXES):
            continue
        if raw.startswith("=== "):
            sections.append((raw, set()))
            continue
        if any(d in raw for d in DROPPED):
            continue
        if not sections:
            sections.append(("", set()))
        if raw.startswith("--- exit"):
            sections[-1][1].add("~" + raw)
        else:
            sections[-1][1].add(line(raw, counted(sections[-1][0])))
    for header, lines in sections:
        print(header)
        for l in sorted(lines):
            if l:
                print(l[1:] if l.startswith("~") else l)


def shape(v):
    if isinstance(v, dict):
        return "{" + ",".join(f"{k}:{shape(x)}" for k, x in v.items()) + "}"
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, int):
        return "int"
    if isinstance(v, str):
        return "str"
    return type(v).__name__


def report(path):
    with open(path, encoding="utf-8") as f:
        samples = json.load(f)
    seen = []
    for s in samples:
        t = s.get("sampleType")
        if t == "summary":
            totals = {k: v for k, v in s.items() if k in ("totalSent", "totalCompleted", "totalReceived")}
            print("summary totals", json.dumps(totals))
        k = shape(s)
        if k not in seen:
            seen.append(k)
    for k in seen:
        print(k)


if __name__ == "__main__":
    # The same file on every platform.
    sys.stdout.reconfigure(newline="\n")
    sys.stdin.reconfigure(encoding="utf-8", errors="replace")
    if sys.argv[1] == "json":
        report(sys.argv[2])
    else:
        output()
