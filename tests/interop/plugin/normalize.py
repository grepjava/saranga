"""Normalizes LoggingActiveMQServerPlugin's lines (AMQ841xxx) of a broker
log for comparison between brokers.

    python -I normalize.py <artemis.log>

The log starts at the first client connection: what is logged while the
broker starts depends on what its journal already held. Each line is cut
into steps at each "created connection" line; within a step the lines are
sorted, as Artemis logs from several threads. IDs, addresses, hashes and
times are replaced, and so are two things Saranga does not know where it
logs them (COMPAT.md): the transaction an acknowledgement was in, and the
session that destroyed a queue. A message's properties are sorted: Artemis
prints them in its HashMap's order.
"""

import re
import sys

LINE = re.compile(r"(AMQ84\d{4}): (.*)")
UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


def reduce(text):
    text = UUID.sub("UUID", text)
    text = re.sub(r"\b\d+\.\d+\.\d+\.\d+:\d+", "ADDR", text)
    text = re.sub(r"@[0-9a-f]+\b", "@HASH", text)
    text = re.sub(r"\bID=[0-9a-f]{8}\b", "ID=CID", text)
    text = re.sub(r"connectionID: [0-9a-f]{8}\b", "connectionID: CID", text)
    text = re.sub(r"(message ID|with ID): \d+", r"\1: MID", text)
    text = re.sub(r"(messageID=|Reference\[)\d+", r"\1MID", text)
    text = re.sub(r"timestamp=[A-Z][a-z]{2} [A-Z][a-z]{2} \d+ [\d:]+ \w+ \d{4}", "timestamp=TIME", text)
    text = re.sub(r"with transaction: .*", "with transaction: TX", text)
    text = re.sub(r"session: (null|ServerSessionImpl\(.*?\)), checkConsumerCount", "session: SESSION, checkConsumerCount", text)
    # A Java HashMap's order: sorted.
    text = re.sub(r"TypedProperties\[(.*?)\]\]", lambda m: "TypedProperties[" + ", ".join(sorted(m.group(1).split(", "))) + "]]", text)
    return text


def main(path):
    steps = []
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            m = LINE.search(line)
            if not m:
                continue
            code, text = m.groups()
            if code == "AMQ841000":
                steps.append([])
            if not steps:
                continue
            steps[-1].append(f"{code}: {reduce(text.rstrip())}")
    for i, step in enumerate(steps):
        print(f"--- connection {i + 1}")
        for line in sorted(step):
            print(line)


main(sys.argv[1])
