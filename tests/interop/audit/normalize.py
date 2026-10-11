"""Normalizes an audit.log for comparison between brokers.

    python -I normalize.py <audit.log>

Each line is reduced to its code, its caller and what it did, with
addresses, connection IDs and UUIDs replaced. The log is cut into steps at
each "connection ... created" line; within a step the lines are sorted, as
Artemis logs from several threads. What only Artemis logs is dropped:

- 601019, 601138, 601718: MBean registration and the JMX guard;
- queue getters on QueueImpl sources: Artemis's list views read each
  queue through its QueueControl, which audits every getter;
- acknowledgements by system@internal: messages left on a deleted queue.
"""

import re
import sys

IGNORED = {"601019", "601138", "601718"}
LINE = re.compile(r"AMQ(60\d{4}): (.*)")
UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


def addresses(text):
    text = re.sub(r"@\d+\.\d+\.\d+\.\d+:\d+", "@ADDR", text)
    text = re.sub(r"from \d+\.\d+\.\d+\.\d+:\d+", "from ADDR", text)
    text = re.sub(r"connection [0-9a-f]{8}\b", "connection ID", text)
    return UUID.sub("UUID", text)


def reduce(code, text):
    """What the line says, without what differs between brokers."""
    text = addresses(text)
    m = re.match(r"User (\S+) sent a message .*?address=([^,]*),", text)
    if code == "601500" and m:
        return f"User {m.group(1)} sent a message to {m.group(2)}"
    for verb in (
        "is consuming a message from",
        "acknowledged message from",
        "added acknowledgement of a message from",
    ):
        m = re.match(rf"User (\S+) {verb} ([^:]*):", text)
        if m:
            return f"User {m.group(1)} {verb} {m.group(2)}"
    m = re.match(r"(.*?) on target resource:? (.*)", text)
    if not m:
        return text
    head, rest = m.groups()
    source, _, params = rest.partition(" with parameters")
    # The source: its class, and the queue's name.
    q = re.match(r"(QueueImpl\[name=[^,]*)", source)
    if q:
        source = q.group(1) + "]"
    elif source.startswith("ServerSessionImpl("):
        source = "ServerSessionImpl"
    # The parameters of a management call; a session's are its objects'.
    if params and source.startswith("ActiveMQServerImpl") and code != "601267":
        return f"{head} on {source} with parameters{params}"
    return f"{head} on {source}"


def main():
    steps = []
    # A connection's step, by its ID: its "destroyed" line can come after
    # the next connection's "created".
    step_of = {}
    with open(sys.argv[1], encoding="utf-8") as f:
        for line in f:
            m = LINE.search(line)
            if not m:
                continue  # a multi-line message's continuation
            code, text = m.groups()
            conn = re.search(r"connection ([0-9a-f]{8})\b", text)
            if code == "601767":
                steps.append([])
                if conn:
                    step_of[conn.group(1)] = len(steps) - 1
            if not steps or code in IGNORED or "User system@internal" in text:
                continue
            if "on target resource: QueueImpl[" in text and re.search(r" is (getting|querying) ", text):
                continue
            step = step_of.get(conn.group(1), len(steps) - 1) if code == "601768" and conn else len(steps) - 1
            steps[step].append(f"AMQ{code}: {reduce(code, text)}")
    for i, step in enumerate(steps):
        print(f"== step {i + 1}")
        for line in sorted(step):
            print(line)


if __name__ == "__main__":
    main()
