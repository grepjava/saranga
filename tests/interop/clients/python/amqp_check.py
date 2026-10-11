"""Phase 7 client matrix: AMQP 1.0 with Qpid Proton's Python binding
(python-qpid-proton 0.40.0).

    python amqp_check.py host:port [test-name...]

Run through scripts/client-matrix.sh. The same checks run in the .NET
(AMQPNetLite) and Rust (fe2o3-amqp) programs beside this one.
"""

import sys
import time
import uuid

from proton import Message
from proton.utils import BlockingConnection

ADDRESS = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:61616"
RUN = uuid.uuid4().hex[:8]


def check(cond, what):
    if not cond:
        raise AssertionError(what)


def eq(want, got, what):
    if want != got:
        raise AssertionError(f"{what}: want <{want!r}> got <{got!r}>")


def queue(name):
    return f"clients.python.{name}.{RUN}"


def connect():
    return BlockingConnection(f"amqp://{ADDRESS}", timeout=10)


def t_round_trip():
    """Bodies, application properties and header fields come back."""
    c = connect()
    try:
        q = queue("roundtrip")
        r = c.create_receiver(q, credit=10)
        s = c.create_sender(q)
        for i in range(3):
            s.send(Message(body=f"hello {i}", properties={"n": i, "who": "python"},
                           subject="greeting", correlation_id=f"corr-{i}", priority=7, durable=True))
        for i in range(3):
            m = r.receive(timeout=5)
            eq(f"hello {i}", m.body, "body")
            eq(i, m.properties["n"], "property n")
            eq("python", m.properties["who"], "property who")
            eq("greeting", m.subject, "subject")
            eq(f"corr-{i}", m.correlation_id, "correlation id")
            eq(7, m.priority, "priority")
            check(m.durable, "durable")
            r.accept()
    finally:
        c.close()


def t_release_redelivers():
    """A released message comes back. Proton releases a delivered message
    with Modified, without delivery-failed, so it is not counted again."""
    c = connect()
    try:
        q = queue("release")
        r = c.create_receiver(q, credit=1)
        c.create_sender(q).send(Message(body="again"))
        m = r.receive(timeout=5)
        eq(0, m.delivery_count, "first delivery count")
        r.release(delivered=True)
        m = r.receive(timeout=5)
        eq("again", m.body, "redelivered body")
        eq(0, m.delivery_count, "delivery count after a release")
        r.accept()
    finally:
        c.close()


def t_large_body():
    """A body far over a frame crosses whole."""
    c = connect()
    try:
        q = queue("large")
        r = c.create_receiver(q, credit=1)
        body = bytes((i * 7) % 251 for i in range(600_000))
        c.create_sender(q).send(Message(body=body))
        m = r.receive(timeout=10)
        eq(len(body), len(m.body), "length")
        check(bytes(m.body) == body, "body differs")
        r.accept()
    finally:
        c.close()


def t_map_body():
    """An amqp-value map keeps its entries and their types."""
    c = connect()
    try:
        q = queue("map")
        r = c.create_receiver(q, credit=1)
        body = {"text": "t", "number": 42, "flag": True, "float": 1.5}
        c.create_sender(q).send(Message(body=body))
        m = r.receive(timeout=5)
        eq(body, dict(m.body), "map")
        r.accept()
    finally:
        c.close()


def t_rejected_is_gone():
    """A rejected message is not delivered again."""
    c = connect()
    try:
        q = queue("reject")
        r = c.create_receiver(q, credit=2)
        s = c.create_sender(q)
        s.send(Message(body="bad"))
        s.send(Message(body="good"))
        eq("bad", r.receive(timeout=5).body, "first")
        r.reject()
        eq("good", r.receive(timeout=5).body, "second")
        r.accept()
        try:
            m = r.receive(timeout=1)
            raise AssertionError(f"delivered again: {m.body}")
        except Exception as e:
            if isinstance(e, AssertionError):
                raise
    finally:
        c.close()


def main():
    only = set(sys.argv[2:])
    tests = {n[2:]: f for n, f in sorted(globals().items()) if n.startswith("t_")}
    failed = 0
    for name, f in tests.items():
        if only and name not in only:
            continue
        start = time.monotonic()
        try:
            f()
            print(f"PASS {'python.' + name:<40} {int((time.monotonic() - start) * 1000):5d} ms", flush=True)
        except Exception as e:
            failed += 1
            print(f"FAIL {'python.' + name:<40} {type(e).__name__}: {e}", flush=True)
    print("ALL PASSED" if failed == 0 else f"{failed} FAILED")
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
