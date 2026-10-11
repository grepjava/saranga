"""The web suite: the REST API, browsing, metrics and the Jolokia layer against
Saranga, and the Jolokia layer against Artemis
2.57.0 where Artemis can answer.

    python -I tests/interop/web/web.py <saranga web URL> <saranga core URL> [<artemis web URL>]

The users are admin/secret (role amq) and viewer/viewer (role view) on
Saranga, artemis/artemis on Artemis. Artemis 2.57.0's console fails every
MBean read and invocation on JDK 25 (docs/upstream/artemis-issues.md,
issue 8), so the responses compared with it are those that need no MBean
call: version, search, list, and the errors found before the call.
"""

import base64
import json
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request

SARANGA, CORE = sys.argv[1], sys.argv[2]
ARTEMIS = sys.argv[3] if len(sys.argv) > 3 else None
SARANGA_AUTH = ("admin", "secret")
ARTEMIS_AUTH = ("artemis", "artemis")
BROKER = 'org.apache.activemq.artemis:broker="0.0.0.0"'
FAILED = []


def request(base, path, auth, body=None, method=None, headers=None):
    """(status, headers, text) of a request to the Jolokia agent."""
    url = base + "/console/jolokia" + path
    data = body.encode() if isinstance(body, str) else body
    req = urllib.request.Request(url, data=data, method=method)
    if auth:
        token = base64.b64encode(f"{auth[0]}:{auth[1]}".encode()).decode()
        req.add_header("Authorization", "Basic " + token)
    req.add_header("Origin", "http://localhost")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, dict(r.headers), r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read().decode()


def q(s):
    return urllib.parse.quote(s, safe="/,=:*!")


def j(base, path, auth, body=None):
    status, _, text = request(base, path, auth, body)
    if status != 200:
        return {"http": status, "text": text}
    return json.loads(text)


def check(name, ok, detail=""):
    if ok:
        print("PASS", name)
    else:
        print("FAIL", name, detail)
        FAILED.append(name)


def signatures(text):
    """Overloads are listed in Class.getMethods order, which differs from
    one JVM run to the next: sort the signatures an error lists."""
    def sort(m):
        sigs = m.group(2).split("),(")
        return m.group(1) + "),(".join(sorted(s.strip("()") for s in sigs)).join("()")
    import re
    return re.sub(r"((?:Signatures found|Known signatures): )((?:\([^()]*\),?)+)", sort, text)


def normalize(v):
    """Agent IDs, URLs and RBAC answers differ; lists of names, and of
    overloads, are sets."""
    if isinstance(v, dict):
        out = {}
        for k, x in v.items():
            if k in ("canInvoke", "canRead", "canWrite", "ctor"):
                continue
            if k in ("agent_id", "id", "agentId", "url"):
                x = "#"
            out[k] = normalize(x)
        return out
    if isinstance(v, list):
        items = [normalize(x) for x in v]
        return sorted(items, key=lambda x: json.dumps(x, sort_keys=True))
    if isinstance(v, str):
        return signatures(v)
    return v


# ------------------------------------------------------------ against Artemis

COMPARED = [
    ("version", "/version"),
    ("root", ""),
    ("search all", "/search/" + q("org.apache.activemq.artemis:*")),
    ("search addresses", "/search/" + q("org.apache.activemq.artemis:broker=*,component=addresses,address=*")),
    ("search queues", "/search/" + q("org.apache.activemq.artemis:subcomponent=queues,*")),
    ("search none", "/search/" + q("foo:*")),
    ("read unknown MBean", "/read/" + q('org.apache.activemq.artemis:broker="nope"') + "/Version"),
    ("read unknown attribute", "/read/" + q(BROKER) + "/Nope"),
    ("write unknown attribute", "/write/" + q(BROKER) + "/Nope/x"),
    ("exec overloaded", "/exec/" + q(BROKER) + "/createQueue/a/b"),
    ("exec unknown operation", "/exec/" + q(BROKER) + "/nope/a"),
    ("exec unknown signature", "/exec/" + q(BROKER) + "/deleteAddress(int)/a"),
    ("exec bad number", "/exec/" + q('org.apache.activemq.artemis:broker="0.0.0.0",component=addresses,address="DLQ",subcomponent=queues,routing-type="anycast",queue="DLQ"') + "/removeMessages(int,java.lang.String)/x/y"),
    ("read without arguments", "/read"),
    ("bad name", "/read/nocolon/Version"),
    ("list acceptor", "/list/org.apache.activemq.artemis/" + q('broker="0.0.0.0",component=acceptors,name="amqp"')),
    ("list broker", "/list/org.apache.activemq.artemis/" + q('broker="0.0.0.0"')),
    ("list queue", "/list/org.apache.activemq.artemis/" + q('address="DLQ",broker="0.0.0.0",component=addresses,queue="DLQ",routing-type="anycast",subcomponent=queues')),
    ("list address", "/list/org.apache.activemq.artemis/" + q('address="DLQ",broker="0.0.0.0",component=addresses')),
    ("list too deep", "/list/org.apache.activemq.artemis/" + q('broker="0.0.0.0"') + "/attr/Version"),
    ("unknown type", "/nope/x"),
]

POSTED = [
    ("post overloaded", '{"type":"exec","mbean":"org.apache.activemq.artemis:broker=\\"0.0.0.0\\"","operation":"deleteAddress","arguments":["x"]}'),
    ("post bulk", '[{"type":"search","mbean":"org.apache.activemq.artemis:component=acceptors,*"},{"type":"version"}]'),
    ("post junk", "xx"),
]

if ARTEMIS:
    for name, path in COMPARED:
        mine = normalize(j(SARANGA, path, SARANGA_AUTH))
        theirs = normalize(j(ARTEMIS, path, ARTEMIS_AUTH))
        check("artemis: " + name, mine == theirs,
              f"\n  saranga {json.dumps(mine)[:1500]}\n  artemis {json.dumps(theirs)[:1500]}")
    for name, body in POSTED:
        mine = normalize(j(SARANGA, "", SARANGA_AUTH, body))
        theirs = normalize(j(ARTEMIS, "", ARTEMIS_AUTH, body))
        check("artemis: " + name, mine == theirs,
              f"\n  saranga {json.dumps(mine)[:800]}\n  artemis {json.dumps(theirs)[:800]}")
    for name, auth in [("no credentials", None), ("wrong password", ("admin", "wrong"))]:
        s = request(SARANGA, "/version", auth)
        a = request(ARTEMIS, "/version", auth and ("artemis", "wrong"))
        reason = lambda h: {k.lower(): v for k, v in h.items()}.get("hawtio-forbidden-reason")
        check("artemis: " + name, (s[0], reason(s[1]), s[2]) == (a[0], reason(a[1]), a[2]),
              f"saranga {s[0]} {s[1]} artemis {a[0]} {a[1]}")

# ------------------------------------------------------------ Saranga alone


def value(path, auth=SARANGA_AUTH, body=None):
    r = j(SARANGA, path, auth, body)
    if r.get("status") != 200:
        raise AssertionError(json.dumps(r)[:600])
    return r["value"]


def run(name, f):
    try:
        f()
        check(name, True)
    except Exception as e:  # noqa: BLE001
        check(name, False, repr(e))


QUEUE = 'org.apache.activemq.artemis:broker="0.0.0.0",component=addresses,address="web.a",subcomponent=queues,routing-type="anycast",queue="web.q"'
ADDRESS = 'org.apache.activemq.artemis:broker="0.0.0.0",component=addresses,address="web.a"'


def t_read():
    assert value("/read/" + q(BROKER) + "/Version") == "2.57.0"
    assert value("/read/" + q(BROKER) + "/Name") == "0.0.0.0"
    v = value("/read/" + q(BROKER) + "/Version,Started")
    assert v == {"Version": "2.57.0", "Started": True}, v
    v = value("/read/" + q(BROKER))
    assert v["Version"] == "2.57.0" and v["Started"] is True and "ConnectionCount" in v, list(v)[:10]


def t_exec_and_search():
    value("/exec/" + q(BROKER) + "/createQueue(java.lang.String,java.lang.String,java.lang.String)/web.a/web.q/ANYCAST")
    names = value("/search/" + q("org.apache.activemq.artemis:queue=\"web.q\",*"))
    canonical = 'org.apache.activemq.artemis:address="web.a",broker="0.0.0.0",component=addresses,queue="web.q",routing-type="anycast",subcomponent=queues'
    assert names == [canonical], names
    assert value("/read/" + q(QUEUE) + "/MessageCount") == 0
    assert value("/read/" + q(ADDRESS) + "/QueueNames") == ["web.q"]


def t_send_and_count():
    body = json.dumps({"type": "exec", "mbean": QUEUE,
                       "operation": "sendMessage(java.util.Map,int,java.lang.String,boolean,java.lang.String,java.lang.String)",
                       "arguments": [{"color": "red"}, 3, "hello", True, "admin", "secret"]})
    value("", body=body)
    assert value("/read/" + q(QUEUE) + "/MessageCount") == 1
    assert value("/exec/" + q(QUEUE) + "/countMessages(java.lang.String)/color='red'") == 1
    assert value("/exec/" + q(QUEUE) + "/countMessages(java.lang.String)/color='blue'") == 0
    msgs = value("/exec/" + q(QUEUE) + "/listMessages(java.lang.String)/[null]")
    assert len(msgs) == 1 and msgs[0].get("color") == "red", msgs


def t_pattern_read():
    v = value("/read/" + q("org.apache.activemq.artemis:broker=*,component=addresses,address=*,subcomponent=queues,routing-type=*,queue=*") + "/MessageCount")
    key = 'org.apache.activemq.artemis:address="web.a",broker="0.0.0.0",component=addresses,queue="web.q",routing-type="anycast",subcomponent=queues'
    assert v[key] == {"MessageCount": 1}, v


def t_inner_path():
    v = value("/exec/" + q(QUEUE) + "/listMessages(java.lang.String)/[null]/0/color")
    assert v == "red", v


def t_write():
    old = value("/write/" + q(BROKER) + "/MessageCounterSamplePeriod/5000")
    assert old == 10000, old
    assert value("/read/" + q(BROKER) + "/MessageCounterSamplePeriod") == 5000
    value("/write/" + q(BROKER) + "/MessageCounterSamplePeriod/10000")


def t_operation_error():
    r = j(SARANGA, "/exec/" + q(BROKER) + "/deleteAddress(java.lang.String)/nosuch", SARANGA_AUTH)
    assert r["status"] == 500 and "AMQ229203" in r["error"], r


def t_viewer_reads_queues_only():
    viewer = ("viewer", "viewer")
    # management.xml: queues are readable by view; everything else needs amq.
    assert value("/read/" + q(QUEUE) + "/MessageCount", viewer) == 1
    r = j(SARANGA, "/read/" + q(BROKER) + "/Version", viewer)
    assert r["status"] == 403 and r["error"] == "java.lang.SecurityException : User not authorized to access attribute: Version", r
    r = j(SARANGA, "/exec/" + q(QUEUE) + "/removeAllMessages()", viewer)
    assert r["status"] == 403 and r["error_type"] == "java.lang.SecurityException", r
    info = value("/list/org.apache.activemq.artemis/" + q('address="web.a",broker="0.0.0.0",component=addresses,queue="web.q",routing-type="anycast",subcomponent=queues'), viewer)
    assert info["attr"]["MessageCount"]["canRead"] is True
    assert info["op"]["removeAllMessages"][0]["canInvoke"] is False if isinstance(info["op"]["removeAllMessages"], list) else info["op"]["removeAllMessages"]["canInvoke"] is False


def t_metrics():
    req = urllib.request.Request(SARANGA + "/metrics/")
    with urllib.request.urlopen(req) as r:
        text = r.read().decode()
        ctype = r.headers.get("Content-Type")
    assert ctype.startswith("text/plain; version=0.0.4"), ctype
    assert '# TYPE artemis_message_count gauge' in text, text[:500]
    assert 'artemis_message_count{address="web.a",broker="0.0.0.0",queue="web.q"} 1.0' in text, text
    assert 'artemis_connection_count{broker="0.0.0.0"}' in text
    assert 'artemis_authentication_count{broker="0.0.0.0",result="success"}' in text


def t_browse():
    msgs = value("/exec/" + q(QUEUE) + "/browse()")
    assert len(msgs) == 1, msgs
    m = msgs[0]
    assert list(m) == sorted(m), list(m)
    assert m["text"] == "hello" and m["type"] == 3 and m["protocol"] == "CORE", m
    assert m["StringProperties"] == {"color": "red"} and m["IntProperties"] is None, m
    assert m["PropertiesText"] == "{color=red}" and m["redelivered"] is False, m
    # sendMessage addresses the queue by its FQQN and sets no user ID.
    assert m["durable"] is True and m["address"] == "web.a::web.q" and m["userID"] == "", m
    assert value("/exec/" + q(QUEUE) + "/browse(java.lang.String)/color='blue'") == []
    assert len(value("/exec/" + q(QUEUE) + "/browse(int,int)/1/10")) == 1
    assert value("/exec/" + q(QUEUE) + "/browse(int,int)/2/10") == []


def rest(path, auth=SARANGA_AUTH, body=None):
    """(status, headers, parsed JSON) of a REST API request."""
    data = None if body is None else (body if isinstance(body, str) else json.dumps(body)).encode()
    req = urllib.request.Request(SARANGA + "/api" + path, data=data, method="POST" if data is not None else "GET")
    if auth:
        token = base64.b64encode(f"{auth[0]}:{auth[1]}".encode()).decode()
        req.add_header("Authorization", "Basic " + token)
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, dict(r.headers), json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), json.loads(e.read().decode())


def t_rest():
    assert rest("/broker/attributes/Version")[::2] == (200, "2.57.0")
    assert rest("/queues/web.q/attributes/MessageCount")[::2] == (200, 1)
    assert rest("/addresses/web.a/attributes/QueueNames")[::2] == (200, ["web.q"])
    assert rest("/queues/web.q/operations/countMessages", body=["color='red'"])[::2] == (200, 1)
    status, _, msgs = rest("/queues/web.q/messages")
    assert status == 200 and msgs == value("/exec/" + q(QUEUE) + "/browse()"), msgs
    assert rest("/queues/web.q/messages?filter=" + q("color='blue'"))[::2] == (200, [])
    assert rest("/queues/web.q/messages?page=1&pageSize=1")[2] == msgs
    assert rest("/queues/web.q/messages?page=1")[0] == 400


def t_rest_errors():
    status, headers, body = rest("/broker/attributes/Version", auth=None)
    assert status == 401 and {k.lower(): v for k, v in headers.items()}.get("www-authenticate") == 'Basic realm="saranga"', (status, headers)
    assert rest("/broker/attributes/Version", auth=("admin", "wrong"))[0] == 401
    # viewer has no manage permission on the management address.
    status, _, body = rest("/queues/web.q/attributes/MessageCount", auth=("viewer", "viewer"))
    assert status == 403 and "AMQ229032" in body["error"], body
    assert rest("/queues/nosuch/attributes/MessageCount")[0] == 404
    assert rest("/queues/web.q/attributes/NoSuch")[0] == 404
    assert rest("/broker/operations/noSuchOperation", body=[])[0] == 404
    status, _, body = rest("/broker/operations/deleteAddress", body=["nosuch"])
    assert status == 400 and "AMQ229203" in body["error"], body
    assert rest("/broker/operations/getVersion", body="{")[0] == 400
    assert rest("/nosuch/x/attributes/y")[0] == 404


def t_purge():
    value("/exec/" + q(QUEUE) + "/removeAllMessages()")
    assert value("/read/" + q(QUEUE) + "/MessageCount") == 0
    value("/exec/" + q(BROKER) + "/destroyQueue(java.lang.String)/web.q")


for name, f in [("read", t_read), ("exec and search", t_exec_and_search), ("send and count", t_send_and_count),
                ("pattern read", t_pattern_read), ("inner path", t_inner_path), ("write", t_write),
                ("operation error", t_operation_error), ("viewer", t_viewer_reads_queues_only),
                ("metrics", t_metrics), ("browse", t_browse), ("rest", t_rest),
                ("rest errors", t_rest_errors), ("purge", t_purge)]:
    run(name, f)

if FAILED:
    print(f"{len(FAILED)} FAILED")
    sys.exit(1)
print("ALL PASSED")
