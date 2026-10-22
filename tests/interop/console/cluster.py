"""The console API across a cluster: /nodes, and /nodes/{node}/... and
/nodes/all/... answered by the other nodes, Saranga and Artemis alike.

    python -I cluster.py <web url> <saranga binary> <core url of node 1> <of node 2> <of node 3>

Node 1 serves the console; nodes 2 and 3 are in its cluster topology. Every
node has admin/admin (role amq); only node 1 has visitor/visitor (role amq),
whom the other nodes do not know. Prints a line per check and ends with the
number that failed.
"""

import base64
import json
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

WEB = sys.argv[1].rstrip("/")
SARANGA = sys.argv[2]
CORE = sys.argv[3:6]
API = WEB + "/saranga/api/v1"
MARK = {"X-Saranga-Console": "1"}
failures = 0


def check(name, ok, detail=""):
    global failures
    if not ok:
        failures += 1
    print(f"{'ok  ' if ok else 'FAIL'} {name}{'' if ok else ': ' + str(detail)}")


def request(method, path, body=None, headers=None, user="admin:admin"):
    data = None if body is None else json.dumps(body).encode()
    h = dict(headers or {})
    if data is not None:
        h["Content-Type"] = "application/json"
    h["Authorization"] = "Basic " + base64.b64encode(user.encode()).decode()
    req = urllib.request.Request(API + path, data=data, method=method, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=40) as r:
            status, raw = r.status, r.read()
    except urllib.error.HTTPError as e:
        status, raw = e.code, e.read()
    try:
        return status, json.loads(raw) if raw else None
    except ValueError:
        return status, raw


def invoke(node, resource, operation, args=(), user="admin:admin"):
    q = urllib.parse.quote(resource, safe="")
    return request(
        "POST",
        f"/nodes/{node}/resources/{q}/operations",
        {"operation": operation, "arguments": list(args)},
        MARK,
        user,
    )


def until(path, ready, timeout=15):
    end = time.time() + timeout
    while True:
        status, body = request("GET", path)
        if (status == 200 and ready(body)) or time.time() > end:
            return status, body
        time.sleep(0.2)


def produce(url, queue, count):
    subprocess.run(
        [SARANGA, "producer", "--destination", f"queue://{queue}", "--url", url,
         "--user", "admin", "--password", "admin", "--message-count", str(count), "--silent"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=120)


# The nodes: this broker first, then the two others with their connectors.
status, nodes = until("/nodes", lambda b: len(b["data"]) == 3)
check("GET /nodes lists the three nodes", status == 200 and len(nodes["data"]) == 3, nodes)
local = nodes["local"]
data = nodes["data"]
check("this broker comes first, as local", data[0]["nodeID"] == local and data[0]["local"], data[0])
others = [n for n in data if not n["local"]]
check("the others have their connectors",
      all(n["primary"] and n["clusters"] for n in others), others)
ids = {}
for n in others:
    status, b = request("GET", f"/nodes/{n['nodeID']}/broker")
    ids[n["nodeID"]] = b["attributes"].get("Name") if status == 200 else None
by_name = {name: node for node, name in ids.items()}
check("each other node answers GET /broker with its own name and node ID",
      set(by_name) == {"node2", "node3"}, ids)
N2, N3 = by_name.get("node2"), by_name.get("node3")
status, b = request("GET", f"/nodes/{N3}/broker")
check("node 3's status has its node ID", status == 200 and b["attributes"].get("NodeID") == N3, b)
check("node 3's status has its version",
      status == 200 and b["attributes"].get("Version") == "2.57.0", b)
status, b = request("GET", "/nodes/local/broker")
check("/nodes/local/... is this broker", status == 200 and b["attributes"].get("NodeID") == local, b)
status, b = request("GET", f"/nodes/{local}/broker")
check("so is /nodes/<its node ID>/...", status == 200 and b["attributes"].get("NodeID") == local, b)

# What another node allows is that node's to say.
status, me = request("GET", f"/nodes/{N2}/me")
check("GET /me on another node: its guard decides, so every action is null",
      status == 200 and me["guard"] == "node" and set(me["broker"].values()) == {None}, me)
status, can = request("POST", f"/nodes/{N2}/me/can",
                      {"checks": [{"resource": "broker", "method": "createQueue"}]})
check("POST /me/can on another node answers null", status == 200 and can["results"] == [None], can)

# Operations and listings on the other nodes, as management messages.
for node, name in ((N2, "only.node2"), (N3, "only.node3")):
    status, r = invoke(node, "broker", "createQueue(java.lang.String,java.lang.String,java.lang.String)",
                       [name, name, "ANYCAST"])
    check(f"createQueue on {ids[node]} through the console", status == 200, r)
produce(CORE[1], "only.node2", 3)
produce(CORE[2], "only.node3", 5)
q = "?filter=" + urllib.parse.quote("name:CONTAINS:only.node")
status, page = until(f"/nodes/{N3}/queues{q}",
                     lambda p: [r["messageCount"] for r in p["data"]] == ["5"])
check("node 3's queues, filtered", status == 200 and [r["name"] for r in page["data"]] == ["only.node3"], page)
check("with node 3's message count", status == 200 and page["data"][0]["messageCount"] == "5", page)
status, page = until(f"/nodes/{N2}/queues{q}",
                     lambda p: [r["messageCount"] for r in p["data"]] == ["3"])
check("node 2's queues, filtered", status == 200 and [r["name"] for r in page["data"]] == ["only.node2"], page)

status, page = until(f"/nodes/all/queues{q}&sort=messageCount&order=desc",
                     lambda p: len(p["data"]) == 2)
check("/nodes/all/queues spans the nodes", status == 200 and page["count"] == 2 and page["errors"] == [], page)
check("each row names its node, sorted across them",
      status == 200 and [(r["name"], r["node"]) for r in page["data"]]
      == [("only.node3", N3), ("only.node2", N2)], page)
status, page = request("GET", f"/nodes/all/queues{q}&size=1&page=2&sort=messageCount&order=desc")
check("and paged across them", status == 200 and [r["name"] for r in page["data"]] == ["only.node2"], page)
status, b = request("GET", "/nodes/all/broker")
check("only the listings span every node", status == 400, b)

# The resources of another node.
status, rs = request("GET", f"/nodes/{N3}/resources")
names = [r["resource"] for r in rs["data"]] if status == 200 else []
check("node 3's resources", status == 200 and "broker" in names and "queue.only.node3" in names, names[:20])
status, r = request("GET", f"/nodes/{N3}/resources/queue.only.node3")
attrs = {a["name"]: a for a in r["attributes"]} if status == 200 else {}
check("node 3's queue, with its attributes' values",
      status == 200 and attrs.get("MessageCount", {}).get("value") == 5, attrs.get("MessageCount"))
check("and its operations, which node 3 allows or not when called",
      status == 200 and all(o["allowed"] is None for o in r["operations"]), status)
status, r = request("GET", f"/nodes/{N2}/resources/queue.only.node2/attributes?name=MessageCount&name=Paused")
check("some attributes of node 2's queue",
      status == 200 and r["attributes"]["MessageCount"].get("value") == 3
      and r["attributes"]["Paused"].get("value") is False, r)
for node, name, count in ((N2, "only.node2", 3), (N3, "only.node3", 5)):
    status, r = invoke(node, f"queue.{name}", "browse(int,int,java.lang.String)", [1, 10, ""])
    check(f"browsing {ids[node]}'s queue, as its page does",
          status == 200 and isinstance(r.get("result"), list) and len(r["result"]) == count, (status, r))
    first = (r.get("result") or [{}])[0] if status == 200 else {}
    check(f"with each message's body and properties",
          bool(first.get("text")) and first.get("address") == name and "PropertiesText" in first, first)
    status, r = invoke(node, f"queue.{name}", "countMessages()")
    check(f"counting {ids[node]}'s queue", status == 200 and r.get("result") == count, (status, r))
    status, r = invoke(node, f"queue.{name}", "countMessages(java.lang.String)", ["AMQPriority >= 0"])
    check(f"counting {ids[node]}'s queue with a filter", status == 200 and r.get("result") == count, (status, r))
status, r = invoke(N3, "queue.only.node3", "removeAllMessages()")
check("an operation on node 3's queue", status == 200 and r["result"] == 5, r)
status, r = request("GET", f"/nodes/{N3}/resources/queue.only.node3/attributes?name=MessageCount")
check("which node 3 did", status == 200 and r["attributes"]["MessageCount"].get("value") == 0, r)
status, r = request("PUT", f"/nodes/{N2}/resources/broker/attributes/MessageCounterSamplePeriod",
                    {"value": 20000}, MARK)
check("an attribute written on node 2, by its setter", status == 200, r)
status, r = request("GET", f"/nodes/{N2}/resources/broker/attributes?name=MessageCounterSamplePeriod")
check("which node 2 took", status == 200 and r["attributes"]["MessageCounterSamplePeriod"].get("value") == 20000, r)
status, r = invoke(N3, "queue.nothing.here", "removeAllMessages()")
check("an operation on a resource node 3 does not have fails with node 3's error",
      status in (400, 404) and "error" in r, (status, r))
status, r = request("GET", f"/nodes/{N2}/resources/nonsense")
check("a resource name that is no resource", status == 404, r)

# The nodes check the user themselves.
status, r = request("GET", f"/nodes/{N2}/broker", user="visitor:visitor")
check("a user node 2 does not know is refused there", status == 403, (status, r))
status, r = request("GET", "/nodes/local/broker", user="visitor:visitor")
check("and allowed here", status == 200, status)
status, r = request("GET", "/nodes/0123456789abcdef/broker")
check("a node the topology does not have", status == 404, r)
status, r = request("GET", f"/nodes/{N2}/broker", user="admin:wrong")
check("without logging in here, nothing is asked of the others", status == 401, status)

# A close on another node.
consumer = subprocess.Popen(
    [SARANGA, "consumer", "--destination", "queue://only.node2", "--url", CORE[1],
     "--user", "admin", "--password", "admin", "--message-count", "100",
     "--receive-timeout", "60000", "--sleep", "1000"],
    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
status, page = until(f"/nodes/{N2}/consumers?filter=" + urllib.parse.quote("queue:EQUALS:only.node2"),
                     lambda p: len(p["data"]) == 1)
check("node 2's consumer", status == 200 and len(page["data"]) == 1, page)
if status == 200 and page["data"]:
    c = page["data"][0]
    path = "/".join(urllib.parse.quote(x, safe="") for x in (c["session"], c["id"]))
    status, r = request("DELETE", f"/nodes/{N2}/consumers/{path}", None, MARK)
    check("closing it on node 2", status == 204, (status, r))
    status, page = until(f"/nodes/{N2}/consumers?filter=" + urllib.parse.quote("queue:EQUALS:only.node2"),
                         lambda p: len(p["data"]) == 0)
    check("which node 2 closed", status == 200 and page["data"] == [], page)
consumer.kill()

# The live stream reads views on the other nodes.
h = {"Authorization": "Basic " + base64.b64encode(b"admin:admin").decode()}
r = urllib.request.urlopen(urllib.request.Request(API + "/events", headers=h), timeout=30)


def event(kind):
    while True:
        name, data = None, []
        while True:
            line = r.readline().decode()
            if line == "":
                return None
            line = line.rstrip("\r\n")
            if line == "":
                break
            k, _, v = line.partition(":")
            v = v[1:] if v.startswith(" ") else v
            if k == "event":
                name = v
            elif k == "data":
                data.append(v)
        if name == kind:
            return json.loads("\n".join(data))


hello = event("hello")
status, w = request("PUT", f"/events/{hello['stream']}", {"views": {
    "here": {"kind": "broker"},
    "three": {"kind": "attributes", "resource": "broker", "names": ["NodeID"], "node": N3},
    "all": {"kind": "listing", "listing": "queues", "node": "all",
            "query": "filter=" + urllib.parse.quote("name:CONTAINS:only.node")},
    "gone": {"kind": "broker", "node": "0123456789abcdef"},
}}, MARK)
check("a stream may watch views of other nodes", status == 200 and w["views"] == 4, w)
views = {}
for _ in range(5):
    s = event("stats")
    views.update(s["views"] if s else {})
    if len(views) == 4:
        break
check("a view of node 3", views.get("three", {}).get("value", {}).get("NodeID", {}).get("value") == N3,
      views.get("three"))
check("a listing across every node",
      sorted(row["node"] for row in views.get("all", {}).get("value", {}).get("data", [])) == sorted([N2, N3]),
      views.get("all"))
check("a node that is not there is the view's error, and the others go on",
      views.get("gone", {}).get("status") == 404 and "value" in views.get("here", {}), views.get("gone"))
status, w = request("PUT", f"/events/{hello['stream']}",
                    {"views": {"x": {"kind": "broker", "node": "all"}}}, MARK)
check("only a listing view spans every node", status == 400, w)
r.close()

print(f"{failures} failed")
sys.exit(1 if failures else 0)
