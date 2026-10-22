"""The console API suite: every endpoint of /saranga/api/v1, as each role.

    python -I api.py <web url> <stomp host:port>

The instance (see api.sh) has the users admin/secret (role amq, may do
anything), viewer/viewer (role view, may only read) and outsider/outsider
(role other, which is not a console role), and the suite's management.xml.
A STOMP client gives the listings a connection, a session, a consumer and a
producer. Prints a line per check and ends with the number that failed.
"""

import base64
import gzip
import hashlib
import json
import os
import re
import socket
import sys
import time
import urllib.error
import urllib.request

WEB = sys.argv[1].rstrip("/")
STOMP_HOST, STOMP_PORT = sys.argv[2].rsplit(":", 1)
API = WEB + "/saranga/api/v1"
MARK = {"X-Saranga-Console": "1"}
failures = 0


def check(name, ok, detail=""):
    global failures
    if not ok:
        failures += 1
    print(f"{'ok  ' if ok else 'FAIL'} {name}{'' if ok else ': ' + str(detail)}")


class Client:
    """A browser: keeps the cookies the API sets."""

    def __init__(self):
        self.cookies = {}
        self.last_set_cookie = None

    def request(self, method, path, body=None, headers=None, basic=None):
        url = path if path.startswith("http") else API + path
        data = None if body is None else json.dumps(body).encode()
        h = dict(headers or {})
        if data is not None:
            h["Content-Type"] = "application/json"
        if self.cookies:
            h["Cookie"] = "; ".join(f"{k}={v}" for k, v in self.cookies.items())
        if basic:
            h["Authorization"] = "Basic " + base64.b64encode(basic.encode()).decode()
        req = urllib.request.Request(url, data=data, method=method, headers=h)
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                status, raw, got = r.status, r.read(), r.headers
        except urllib.error.HTTPError as e:
            status, raw, got = e.code, e.read(), e.headers
        for c in got.get_all("Set-Cookie") or []:
            self.last_set_cookie = c
            k, v = c.split(";", 1)[0].split("=", 1)
            if "Max-Age=0" in c:
                self.cookies.pop(k, None)
            else:
                self.cookies[k] = v
        try:
            payload = json.loads(raw) if raw else None
        except ValueError:
            payload = raw
        return status, payload, got

    def login(self, user, password):
        return self.request("POST", "/session", {"user": user, "password": password}, MARK)


class Stomp:
    """A STOMP 1.2 client, enough for a consumer and a producer."""

    def __init__(self, user, password):
        self.s = socket.create_connection((STOMP_HOST, int(STOMP_PORT)), timeout=10)
        self.frame("CONNECT", {"accept-version": "1.2", "host": "/", "login": user, "passcode": password})
        self.read()

    def frame(self, command, headers, body=""):
        lines = [command] + [f"{k}:{v}" for k, v in headers.items()]
        self.s.sendall(("\n".join(lines) + "\n\n" + body + "\0").encode())

    def read(self):
        data = b""
        while b"\0" not in data:
            chunk = self.s.recv(65536)
            if not chunk:
                break
            data += chunk
        return data.decode(errors="replace")

    def close(self):
        try:
            self.s.close()
        except OSError:
            pass


def rows(client, kind, query=""):
    status, page, _ = client.request("GET", f"/{kind}{query}")
    return status, page


def until(client, kind, query, ready, timeout=5):
    """A listing, once `ready(page)` holds or the time is up."""
    end = time.time() + timeout
    while True:
        status, page = rows(client, kind, query)
        if (status == 200 and ready(page)) or time.time() > end:
            return status, page
        time.sleep(0.1)


def fetch(path, headers=None):
    """A page of the console: its status, headers and bytes as sent."""
    req = urllib.request.Request(WEB + path, headers=headers or {})
    opener = urllib.request.build_opener(NoRedirect)
    try:
        with opener.open(req, timeout=10) as r:
            return r.status, r.headers, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.headers, e.read()


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args):
        return None


# The pages, which need no login. `/` goes to Artemis's console where its
# console.war is, and to Saranga's otherwise.
artemis_console = fetch("/console/index.html")[0] == 200
status, h, _ = fetch("/")
want = "/console" if artemis_console else "/saranga/"
check(f"/ redirects to {want}", status == 302 and h.get("Location") == want, (status, dict(h)))
status, h, _ = fetch("/saranga")
check("/saranga redirects to /saranga/", status == 301 and h.get("Location") == "/saranga/", (status, dict(h)))
status, h, index = fetch("/saranga/")
check("the console's page is served", status == 200 and h.get("Content-Type", "").startswith("text/html"), (status, dict(h)))
check("the page is not cached unchecked", "no-cache" in h.get("Cache-Control", "") and h.get("ETag"), dict(h))
check("the page has the security headers", h.get("X-Frame-Options") == "DENY" and h.get("X-Content-Type-Options") == "nosniff", dict(h))
csp = h.get("Content-Security-Policy", "")
html = index.decode()
inline = [m.group(1) for m in re.finditer(r"<script>(.*?)</script>", html, re.S)]
hashes = ["'sha256-" + base64.b64encode(hashlib.sha256(s.encode()).digest()).decode() + "'" for s in inline]
check("the CSP allows exactly the page's inline scripts", all(x in csp for x in hashes) and csp.count("'sha256-") == len(hashes) and "'unsafe-inline'" not in csp.split("script-src", 1)[1].split(";", 1)[0], (csp, hashes))
check("the CSP forbids framing", "frame-ancestors 'none'" in csp, csp)
status, h2, _ = fetch("/saranga/", {"If-None-Match": h.get("ETag", "")})
check("a page the browser holds is 304", status == 304, status)
status, h2, gz = fetch("/saranga/", {"Accept-Encoding": "gzip, br"})
check("the page is sent gzipped when the browser takes it", h2.get("Content-Encoding") == "gzip" and gzip.decompress(gz) == index, dict(h2))
check("the page says it varies by encoding", h2.get("Vary") == "Accept-Encoding", dict(h2))
status, _, _ = fetch("/saranga/no-such-file.js")
check("a file the console lacks is 404", status == 404, status)
assets = sorted(set(re.findall(r'"/saranga/(_app/immutable/[^"]+)"', html)))
if os.environ.get("SARANGA_CONSOLE_STUB") == "1":
    check("the stub page says how to build the console", "npm run build" in html, html[:200])
else:
    check("the page loads the console's scripts", len(assets) > 3, html[:400])
    for a in assets:
        status, ah, body = fetch("/saranga/" + a)
        want = "text/css" if a.endswith(".css") else "text/javascript"
        if not (status == 200 and ah.get("Content-Type", "").startswith(want) and "immutable" in ah.get("Cache-Control", "") and body):
            check(f"{a} is served, typed and cached for good", False, (status, dict(ah)))
            break
    else:
        check(f"its {len(assets)} scripts and styles are served, typed and cached for good", True)

anon = Client()
status, doc, _ = anon.request("GET", "/openapi.json")
check("openapi.json is served without a login", status == 200 and doc.get("openapi") == "3.1.0", status)
check("openapi.json lists the listings", f"/saranga/api/v1/queues" in (doc or {}).get("paths", {}), doc)

status, body, _ = anon.request("GET", "/me")
check("GET /me without a login is 401", status == 401 and body.get("status") == 401, (status, body))
status, _, _ = anon.request("GET", "/queues")
check("a listing without a login is 401", status == 401, status)
status, _, _ = anon.request("GET", "/nothing/here")
check("an unknown path is 404", status == 404, status)

# Logging in.
c = Client()
status, body, _ = c.request("POST", "/session", {"user": "admin", "password": "secret"})
check("a login without X-Saranga-Console is 403", status == 403, status)
status, body, _ = c.login("admin", "wrong")
check("a wrong password is 401", status == 401, status)
status, body, _ = c.login("outsider", "outsider")
check("a user without a console role is 401", status == 401, status)
status, body, _ = c.login("admin", "secret")
check("admin logs in", status == 200 and body.get("user") == "admin", (status, body))
cookie = c.last_set_cookie or ""
check(
    "the session cookie is HttpOnly, SameSite=Strict, on /saranga",
    cookie.startswith("SARANGA_SESSION_") and "HttpOnly" in cookie and "SameSite=Strict" in cookie and "Path=/saranga" in cookie,
    cookie,
)
check("admin's roles include amq", "amq" in body.get("roles", []), body)
check("admin may close connections", body.get("broker", {}).get("closeConnectionWithID") is True, body)
check("the guard is management.xml's", body.get("guard") == "management.xml", body)

v = Client()
status, body, _ = v.login("viewer", "viewer")
check("viewer logs in", status == 200, (status, body))
check("viewer may list connections", body.get("broker", {}).get("listConnections") is True, body)
check("viewer may not close connections", body.get("broker", {}).get("closeConnectionWithID") is False, body)
check("viewer may not create queues", body.get("broker", {}).get("createQueue") is False, body)

status, body, _ = Client().request("GET", "/me", basic="viewer:viewer")
check("Authorization: Basic works too", status == 200 and body.get("user") == "viewer", (status, body))

# The status and the views.
status, body, _ = c.request("GET", "/broker")
attrs = (body or {}).get("attributes", {})
check("GET /broker gives the version", status == 200 and bool(attrs.get("Version")), body)
check("GET /broker gives the uptime", "Uptime" in attrs, body)
status, body, _ = v.request("GET", "/broker")
check("viewer reads the status too", status == 200 and not body.get("denied"), body)
status, body, _ = c.request("GET", "/views")
check("GET /views has the six listings", status == 200 and sorted(body) == sorted(
    ["connections", "sessions", "consumers", "producers", "addresses", "queues"]), body)
check("the queue view sorts by name by default", body.get("queues", {}).get("defaultSort") == "name", body.get("queues"))

# Some clients for the listings.
consumer = Stomp("admin", "secret")
consumer.frame("SUBSCRIBE", {"id": "s1", "destination": "console.q1", "subscription-type": "ANYCAST",
                             "ack": "client-individual", "receipt": "r0"})
consumer.read()
producer = Stomp("admin", "secret")
producer.frame("SEND", {"destination": "console.q2", "destination-type": "ANYCAST", "receipt": "r1"}, "hello")
producer.read()

status, page = until(c, "connections", "?filter=protocol:EQUALS:STOMP", lambda p: p.get("count", 0) >= 2)
check("both STOMP connections are listed", status == 200 and page.get("count") >= 2, page)
check("the listing says its page", page.get("page") == 1 and page.get("size") == 100, page)
status, page = rows(v, "connections")
check("viewer lists connections", status == 200 and page.get("count", 0) >= 2, (status, page))

status, page = until(c, "consumers", "?filter=queue:EQUALS:console.q1", lambda p: p.get("count") == 1)
check("the consumer is listed", status == 200 and page.get("count") == 1, page)
consumer_row = (page.get("data") or [{}])[0]
status, page = rows(c, "sessions", "?filter=consumerCount:GREATER_THAN:0")
check("its session is listed", status == 200 and any(r.get("id") == consumer_row.get("session") for r in page.get("data", [])), page)
status, page = rows(c, "queues", "?filter=name:EQUALS:console.q1")
q1 = (page.get("data") or [{}])[0]
check("queue console.q1 is listed with its consumer", status == 200 and str(q1.get("consumerCount")) == "1", page)
status, page = rows(c, "queues", "?filter=name:EQUALS:console.q2")
q2 = (page.get("data") or [{}])[0]
check("queue console.q2 holds the message sent", str(q2.get("messageCount")) == "1", page)
status, page = rows(c, "addresses", "?filter=name:CONTAINS:console.q")
check("both addresses are listed", status == 200 and page.get("count") == 2, page)
status, page = rows(c, "producers")
check("the producers listing answers", status == 200 and "count" in page, (status, page))

# Every row is exactly as the document says: the console's types come from it.
TYPES = {"string": str, "integer": int}
for kind in ["connections", "sessions", "consumers", "producers", "addresses", "queues"]:
    schema = doc["components"]["schemas"][kind[0].upper() + kind[1:] + "Row"]
    status, page = rows(c, kind, "?size=10000")
    data = page.get("data", []) if status == 200 else []
    wrong = []
    for r in data:
        if set(r) != set(schema["properties"]):
            wrong.append(("members", sorted(set(r) ^ set(schema["properties"]))))
        for k, p in schema["properties"].items():
            t = TYPES[p["type"]]
            if k in r and (not isinstance(r[k], t) or isinstance(r[k], bool)):
                wrong.append((k, repr(r[k])))
    check(f"the {len(data)} {kind} rows have the documented members and types", data and not wrong, wrong[:5] or page)

status, page = rows(c, "queues", "?sort=name&order=desc&size=1")
check("a page of one", status == 200 and len(page.get("data", [])) == 1 and page.get("count", 0) > 1, page)
status, page2 = rows(c, "queues", "?sort=name&order=desc&size=1&page=2")
check("page 2 is another queue", page2.get("data") and page2["data"][0].get("name") != page["data"][0].get("name"), (page, page2))
status, body = rows(c, "queues", "?filter=name:LIKE:x")
check("an unknown filter operation is 400", status == 400, (status, body))
status, body = rows(c, "queues", "?page=0")
check("page 0 is 400", status == 400, (status, body))
status, body = rows(c, "queues", "?filter=nosuchfield:EQUALS:x")
check("an unknown filter field is 400", status == 400, (status, body))

# Permissions on things.
checks = {"checks": [
    {"resource": "queue.console.q1", "method": "removeMessages"},
    {"resource": "queue.console.q1", "method": "countMessages"},
    {"resource": "queue.nosuchqueue", "method": "countMessages"},
]}
status, body, _ = c.request("POST", "/me/can", checks)
check("admin may remove messages and count them", body.get("results") == [True, True, False], body)
status, body, _ = v.request("POST", "/me/can", checks)
check("viewer may count messages but not remove them", body.get("results") == [False, True, False], body)

# The resources: the explorer's view of the MBeans, and the operations the
# pages call through it.
status, body, _ = c.request("GET", "/resources")
names = {r.get("resource"): r for r in (body or {}).get("data", [])}
check("GET /resources lists the broker, the queues and the process",
      status == 200 and "broker" in names and "queue.console.q2" in names and "java.lang.Runtime" in names,
      (status, sorted(names)[:20]))
check("a resource has its type and MBean name",
      names.get("queue.console.q2", {}).get("type") == "queue"
      and 'queue="console.q2"' in names.get("queue.console.q2", {}).get("objectName", ""),
      names.get("queue.console.q2"))
status, q2r, _ = c.request("GET", "/resources/queue.console.q2")
attrs = {a["name"]: a for a in (q2r or {}).get("attributes", [])}
ops = {o["signature"]: o for o in (q2r or {}).get("operations", [])}
check("a resource has its attributes' values", status == 200 and attrs.get("MessageCount", {}).get("value") == 1,
      (status, attrs.get("MessageCount")))
check("and its operations, each allowed or not",
      ops.get("browse(int,int,java.lang.String)", {}).get("allowed") is True
      and ops.get("removeAllMessages()", {}).get("allowed") is True
      and ops.get("removeAllMessages()", {}).get("impact") == "ACTION",
      list(ops)[:5])
status, body, _ = v.request("GET", "/resources/queue.console.q2")
vops = {o["signature"]: o.get("allowed") for o in (body or {}).get("operations", [])}
check("viewer may browse a queue but not purge it",
      status == 200 and vops.get("browse(int,int,java.lang.String)") is True and vops.get("removeAllMessages()") is False,
      (status, vops.get("browse(int,int,java.lang.String)"), vops.get("removeAllMessages()")))
status, body, _ = c.request("GET", "/resources/queue.nosuchqueue")
check("an unknown resource is 404", status == 404, (status, body))
status, body, _ = c.request("GET", "/resources/queue.console.q2/attributes?name=MessageCount&name=Nope")
values = (body or {}).get("attributes", {})
check("some attributes are read, and one that is not there says so",
      status == 200 and values.get("MessageCount", {}).get("value") == 1 and "error" in values.get("Nope", {}),
      (status, body))

def invoke(client, resource, operation, args=(), headers=MARK):
    return client.request("POST", f"/resources/{resource}/operations",
                          {"operation": operation, "arguments": list(args)}, headers)

status, body, _ = invoke(c, "queue.console.q2", "browse(int,int,java.lang.String)", [1, 10, ""])
browsed = (body or {}).get("result") or []
check("admin browses the queue", status == 200 and len(browsed) == 1 and browsed[0].get("messageID"), (status, body))
status, body, _ = invoke(v, "queue.console.q2", "countMessages()")
check("viewer counts its messages", status == 200 and body.get("result") == 1, (status, body))
status, body, _ = invoke(c, "queue.console.q2", "countMessages()", headers={})
check("an operation without X-Saranga-Console is 403", status == 403, (status, body))
status, body, _ = invoke(v, "queue.console.q2", "removeAllMessages()")
check("viewer may not purge it", status == 403, (status, body))
status, body, _ = invoke(c, "queue.console.q2", "moveMessages")
check("an overloaded operation without its signature is 400", status == 400 and "overloaded" in body.get("error", ""),
      (status, body))
send = "sendMessage(java.util.Map,int,java.lang.String,boolean,java.lang.String,java.lang.String,boolean)"
status, body, _ = invoke(c, "address.console.q2", send, [{"color": "red"}, 3, "from the console", True, None, None, True])
check("admin sends a message as themself, with no user given", status == 200 and str(body.get("result", "")).isdigit(),
      (status, body))
status, body, _ = invoke(v, "address.console.q2", send, [{}, 3, "no", True, None, None, True])
check("viewer may not send", status == 403, (status, body))
status, body, _ = invoke(c, "queue.console.q2", "browse(int,int,java.lang.String)", [1, 10, "color = 'red'"])
sent = (body or {}).get("result") or [{}]
check("the message sent is there, as admin sent it",
      len(sent) == 1 and sent[0].get("text") == "from the console" and sent[0].get("StringProperties", {}).get("color") == "red",
      sent)
status, body, _ = invoke(c, "queue.console.q2", "removeMessage(long)", [sent[0].get("messageID")])
check("admin deletes it by its ID", status == 200 and body.get("result") is True, (status, body))

status, body, _ = c.request("PUT", "/resources/broker/attributes/MessageCounterMaxDayCount", {"value": 9}, MARK)
previous = (body or {}).get("previous")
check("admin writes an attribute, which answers its previous value", status == 200 and isinstance(previous, int),
      (status, body))
status, body, _ = c.request("GET", "/resources/broker/attributes?name=MessageCounterMaxDayCount")
check("the attribute has its new value", body.get("attributes", {}).get("MessageCounterMaxDayCount", {}).get("value") == 9,
      body)
c.request("PUT", "/resources/broker/attributes/MessageCounterMaxDayCount", {"value": previous}, MARK)
status, body, _ = v.request("PUT", "/resources/broker/attributes/MessageCounterMaxDayCount", {"value": 9}, MARK)
check("viewer may not write it", status == 403, (status, body))

# Closing.
session_id, consumer_id = consumer_row.get("session"), consumer_row.get("id")
status, body, _ = v.request("DELETE", f"/consumers/{session_id}/{consumer_id}", headers=MARK)
check("viewer may not close a consumer", status == 403, (status, body))
status, body, _ = c.request("DELETE", f"/consumers/{session_id}/{consumer_id}")
check("closing without X-Saranga-Console is 403", status == 403, (status, body))
status, body, _ = c.request("DELETE", f"/consumers/{session_id}/{consumer_id}", headers=MARK)
check("admin closes the consumer", status == 204, (status, body))
status, page = until(c, "consumers", "?filter=queue:EQUALS:console.q1", lambda p: p.get("count") == 0)
check("the consumer is gone", page.get("count") == 0, page)

status, page = until(c, "sessions", "?filter=protocol:EQUALS:CORE&filter=consumerCount:GREATER_THAN:0",
                     lambda p: p.get("count") == 1)
core = (page.get("data") or [{}])[0]
check("the Core consumer's session is listed", page.get("count") == 1, page)
status, body, _ = v.request("DELETE", f"/sessions/{core.get('connectionID')}/{core.get('id')}", headers=MARK)
check("viewer may not close a session", status == 403, (status, body))
status, body, _ = c.request("DELETE", f"/sessions/{core.get('connectionID')}/{core.get('id')}", headers=MARK)
check("admin closes the session", status == 204, (status, body))
status, page = until(c, "sessions", f"?filter=id:EQUALS:{core.get('id')}", lambda p: p.get("count") == 0)
check("the session is gone", page.get("count") == 0, page)

# A session of another protocol closes too, as in Artemis.
status, page = until(c, "sessions", "?filter=protocol:EQUALS:STOMP", lambda p: p.get("count", 0) >= 1)
stomp = (page.get("data") or [{}])[0]
status, body, _ = c.request("DELETE", f"/sessions/{stomp.get('connectionID')}/{stomp.get('id')}", headers=MARK)
check("admin closes a STOMP session", status == 204, (status, body))
status, page = until(c, "sessions", f"?filter=id:EQUALS:{stomp.get('id')}", lambda p: p.get("count") == 0)
check("the STOMP session is gone", page.get("count") == 0, page)

status, page = rows(c, "connections", "?filter=protocol:EQUALS:STOMP")
ids = [r.get("connectionID") for r in page.get("data", [])]
status, body, _ = v.request("DELETE", f"/connections/{ids[0]}", headers=MARK)
check("viewer may not close a connection", status == 403, (status, body))
for i in ids:
    status, body, _ = c.request("DELETE", f"/connections/{i}", headers=MARK)
    check(f"admin closes connection {i}", status == 204, (status, body))
status, body, _ = c.request("DELETE", f"/connections/{ids[0]}", headers=MARK)
check("closing it again is 404", status == 404, (status, body))
consumer.close()
producer.close()

# Logging out.
status, _, _ = c.request("DELETE", "/session", headers=MARK)
check("logout is 204", status == 204, status)
status, _, _ = c.request("GET", "/me")
check("after logout the session is gone", status == 401, status)

# Throttling: three failures are free, then the user waits.
t = Client()
codes = [t.login("viewer", "nope")[0] for _ in range(5)]
check("repeated failed logins are throttled (429)", codes[:3] == [401, 401, 401] and 429 in codes[3:], codes)

print(f"{failures} failed")
sys.exit(1 if failures else 0)
