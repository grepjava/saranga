"""The console API suite: every endpoint of /saranga/api/v1, as each role.

    python -I api.py <web url> <stomp host:port>

The instance (see api.sh) has the users admin/secret (role amq, may do
anything), viewer/viewer (role view, may only read) and outsider/outsider
(role other, which is not a console role), and the suite's management.xml.
A STOMP client gives the listings a connection, a session, a consumer and a
producer. Prints a line per check and ends with the number that failed.
"""

import base64
import json
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
