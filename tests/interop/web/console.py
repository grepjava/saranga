"""The web console suite: Saranga serving Artemis's own console
(web/console.war) against Artemis 2.57.0 serving it, request by request.

    python -I tests/interop/web/console.py SARANGA ARTEMIS

Each broker is `URL,user,password`, the user holding a console role. The
same requests go to both: the redirects of the console's routes, the
hawtio endpoints (login and its throttling, logout, the user, the auth
configuration, Keycloak, plugins, the proxy, refresh), the console's files,
and Jolokia through a console session. Their statuses, the headers below
and the bodies must be the same, after session IDs, ports and the time are
normalized. When both brokers have a session timeout of 10 s or less
(`-Dhawtio.sessionTimeout`), sessions are also left idle until they expire.
"""

import base64
import http.client
import json
import re
import sys
import time
import urllib.parse

HEADERS = [
    "location", "content-type", "cache-control", "hawtio-forbidden-reason",
    "retry-after", "x-frame-options", "x-xss-protection",
    "x-content-type-options", "content-security-policy",
    "strict-transport-security", "referrer-policy", "expires", "set-cookie",
]
FAILED = []


class Broker:
    def __init__(self, spec):
        url, self.user, self.password = spec.split(",")
        u = urllib.parse.urlsplit(url)
        self.host, self.port = u.hostname, u.port
        self.cookie = None
        # A user no other check logs in as, for the throttling.
        self.nobody = "nobody%d" % time.time_ns()

    def request(self, method, path, body=None, headers=None, keep=True):
        """(status, {header: value}, body bytes); a session cookie the
        answer sets is kept for the next requests."""
        c = http.client.HTTPConnection(self.host, self.port, timeout=30)
        h = dict(headers or {})
        if self.cookie:
            h["Cookie"] = self.cookie
        if body is not None:
            h["Content-Type"] = "application/json"
        c.request(method, path, body=body, headers=h)
        r = c.getresponse()
        data = r.read()
        hs = {}
        for k, v in r.getheaders():
            k = k.lower()
            hs[k] = hs[k] + ", " + v if k in hs else v
        c.close()
        if keep and "set-cookie" in hs:
            self.cookie = hs["set-cookie"].split(";")[0]
        return r.status, hs, data

    def basic(self, user, password):
        token = base64.b64encode(f"{user}:{password}".encode()).decode()
        return {"Authorization": "Basic " + token}

    def login(self, user=None, password=None):
        body = json.dumps({"username": user or self.user,
                           "password": password or self.password})
        return self.request("POST", "/console/auth/login", body)


def normalize(b, status, headers, data):
    hs = {}
    for k in HEADERS:
        if k in headers:
            v = headers[k]
            v = re.sub(r"JSESSIONID_(\d+)_\d+=[^;]+", r"JSESSIONID_\1_PORT=ID", v)
            hs[k] = v
    for k in ("last-modified", "expires"):
        if k in headers and not headers[k].startswith("Thu, 01 Jan 1970"):
            hs[k] = "present"
    text = data.decode("utf-8", "replace").replace("\r\n", "\n")
    text = re.sub(r'"now":\d+', '"now":NOW', text)
    # The users differ: their names, where an answer gives them.
    if text.strip() == json.dumps(b.user):
        text = text.replace(json.dumps(b.user), '"USER"')
    text = text.replace(f'"name":"{b.user}"', '"name":"USER"')
    return status, hs, text


def compare(name, results):
    """Each broker's normalized answer must be the same."""
    (s, sa), (a, aa) = results
    if sa == aa:
        print("PASS", name)
        return True
    print("FAIL", name)
    for label, x, y in (("status", sa[0], aa[0]), ("headers", sa[1], aa[1]),
                        ("body", sa[2], aa[2])):
        if x != y:
            if isinstance(x, dict):
                for k in sorted(set(x) | set(y)):
                    if x.get(k) != y.get(k):
                        print(f"  {k}: saranga {x.get(k)!r}, artemis {y.get(k)!r}")
            else:
                print(f"  {label}: saranga {str(x)[:300]!r}\n  {' ' * len(label)}  artemis {str(y)[:300]!r}")
    FAILED.append(name)
    return False


def both(name, brokers, f):
    """Runs f(broker) -> (status, headers, body) on each and compares."""
    return compare(name, [(b, normalize(b, *f(b))) for b in brokers])


def check(name, ok, detail=""):
    print("PASS" if ok else "FAIL", name, "" if ok else detail)
    if not ok:
        FAILED.append(name)


def jolokia_read(b, extra=None):
    body = json.dumps({"type": "read", "mbean": "org.apache.activemq.artemis:broker=\"0.0.0.0\"",
                       "attribute": "Version"})
    h = {"Origin": f"http://localhost:{b.port}"}
    h.update(extra or {})
    status, headers, data = b.request("POST", "/console/jolokia", body, h)
    if status == 200:
        j = json.loads(data)
        # The version and timestamp differ; the status and type must not.
        data = json.dumps({"status": j.get("status"), "type": j.get("request", {}).get("type"),
                           "error_type": j.get("error_type")}).encode()
    return status, headers, data


def main():
    brokers = [Broker(sys.argv[1]), Broker(sys.argv[2])]
    get = lambda path, **kw: (lambda b: b.request("GET", path, **kw))

    # Without a session.
    for path in ["/console", "/console/", "/console/login", "/console/login/x",
                 "/console/index.html", "/console/artemis/queues",
                 "/console/artemisconfig.json", "/console/asset-manifest.json",
                 "/console/html.worker.js", "/console/auth/x",
                 "/console/auth/config", "/console/auth/config/",
                 "/console/auth/config/login", "/console/auth/config/oidc",
                 "/console/auth/config/x", "/console/auth/login",
                 "/console/auth/logout", "/console/user", "/console/user/x",
                 "/console/keycloak/enabled", "/console/keycloak/client-config",
                 "/console/keycloak", "/console/keycloak/x",
                 "/console/keycloak/validate-subject-matches",
                 "/console/keycloak/validate-subject-matches?keycloakUser=a",
                 "/console/plugin", "/console/plugin/x", "/console/proxy/enabled",
                 "/console/proxy", "/console/proxy/x", "/console/refresh",
                 "/console/preset-connections", "/console/jolokiax",
                 "/console/favicon.ico", "/console/hawtconfig.json",
                 "/console/css/artemis.css", "/console/img/favicon.png",
                 "/console/editor.worker.js", "/console/static/nosuch.js",
                 "/console/static/a%3Cb.js", "/console/static/../index.html",
                 "/console/jolokia/version"]:
        both("GET " + path, brokers, get(path))
    both("session-timeout", brokers, lambda b: (lambda r: (
        r[0], r[1], re.sub(rb'"timeout":-?\d+', b'"timeout":T', r[2])))(
        b.request("GET", "/console/auth/config/session-timeout?t=5")))
    both("session-timeout, no t", brokers, lambda b: (lambda r: (
        r[0], r[1], re.sub(rb'"req":\d+', b'"req":REQ', re.sub(rb'"timeout":-?\d+', b'"timeout":T', r[2]))))(
        b.request("GET", "/console/auth/config/session-timeout?t=x")))

    # Every file the console's index.html loads, byte for byte.
    status, _, index = brokers[1].request("GET", "/console/index.html")
    assets = re.findall(rb'(?:src|href)="(static/[^"]+|img/[^"]+)"', index)
    check("index.html names assets", len(assets) >= 5, assets)
    for a in assets:
        both("asset " + a.decode(), brokers, get("/console/" + a.decode()))
    # A file not modified since the browser's copy.
    def not_modified(b):
        _, h, _ = b.request("GET", "/console/hawtconfig.json")
        return b.request("GET", "/console/hawtconfig.json",
                         headers={"If-Modified-Since": h["last-modified"]})
    both("If-Modified-Since", brokers, not_modified)

    # Login failures, and the throttling of a user who keeps failing.
    both("login, wrong password", brokers, lambda b: b.login(b.user, "wrong" + b.password))
    both("login, no body", brokers, lambda b: b.request("POST", "/console/auth/login", "{}"))
    # Java's parse error is not compared.
    both("login, not JSON", brokers, lambda b: (lambda r: (r[0], r[1], b""))(
        b.request("POST", "/console/auth/login", "x")))
    both("login, no password", brokers, lambda b: b.request(
        "POST", "/console/auth/login", json.dumps({"username": b.user})))
    for i in range(1, 6):
        both(f"login, unknown user, attempt {i}", brokers, lambda b: b.login(b.nobody, "x"))
    both("login, throttled user again", brokers, lambda b: b.login(b.nobody, "x"))
    both("jolokia, Basic, throttled user", brokers,
         lambda b: jolokia_read(b, b.basic(b.nobody, "x")))

    # A session.
    both("login", brokers, lambda b: b.login())
    for path in ["/console/user", "/console/user/x", "/console/refresh", "/console/",
                 "/console/artemis/queues", "/console/login", "/console/login/x",
                 "/console/artemisconfig.json", "/console/proxy/x",
                 "/console/keycloak/validate-subject-matches?keycloakUser=" + "{user}"]:
        both("session: GET " + path, brokers,
             lambda b, p=path: b.request("GET", p.replace("{user}", b.user)))
    both("session: jolokia", brokers, lambda b: jolokia_read(b))
    # What the console's tree and permissions are built from: names as
    # registered (canonicalNaming=false), and hawtio's permission MBean.
    for canonical in ("true", "false"):
        both(f"search, canonicalNaming={canonical}", brokers, lambda b, c=canonical: b.request(
            "GET", "/console/jolokia/search/" + urllib.parse.quote(
                'org.apache.activemq.artemis:subcomponent=queues,queue="DLQ",*', safe=":=,*")
            + "?canonicalNaming=" + c, headers={"Origin": f"http://localhost:{b.port}"}))
    def can_invoke(b):
        sec = "hawtio:type=security,area=jmx,name=ArtemisJMXSecurity"
        q = 'org.apache.activemq.artemis:broker="0.0.0.0",component=addresses,address="DLQ",subcomponent=queues,routing-type="anycast",queue="DLQ"'
        ops = ["browse(int,int,java.lang.String)", "removeAllMessages()", "sendMessage(java.util.Map,int,java.lang.String,boolean,java.lang.String,java.lang.String,boolean)", "browse"]
        body = json.dumps([
            {"type": "exec", "mbean": sec, "operation": "canInvoke(java.lang.String)", "arguments": [q]},
            {"type": "exec", "mbean": sec, "operation": "canInvoke(java.lang.String,java.lang.String)", "arguments": [q, "browse"]},
            {"type": "exec", "mbean": sec, "operation": "canInvoke(java.util.Map)", "arguments": [{q: ops, sec: []}]},
            {"type": "exec", "mbean": sec, "operation": "canInvoke(java.lang.String)", "arguments": ["not a name"]},
        ])
        status, headers, data = b.request("POST", "/console/jolokia", body,
                                          {"Origin": f"http://localhost:{b.port}"})
        values = [r.get("value", r.get("error_type")) for r in json.loads(data)]
        return status, headers, json.dumps(values, sort_keys=True).encode()
    both("hawtio canInvoke", brokers, can_invoke)
    both("hawtio search", brokers, lambda b: (lambda r: (r[0], r[1], json.dumps(
        "hawtio:area=jmx,name=ArtemisJMXSecurity,type=security" in json.loads(r[2])["value"]).encode()))(
        b.request("GET", "/console/jolokia/search/hawtio:*", headers={"Origin": f"http://localhost:{b.port}"})))
    both("session: jolokia, Basic of the same user", brokers,
         lambda b: jolokia_read(b, b.basic(b.user, b.password)))
    # Another user's Authorization ends the session.
    both("session: jolokia, Basic of another user", brokers,
         lambda b: jolokia_read(b, b.basic(b.nobody + "x", "x")))
    both("ended: GET /console/user", brokers, get("/console/user"))
    both("ended: jolokia", brokers, lambda b: jolokia_read(b))

    # Keycloak's check of another user ends the session too.
    both("login again", brokers, lambda b: b.login())
    both("keycloak user mismatch", brokers,
         get("/console/keycloak/validate-subject-matches?keycloakUser=someoneelse"))
    both("after mismatch: GET /console/user", brokers, get("/console/user"))

    # Logging in again replaces the session; logging out ends it.
    both("login", brokers, lambda b: b.login())
    def relogin(b):
        old = b.cookie
        r = b.login()
        r2 = b.request("GET", "/console/user", keep=False, headers={})
        b.cookie = old
        r3 = b.request("GET", "/console/user", keep=False)
        return r[0], r[1], b"%d %d" % (r2[0], r3[0])
    both("login replaces the session", brokers, relogin)
    both("logout", brokers, get("/console/auth/logout"))
    both("after logout: GET /console/user", brokers, get("/console/user"))
    both("after logout: GET /console/artemis/queues", brokers, get("/console/artemis/queues"))
    both("after logout: jolokia", brokers, lambda b: jolokia_read(b))

    # Pre-emptive authentication of a client route.
    for b in brokers:
        b.cookie = None
    both("route, Basic", brokers,
         lambda b: b.request("GET", "/console/artemis/queues", headers=b.basic(b.user, b.password)))
    both("route, Basic: GET /console/user", brokers, get("/console/user"))
    for b in brokers:
        b.cookie = None
    both("route, wrong Basic", brokers,
         lambda b: b.request("GET", "/console/artemis/queues", headers=b.basic(b.user, "wrong")))
    both("route, throttled Basic", brokers,
         lambda b: b.request("GET", "/console/artemis/queues", headers=b.basic(b.nobody, "x")))
    both("login page, Basic", brokers,
         lambda b: b.request("GET", "/console/login", headers=b.basic(b.user, b.password)))

    # Idle sessions, when both brokers time them out quickly.
    timeouts = [json.loads(b.request("GET", "/console/auth/config/session-timeout")[2])["timeout"]
                for b in brokers]
    if all(0 < t <= 10 for t in timeouts) and timeouts[0] == timeouts[1]:
        t = timeouts[0]
        for b in brokers:
            b.cookie = None
            b.login()
            b.request("GET", "/console/user")
        time.sleep(t / 2 + 0.5)
        for b in brokers:
            jolokia_read(b)
        time.sleep(t / 2 + 1)
        # Jolokia does not keep a session alive.
        both("idle but for Jolokia", brokers, get("/console/user"))
        for b in brokers:
            b.cookie = None
            b.login()
            b.request("GET", "/console/user")
        time.sleep(t / 2 + 0.5)
        for b in brokers:
            b.request("GET", "/console/refresh")
        time.sleep(t / 2 + 1)
        both("refreshed", brokers, get("/console/user"))
        time.sleep(t + 2)
        both("idle: refresh", brokers, get("/console/refresh"))
        both("idle: GET /console/user", brokers, get("/console/user"))
    else:
        print("SKIP idle sessions: timeouts", timeouts)

    print(f"{len(FAILED)} failed" if FAILED else "all passed")
    return 1 if FAILED else 0


if __name__ == "__main__":
    sys.exit(main())
