"""What a `data exp` file holds, without what an import changes: IDs and
user IDs, the broker's own addresses and queues, and whether a body is
large, with its _AMQ_LARGE_SIZE (a big body converted from AMQP is imported
as a large message).

    python -I content.py <export.xml> [<names.txt>]

Prints the bindings and messages, sorted. With names.txt (one queue or
address name a line), only those bindings, and messages on those queues.
"""

import base64
import hashlib
import sys
import xml.etree.ElementTree as ET

sys.stdout.reconfigure(encoding="utf-8")

root = ET.parse(sys.argv[1]).getroot()
names = None
if len(sys.argv) > 2:
    with open(sys.argv[2], encoding="utf-8") as f:
        names = {line.strip() for line in f if line.strip()}

out = []
for b in root.find("bindings"):
    a = b.attrib
    if names is not None and a["name"] not in names:
        continue
    if b.tag == "address-binding":
        out.append(f"address {a['name']} {a['routing-types']}")
    else:
        out.append(f"queue {a['name']} address={a['address']} filter={a['filter-string']} {a['routing-type']}")

for m in root.find("messages"):
    queues = sorted(q.attrib["name"] for q in m.find("queues"))
    if names is not None:
        queues = [q for q in queues if q in names]
        if not queues:
            continue
    a = m.attrib
    props = sorted(
        f"{p.attrib['name']}={p.attrib.get('value')}:{p.attrib.get('type')}"
        for p in m.find("properties")
        if p.attrib["name"] not in ("_AMQ_ROUTE_TO", "_AMQ_LARGE_SIZE")
    )
    body = m.find("body")
    # Each chunk (a large body has several) is Base64 of its own.
    raw = b"".join(base64.urlsafe_b64decode(c) for c in "".join(body.itertext()).split())
    data = f"{len(raw)}:{hashlib.sha1(raw).hexdigest()}"
    out.append(
        f"message queues={queues} type={a['type']} priority={a['priority']} "
        f"expiration={a['expiration']} timestamp={a['timestamp']} user-id={'user-id' in a} "
        f"props={props} body={data}"
    )

for line in sorted(out):
    print(line)
