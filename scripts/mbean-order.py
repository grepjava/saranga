#!/usr/bin/env python3
"""Writes scripts/mbean-order.json: the order of the attributes and
operations of the Artemis MBeans as their MBeanInfo lists them (the order of
Class.getMethods, which the source does not give), from a Jolokia `list` of
an Artemis 2.57.0 broker:

    curl -u artemis:artemis -H 'Origin: http://localhost' \
      http://localhost:8161/console/jolokia/list/org.apache.activemq.artemis > list.json
    python3 scripts/mbean-order.py list.json

gen-mbean-catalog.py orders the catalog by it.
"""
import json
import os
import sys

KEYS = {
    "ActiveMQServerControl": lambda k: "component=" not in k,
    "AddressControl": lambda k: k.endswith("component=addresses") and "subcomponent" not in k,
    "QueueControl": lambda k: "subcomponent=queues" in k,
    "DivertControl": lambda k: "subcomponent=diverts" in k,
    "AcceptorControl": lambda k: "component=acceptors" in k,
    "ClusterConnectionControl": lambda k: "component=cluster-connections" in k,
}

value = json.load(open(sys.argv[1], encoding="utf-8"))["value"]
order = {}
for iface, pick in KEYS.items():
    key = next((k for k in value if pick(k)), None)
    if key is None:
        continue
    info = value[key]
    ops = []
    for name, o in info["op"].items():
        for x in (o if isinstance(o, list) else [o]):
            ops.append([name, "(" + ",".join(a["type"] for a in x["args"]) + ")"])
    order[iface] = {"attrs": list(info["attr"]), "ops": ops}
out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mbean-order.json")
with open(out, "w", encoding="utf-8", newline="\n") as f:
    json.dump(order, f, indent=1)
    f.write("\n")
print("wrote", out, {k: (len(v["attrs"]), len(v["ops"])) for k, v in order.items()})
