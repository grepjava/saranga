#!/usr/bin/env python3
"""Generates crates/saranga-management/src/catalog.rs: the attributes and
operations of the Artemis management MBeans, as MBeanInfoHelper builds
their MBeanInfo from the control interfaces (artemis-core-client's
api/core/management/*Control.java).

    python3 scripts/gen-mbean-catalog.py

- An attribute is a getter (getX, no parameters, not void), a setter (setX,
  one parameter, void) or a boolean isX; its description is @Attribute's,
  else "N/A". A getter and a setter of the same name make one read/write
  attribute.
- Every other method is an operation, described by @Operation (or its name)
  with its impact, and parameters named by @Parameter (or "p N").
- Types are written as Class.getName writes them.
"""

import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "reference", "activemq-artemis", "artemis-core-client", "src", "main",
                   "java", "org", "apache", "activemq", "artemis", "api", "core", "management")
OUT = os.path.join(ROOT, "crates", "saranga-management", "src", "catalog.rs")

# The MBeans Saranga has resources for.
CONTROLS = [
    ("BROKER", "ActiveMQServerControl"),
    ("ADDRESS", "AddressControl"),
    ("QUEUE", "QueueControl"),
    ("DIVERT", "DivertControl"),
    ("ACCEPTOR", "AcceptorControl"),
    ("BROADCAST_GROUP", "BroadcastGroupControl"),
    ("BRIDGE", "BridgeControl"),
]

IMPACT = {"MBeanOperationInfo.INFO": 0, "MBeanOperationInfo.ACTION": 1,
          "MBeanOperationInfo.ACTION_INFO": 2, "MBeanOperationInfo.UNKNOWN": 3}

PRIMITIVES = {"int": "I", "long": "J", "boolean": "Z", "double": "D", "float": "F",
              "short": "S", "byte": "B", "char": "C"}
PACKAGES = {
    "String": "java.lang", "Object": "java.lang", "Long": "java.lang", "Integer": "java.lang",
    "Boolean": "java.lang", "Double": "java.lang", "Float": "java.lang", "Short": "java.lang",
    "Byte": "java.lang", "Character": "java.lang", "Number": "java.lang",
    "Map": "java.util", "List": "java.util", "Set": "java.util", "Collection": "java.util",
    "CompositeData": "javax.management.openmbean", "TabularData": "javax.management.openmbean",
}


def strip_comments(text):
    out, i, n = [], 0, len(text)
    while i < n:
        if text.startswith("//", i):
            i = text.find("\n", i)
            i = n if i < 0 else i
        elif text.startswith("/*", i):
            j = text.find("*/", i + 2)
            i = n if j < 0 else j + 2
        elif text[i] in "\"'":
            q, j = text[i], i + 1
            while j < n and text[j] != q:
                j += 2 if text[j] == "\\" else 1
            out.append(text[i:j + 1])
            i = j + 1
        else:
            out.append(text[i])
            i += 1
    return "".join(out)


def declarations(body):
    """The interface's member declarations: text up to ';' at depth 0, or up to
    a default method's body."""
    decls, cur, i, n = [], [], 0, len(body)
    paren = 0
    while i < n:
        c = body[i]
        if c in "\"'":
            q, j = c, i + 1
            while j < n and body[j] != q:
                j += 2 if body[j] == "\\" else 1
            cur.append(body[i:j + 1])
            i = j + 1
            continue
        if c == "(":
            paren += 1
        elif c == ")":
            paren -= 1
        if c == ";" and paren == 0:
            decls.append("".join(cur).strip())
            cur = []
        elif c == "{" and paren == 0:
            # A default method's body (or a nested type): skip it.
            depth, j = 1, i + 1
            while j < n and depth:
                if body[j] == "{":
                    depth += 1
                elif body[j] == "}":
                    depth -= 1
                j += 1
            decls.append("".join(cur).strip())
            cur = []
            i = j
            continue
        else:
            cur.append(c)
        i += 1
    return [d for d in decls if d]


# String constants of the interfaces, by name.
CONSTANTS = {
    # ActiveMQDefaultConfiguration, which a description names.
    "DEFAULT_EMBEDDED_WEB_SERVER_RESTART_TIMEOUT": "5000",
}


def unescape(s):
    return s.encode("utf-8").decode("unicode_escape").encode("latin-1").decode("utf-8")


def java_string(expr):
    """The value of a constant string expression: literals and constants
    joined with +."""
    out = []
    for lit, ident in re.findall(r'"((?:[^"\\]|\\.)*)"|([A-Za-z_][\w.]*)', expr):
        if ident:
            out.append(CONSTANTS.get(ident.split(".")[-1], ""))
        else:
            out.append(unescape(lit))
    return "".join(out)


def annotation(text, name):
    """The arguments of @name(...) in text, or None."""
    m = re.search(r"@" + name + r"\s*\(", text)
    if not m:
        return None
    i, depth = m.end(), 1
    j = i
    while depth:
        if text[j] in "\"":
            j += 1
            while text[j] != '"':
                j += 2 if text[j] == "\\" else 1
        elif text[j] == "(":
            depth += 1
        elif text[j] == ")":
            depth -= 1
        j += 1
    return text[i:j - 1]


def arg(args, key):
    """key = value in annotation arguments, up to the next top-level comma."""
    m = re.search(r"\b" + key + r"\s*=\s*", args)
    if not m:
        return None
    i, depth, inq = m.end(), 0, False
    j = i
    while j < len(args):
        c = args[j]
        if inq:
            if c == "\\":
                j += 1
            elif c == '"':
                inq = False
        elif c == '"':
            inq = True
        elif c in "([":
            depth += 1
        elif c in ")]":
            depth -= 1
        elif c == "," and depth == 0:
            break
        j += 1
    return args[i:j].strip()


def strip_annotations(text):
    out, i = [], 0
    while i < len(text):
        if text[i] == "@":
            m = re.match(r"@[\w.]+", text[i:])
            j = i + m.end()
            k = j
            while k < len(text) and text[k].isspace():
                k += 1
            if k < len(text) and text[k] == "(":
                depth, k = 1, k + 1
                while depth:
                    if text[k] == '"':
                        k += 1
                        while text[k] != '"':
                            k += 2 if text[k] == "\\" else 1
                    elif text[k] == "(":
                        depth += 1
                    elif text[k] == ")":
                        depth -= 1
                    k += 1
                j = k
            i = j
        else:
            out.append(text[i])
            i += 1
    return "".join(out)


def class_name(t):
    """Class.getName of a source type."""
    t = re.sub(r"<.*>", "", t.strip())  # erasure
    t = t.replace("...", "[]")
    dims = t.count("[]")
    base = t.replace("[]", "").strip()
    if dims == 0:
        if base in PRIMITIVES or base == "void":
            return base
        return f"{PACKAGES.get(base, 'java.lang')}.{base}" if "." not in base else base
    elem = PRIMITIVES.get(base) or f"L{PACKAGES.get(base, 'java.lang')}.{base};"
    return "[" * dims + elem


def split_params(s):
    parts, depth, cur = [], 0, []
    for c in s:
        if c in "<(":
            depth += 1
        elif c in ">)":
            depth -= 1
        if c == "," and depth == 0:
            parts.append("".join(cur))
            cur = []
        else:
            cur.append(c)
    if "".join(cur).strip():
        parts.append("".join(cur))
    return parts


def parse_interface(name, seen=None):
    text = strip_comments(open(os.path.join(SRC, name + ".java"), encoding="utf-8").read())
    m = re.search(r"interface\s+" + name + r"\s*(?:extends\s+([\w\s,]+))?\{", text)
    supers = [s.strip() for s in (m.group(1) or "").split(",") if s.strip()]
    body = text[m.end():text.rfind("}")]
    methods = []
    decls = declarations(body)
    for d in decls:
        c = re.match(r"\s*(?:public\s+|static\s+|final\s+)*String\s+(\w+)\s*=\s*(.*)$", d, re.S)
        if c:
            CONSTANTS[c.group(1)] = java_string(c.group(2))
        n = re.match(r"\s*(?:public\s+|static\s+|final\s+)*(?:long|int)\s+(\w+)\s*=\s*(-?\d+)L?\s*$", d)
        if n:
            CONSTANTS[n.group(1)] = n.group(2)
    for d in decls:
        plain = strip_annotations(d)
        mm = re.search(r"([\w<>\[\],.?\s]+?)\s+(\w+)\s*\((.*)\)\s*(?:throws[\w\s,.]+)?$", plain, re.S)
        if not mm or "=" in plain.split("(")[0]:
            continue
        ret = re.sub(r"\b(default|static|public|abstract|synchronized)\b", "", mm.group(1))
        ret = re.sub(r"^\s*<[^>]+>\s*", "", ret).strip()
        mname, params_src = mm.group(2), mm.group(3)
        # Parameters with their annotations, from the original text.
        orig_params = d[d.index("(", d.index(mname)) + 1:d.rindex(")")]
        params = []
        for k, p in enumerate(split_params(orig_params)):
            pa = annotation(p, "Parameter")
            ptext = strip_annotations(p).strip()
            ptext = re.sub(r"\bfinal\b", "", ptext).strip()
            ptype = ptext.rsplit(None, 1)[0] if " " in ptext else ptext
            if pa is not None:
                pname = java_string(arg(pa, "name") or '""')
                pdesc = java_string(arg(pa, "desc") or '""')
            else:
                pname, pdesc = f"p {k + 1}", f"parameter {k + 1}"
            params.append((pname, class_name(ptype), pdesc))
        methods.append({
            "name": mname, "ret": class_name(ret), "params": params,
            "attribute": annotation(d, "Attribute"), "operation": annotation(d, "Operation"),
        })
    for s in supers:
        methods += parse_interface(s)
    return methods


def is_getter(m):
    return m["name"] != "get" and m["name"].startswith("get") and not m["params"] and m["ret"] != "void"


def is_setter(m):
    return m["name"] != "set" and m["name"].startswith("set") and len(m["params"]) == 1 and m["ret"] == "void"


def is_is(m):
    return m["name"] != "is" and m["name"].startswith("is") and not m["params"] and m["ret"] == "boolean"


def rust_str(s):
    return '"' + s.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n") + '"'


def catalog(iface):
    methods = parse_interface(iface)
    attrs, order = {}, []
    ops = []
    for m in methods:
        if is_getter(m) or is_setter(m) or is_is(m):
            name = m["name"][2:] if is_is(m) else m["name"][3:]
            desc = java_string(arg(m["attribute"], "desc") or '""') if m["attribute"] is not None else "N/A"
            if m["attribute"] is not None and arg(m["attribute"], "desc") is None:
                desc = ""
            typ = m["params"][0][1] if is_setter(m) else m["ret"]
            a = attrs.get(name)
            if a is None:
                attrs[name] = {"type": typ, "desc": desc, "r": not is_setter(m), "w": is_setter(m), "is": is_is(m)}
                order.append(name)
            else:
                a["r"] |= not is_setter(m)
                a["w"] |= is_setter(m)
                a["is"] |= is_is(m)
                if a["type"] == "void":
                    a["type"] = typ
        else:
            if m["operation"] is not None:
                desc = java_string(arg(m["operation"], "desc") or '""')
                impact = IMPACT.get(arg(m["operation"], "impact") or "", 3)
            else:
                desc, impact = m["name"], 3
            ops.append((m["name"], desc, impact, m["ret"], m["params"]))
    return order, attrs, ops


def ordered(iface, order, attrs, ops):
    """The members in the order Artemis's MBeanInfo gives them
    (scripts/mbean-order.json, from mbean-order.py), else source order."""
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mbean-order.json")
    if not os.path.exists(path):
        return order, ops
    import json
    known = json.load(open(path, encoding="utf-8")).get(iface)
    if not known:
        return order, ops
    rank = {n: i for i, n in enumerate(known["attrs"])}
    order = sorted(order, key=lambda n: rank.get(n, len(rank)))
    oprank = {(n, sig): i for i, (n, sig) in enumerate(known["ops"])}
    sig = lambda o: "(" + ",".join(p[1] for p in o[4]) + ")"
    ops = sorted(ops, key=lambda o: oprank.get((o[0], sig(o)), len(oprank)))
    return order, ops


def main():
    out = ["// Generated by scripts/gen-mbean-catalog.py from the Artemis 2.57.0 control",
           "// interfaces. Do not edit.", "",
           "use crate::mbean::{Attr, Control, Op, Param};", ""]
    for const, iface in CONTROLS:
        order, attrs, ops = catalog(iface)
        order, ops = ordered(iface, order, attrs, ops)
        out.append(f"/// `{iface}`.")
        out.append(f"pub static {const}: Control = Control {{")
        out.append(f"    interface: {rust_str(iface)},")
        out.append("    attrs: &[")
        for n in order:
            a = attrs[n]
            out.append(f"        Attr {{ name: {rust_str(n)}, type_name: {rust_str(a['type'])}, desc: {rust_str(a['desc'])}, "
                       f"readable: {str(a['r']).lower()}, writable: {str(a['w']).lower()}, is: {str(a['is']).lower()} }},")
        out.append("    ],")
        out.append("    ops: &[")
        for (n, desc, impact, ret, params) in ops:
            ps = ", ".join(f"Param {{ name: {rust_str(p[0])}, type_name: {rust_str(p[1])}, desc: {rust_str(p[2])} }}" for p in params)
            out.append(f"        Op {{ name: {rust_str(n)}, desc: {rust_str(desc)}, impact: {impact}, ret: {rust_str(ret)}, params: &[{ps}] }},")
        out.append("    ],")
        out.append("};")
        out.append("")
        print(f"{iface}: {len(order)} attributes, {len(ops)} operations", file=sys.stderr)
    with open(OUT, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(out))


if __name__ == "__main__":
    main()
