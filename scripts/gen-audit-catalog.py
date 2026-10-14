#!/usr/bin/env python3
"""Generates crates/saranga-server/src/audit/catalog.rs: the audit message
each management method logs, from Artemis's AuditLogger and the *ControlImpl
classes that call it.

For every public method of the controls Saranga serves, the first
AuditLogger.<name>(...) call in its body gives the message: the static
method's logger (base, resource or message), the @LogMessage id and text
of the bundle method it calls, and what it passes for
AuditLogger.parametersList. A method that only calls another of its class
(an overload that fills in defaults) logs what that one logs, with the
arguments it is given.

Usage: scripts/gen-audit-catalog.py [artemis-source-dir]
"""
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = sys.argv[1] if len(sys.argv) > 1 else os.path.join(ROOT, "reference", "activemq-artemis")
AUDIT = os.path.join(SRC, "artemis-commons/src/main/java/org/apache/activemq/artemis/logs/AuditLogger.java")
IMPL = os.path.join(SRC, "artemis-server/src/main/java/org/apache/activemq/artemis/core/management/impl")
OUT = os.path.join(ROOT, "crates/saranga-server/src/audit/catalog.rs")

# The controls, and the Rust name of each.
CONTROLS = {
    "ActiveMQServerControlImpl.java": "Broker",
    "AddressControlImpl.java": "Address",
    "QueueControlImpl.java": "Queue",
    "DivertControlImpl.java": "Divert",
    "AcceptorControlImpl.java": "Acceptor",
    "BaseBroadcastGroupControlImpl.java": "BroadcastGroup",
    "BroadcastGroupControlImpl.java": "BroadcastGroup",
}


def strip_comments(text):
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    return re.sub(r"//[^\n]*", "", text)


def split_top(s):
    """Splits on commas outside (), <>, [] and {} and string literals."""
    parts, depth, cur, quote = [], 0, "", None
    i = 0
    while i < len(s):
        c = s[i]
        if quote:
            cur += c
            if c == "\\":
                cur += s[i + 1]
                i += 2
                continue
            if c == quote:
                quote = None
            i += 1
            continue
        if c in "\"'":
            quote = c
        elif c in "(<[{":
            depth += 1
        elif c in ")>]}":
            depth -= 1
        elif c == "," and depth == 0:
            parts.append(cur.strip())
            cur = ""
            i += 1
            continue
        cur += c
        i += 1
    if cur.strip():
        parts.append(cur.strip())
    return parts


def matching(text, start):
    """The index after the bracket that closes the one at start."""
    open_c = text[start]
    close_c = {"(": ")", "{": "}"}[open_c]
    depth, i, quote = 0, start, None
    while i < len(text):
        c = text[i]
        if quote:
            if c == "\\":
                i += 2
                continue
            if c == quote:
                quote = None
        elif c in "\"'":
            quote = c
        elif c == open_c:
            depth += 1
        elif c == close_c:
            depth -= 1
            if depth == 0:
                return i + 1
        i += 1
    raise ValueError("unbalanced")


def audit_logger():
    text = strip_comments(open(AUDIT, encoding="utf-8").read())
    # Bundle methods: (name, arity) -> (id, text).
    messages = {}
    for m in re.finditer(
        r'@LogMessage\(\s*id\s*=\s*(\d+)\s*,\s*value\s*=\s*"((?:[^"\\]|\\.)*)"\s*,\s*level\s*=\s*LogMessage\.Level\.(\w+)\s*\)\s*void\s+(\w+)\s*\(',
        text,
    ):
        start = m.end() - 1
        params = text[start + 1:matching(text, start) - 1]
        arity = len(split_top(params)) if params.strip() else 0
        messages[(m.group(4), arity)] = (int(m.group(1)), m.group(2))
    # Static methods: (name, arity, varargs) -> (logger, message, bundle args).
    statics = {}
    for m in re.finditer(r"static\s+void\s+(\w+)\s*\(", text):
        start = m.end() - 1
        end = matching(text, start)
        params = split_top(text[start + 1:end - 1])
        brace = text.index("{", end)
        body = text[brace + 1:matching(text, brace) - 1]
        call = re.search(r"(BASE|RESOURCE|MESSAGE)_LOGGER\.(\w+)\(", body)
        if not call:
            continue
        cstart = call.end() - 1
        args = split_top(body[cstart + 1:matching(body, cstart) - 1])
        varargs = bool(params) and "..." in params[-1]
        message = messages.get((call.group(2), len(args)))
        if message is None:
            print(f"no message for {call.group(2)}/{len(args)}", file=sys.stderr)
            continue
        names = [p.split()[-1] for p in params]
        joined = " ".join(args)
        # The parameter whose values parametersList shows, and whether the
        # caller is the subject passed in rather than the current one.
        plist = re.search(r"parametersList[(](\w+)[)]", joined)
        first = names.index(plist.group(1)) if plist and plist.group(1) in names else None
        explicit = bool(re.search(r"getCaller[(]\s*\w", joined))
        statics.setdefault(
            (m.group(1), len(params), varargs), (call.group(1), message, first, varargs, explicit)
        )
    return statics, messages


def resolve_static(statics, name, nargs):
    """The static method a call with nargs arguments reaches."""
    for (n, arity, varargs), v in statics.items():
        if n == name and not varargs and arity == nargs:
            return v
    for (n, arity, varargs), v in statics.items():
        if n == name and varargs and nargs >= arity - 1:
            return v
    return None


def java_string(lit):
    return bytes(lit[1:-1], "utf-8").decode("unicode_escape")


def value_of(expr, names):
    """What an argument expression is, in terms of the method's parameters:
    ("param", i), ("text", s), ("null",), or ("expr", e)."""
    e = expr.strip()
    # Casts and conversions that keep the value: (String) x, x.toString(),
    # String.valueOf(x), SimpleString.of(x).
    while True:
        m = re.fullmatch(r"\(\s*[\w.<>\[\]]+\s*\)\s*(.+)", e)
        if m:
            e = m.group(1).strip()
            continue
        m = re.fullmatch(r"(?:String\.valueOf|SimpleString\.of|Objects\.toString)\((.+)\)", e)
        if m and len(split_top(m.group(1))) == 1:
            e = m.group(1).strip()
            continue
        m = re.fullmatch(r"(.+)\.toString\(\)", e)
        if m:
            e = m.group(1).strip()
            continue
        break
    if e in names:
        return ("param", names.index(e))
    if e == "null":
        return ("null",)
    if re.fullmatch(r'"(?:[^"\\]|\\.)*"', e):
        return ("text", java_string(e))
    if re.fullmatch(r"true|false|-?\d+L?|-?\d+\.\d*[dDfF]?", e):
        return ("text", e.rstrip("LdDfF") if not re.fullmatch(r"-?\d+\.\d*[dDfF]?", e) else e.rstrip("dDfF"))
    m = re.fullmatch(r"(?:\w+\.)*([A-Z][A-Z_0-9]+)", e)
    if m:
        # A constant: the default it names.
        return ("default", e)
    return ("expr", e)


def parse_methods(text):
    methods = []
    for m in re.finditer(r"\n\s*public\s+(?:synchronized\s+)?[\w<>\[\], ?.]+?\s+(\w+)\s*\(", text):
        start = m.end() - 1
        end = matching(text, start)
        params = []
        for p in split_top(text[start + 1:end - 1]):
            p = re.sub(r"@\w+(\([^)]*\))?\s*", "", p).replace("final ", "").strip()
            if p:
                ptype, pname = p.rsplit(None, 1)
                params.append((ptype.strip(), pname.strip()))
        brace = text.find("{", end)
        semi = text.find(";", end)
        if brace < 0 or (0 <= semi < brace):
            continue
        methods.append((m.group(1), params, text[brace + 1:matching(text, brace) - 1]))
    return methods


def controls(statics):
    entries = []
    for file, control in CONTROLS.items():
        text = strip_comments(open(os.path.join(IMPL, file), encoding="utf-8").read())
        methods = parse_methods(text)
        by_name = {}
        for i, (name, params, _) in enumerate(methods):
            by_name.setdefault(name, []).append(i)
        resolved = {}

        def resolve(i, seen=()):
            """(logger, message, with params, [values]) for method i."""
            if i in resolved:
                return resolved[i]
            if i in seen:
                return None
            name, params, body = methods[i]
            names = [p[1] for p in params]
            result = None
            call = next(
                (c for c in re.finditer(r"AuditLogger\.(\w+)\(", body)
                 if not re.fullmatch(r"is\w*LoggingEnabled", c.group(1))),
                None,
            )
            if call:
                cstart = call.end() - 1
                args = split_top(body[cstart + 1:matching(body, cstart) - 1])
                target = resolve_static(statics, call.group(1), len(args))
                if target is None:
                    print(f"{file}: no static AuditLogger.{call.group(1)}/{len(args)}", file=sys.stderr)
                else:
                    logger, message, first, varargs, explicit = target
                    values = None
                    if first is not None:
                        shown = args[first:] if varargs else args[first:first + 1]
                        values = [value_of(a, names) for a in shown]
                    result = (logger, message, values, varargs, explicit)
            else:
                # A call to another method of the class: its message, with
                # the values this one gives it.
                for c in re.finditer(r"(?<![\w.])(?:this\.)?(\w+)\(", body):
                    if c.group(1) not in by_name:
                        continue
                    cstart = c.end() - 1
                    args = split_top(body[cstart + 1:matching(body, cstart) - 1])
                    for j in by_name[c.group(1)]:
                        if j == i or len(methods[j][1]) != len(args):
                            continue
                        inner = resolve(j, seen + (i,))
                        if inner is None:
                            continue
                        logger, message, values, varargs, explicit = inner
                        outer = [value_of(a, names) for a in args]
                        if values is not None:
                            values = [outer[v[1]] if v[0] == "param" else v for v in values]
                        result = (logger, message, values, varargs, explicit)
                        break
                    if result:
                        break
            resolved[i] = result
            return result

        for i, (name, params, _) in enumerate(methods):
            r = resolve(i)
            if r is None:
                continue
            logger, (mid, mtext), values, varargs, explicit = r
            entries.append(
                (control, name, params, logger, mid, mtext, values, varargs, explicit)
            )
    return entries


def rust_str(s):
    return '"' + s.replace("\\", "\\\\").replace('"', '\\"') + '"'


def rust_arg(v):
    kind = v[0]
    if kind == "param":
        return f"Arg::Param({v[1]})"
    if kind == "null":
        return "Arg::Null"
    if kind == "text":
        return f"Arg::Text({rust_str(v[1])})"
    # A value the broker computes (a default from the address settings or
    # a constant): shown as the expression that gives it.
    return f"Arg::Computed({rust_str(v[1])})"


def main():
    statics, messages = audit_logger()
    entries = controls(statics)
    out = [
        "// Generated by scripts/gen-audit-catalog.py from Artemis's AuditLogger and",
        "// *ControlImpl classes. Do not edit.",
        "",
        "use super::{Arg, Control, Entry, Logger};",
        "",
        "/// The audit message of each management method.",
        "pub(crate) static CONTROLS: &[Entry] = &[",
    ]
    for control, name, params, logger, mid, mtext, args, varargs, explicit in entries:
        logger = {"BASE": "Base", "RESOURCE": "Resource", "MESSAGE": "Message"}[logger]
        types = ", ".join(rust_str(p[0]) for p in params)
        names = ", ".join(rust_str(p[1]) for p in params)
        out.append("    Entry {")
        out.append(f"        control: Control::{control},")
        out.append(f"        method: {rust_str(name)},")
        out.append(f"        types: &[{types}],")
        out.append(f"        names: &[{names}],")
        out.append(f"        logger: Logger::{logger},")
        out.append(f"        id: {mid},")
        out.append(f"        text: {rust_str(mtext)},")
        if args is not None:
            out.append(f"        params: Some(&[{', '.join(rust_arg(a) for a in args)}]),")
        else:
            out.append("        params: None,")
        out.append(f"        list: {'true' if varargs else 'false'},")
        out.append(f"        anonymous: {'true' if explicit else 'false'},")
        out.append("    },")
    out.append("];")
    out.append("")
    out.append("/// Every audit message by its bundle method's name: its code and text.")
    out.append("pub(crate) static MESSAGES: &[(&str, u32, &str)] = &[")
    seen = set()
    for (name, _), (mid, mtext) in sorted(messages.items(), key=lambda kv: kv[1][0]):
        if name in seen:
            continue
        seen.add(name)
        out.append(f"    ({rust_str(name)}, {mid}, {rust_str(mtext)}),")
    out.append("];")
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(out) + "\n")
    print(f"{len(entries)} entries", file=sys.stderr)


if __name__ == "__main__":
    main()
