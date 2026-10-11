"""Extracts the OpenWire marshaller schemas (v1..v12) into schema.json.

The input is the ActiveMQ 5.19.10 activemq-client and activemq-openwire-legacy
jars (the ones Artemis 2.57.0 ships), decompiled with Fernflower and unzipped
as <root>/client/x and <root>/legacy/x:

  java -cp java-decompiler-engine.jar       org.jetbrains.java.decompiler.main.decompiler.ConsoleDecompiler <jar> <dir>

  python extract.py <root> schema.json

Each type gets, per version, its parent marshaller, its data structure type
and the fields its looseMarshal writes, in order, with their encoding and Java
type. generate.py turns schema.json into Rust."""
import json, os, re, sys

ROOT = sys.argv[1]  # .../ow-src
OUT = sys.argv[2]
DIRS = [os.path.join(ROOT, 'client', 'x', 'org', 'apache', 'activemq', 'openwire'),
        os.path.join(ROOT, 'legacy', 'x', 'org', 'apache', 'activemq', 'openwire')]
CMD = os.path.join(ROOT, 'client', 'x', 'org', 'apache', 'activemq', 'command')

KINDS = [
    (r'this\.looseMarshalCachedObject\(wireFormat, (?:\(\w+\))?info\.(?:get|is)(\w+)\(\), dataOut\)', 'cached'),
    (r'this\.looseMarshalNestedObject\(wireFormat, (?:\(\w+\))?info\.(?:get|is)(\w+)\(\), dataOut\)', 'nested'),
    (r'this\.looseMarshalObjectArray\(wireFormat, (?:\(\w+\[\]\))?info\.(?:get|is)(\w+)\(\), dataOut\)', 'array'),
    (r'this\.looseMarshalString\(info\.(?:get|is)(\w+)\(\), dataOut\)', 'string'),
    (r'this\.looseMarshalLong\(wireFormat, info\.(?:get|is)(\w+)\(\), dataOut\)', 'long'),
    (r'this\.looseMarshalByteArray\(wireFormat, info\.(?:get|is)(\w+)\(\), dataOut\)', 'bytes'),
    (r'this\.looseMarshalByteSequence\(wireFormat, info\.(?:get|is)(\w+)\(\), dataOut\)', 'byteseq'),
    (r'this\.looseMarshalConstByteArray\(wireFormat, info\.(?:get|is)(\w+)\(\), dataOut, (\d+)\)', 'constbytes'),
    (r'this\.looseMarshalThrowable\(wireFormat, info\.(?:get|is)(\w+)\(\), dataOut\)', 'throwable'),
    (r'dataOut\.writeBoolean\(info\.(?:get|is)(\w+)\(\)\)', 'bool'),
    (r'dataOut\.writeByte\(info\.(?:get|is)(\w+)\(\)\)', 'byte'),
    (r'dataOut\.writeShort\(info\.(?:get|is)(\w+)\(\)\)', 'short'),
    (r'dataOut\.writeInt\(info\.(?:get|is)(\w+)\(\)\)', 'int'),
]
IGNORE = re.compile(r'^(super\.looseMarshal|\w+ info = |info\.(before|after)\w*\(wireFormat\);|$)')


def body(src, sig):
    i = src.find(sig)
    if i < 0:
        return None
    j = src.index('{', i) + 1
    depth, k = 1, j
    while depth:
        if src[k] == '{':
            depth += 1
        elif src[k] == '}':
            depth -= 1
        k += 1
    return src[j:k - 1]


def getter_types():
    types = {}
    for f in os.listdir(CMD):
        if not f.endswith('.java'):
            continue
        src = open(os.path.join(CMD, f), encoding='utf-8').read()
        m = re.search(r'class (\w+)(?:<[^>]*>)?(?: extends (\w+))?', src)
        if not m:
            continue
        g = {}
        for gm in re.finditer(r'public (?:synchronized )?(?:final )?([\w.\[\]<>]+) (?:get|is)(\w+)\(\)', src):
            g.setdefault(gm.group(2), gm.group(1))
        types[m.group(1)] = {'extends': m.group(2), 'getters': g,
                             'marshall_aware': 'isMarshallAware()' in src and re.search(r'isMarshallAware\(\) \{\s*return true', src) is not None}
    return types


def main():
    gt = getter_types()
    schema = {}
    for d in DIRS:
        for v in os.listdir(d):
            if not re.fullmatch(r'v\d+', v):
                continue
            ver = int(v[1:])
            for f in sorted(os.listdir(os.path.join(d, v))):
                if not f.endswith('Marshaller.java') or f in ('BaseDataStreamMarshaller.java',):
                    continue
                src = open(os.path.join(d, v, f), encoding='utf-8').read()
                cls = f[:-len('Marshaller.java')]
                m = re.search(r'class \w+ extends (\w+)', src)
                parent = m.group(1)[:-len('Marshaller')] if m else None
                if parent == 'BaseDataStream':
                    parent = None
                t = re.search(r'getDataStructureType\(\) \{\s*return (-?\d+);', src)
                b = body(src, 'public void looseMarshal(')
                fields = []
                if b is not None:
                    for stmt in [s.strip() for s in b.split(';')]:
                        stmt2 = stmt + ';'
                        if IGNORE.match(stmt) or IGNORE.match(stmt2):
                            continue
                        for pat, kind in KINDS:
                            mm = re.fullmatch(pat, stmt)
                            if mm:
                                fd = {'name': mm.group(1), 'kind': kind}
                                if kind == 'constbytes':
                                    fd['size'] = int(mm.group(2))
                                fields.append(fd)
                                break
                        else:
                            raise SystemExit(f'{v}/{f}: unknown statement {stmt!r}')
                schema.setdefault(cls, {})[ver] = {
                    'parent': parent, 'type': int(t.group(1)) if t else None,
                    'abstract': 'abstract class' in src, 'fields': fields}
    def java_type(cls, name):
        while cls:
            t = gt.get(cls, {}).get('getters', {}).get(name)
            if t:
                return t
            cls = gt.get(cls, {}).get('extends')
        raise SystemExit(f'no getter for {name}')
    for cls, versions in schema.items():
        for s in versions.values():
            for f in s['fields']:
                f['java'] = java_type(cls, f['name'])
    with open(OUT, 'w', newline=chr(10)) as out:
        # One line per type and version, so changes diff well.
        out.write('{' + chr(10))
        rows = []
        for cls in sorted(schema):
            for v in sorted(schema[cls], key=int):
                rows.append(' ' + json.dumps(f'{cls}/{v}') + ': ' + json.dumps(schema[cls][v], sort_keys=True))
        out.write((',' + chr(10)).join(rows) + chr(10) + '}' + chr(10))


main()
