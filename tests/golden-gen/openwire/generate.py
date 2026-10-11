"""Generates the OpenWire command structs and their marshalling from
schema.json (see extract.py):

  python generate.py schema.json ../../../crates/saranga-protocol-openwire/src/commands.rs fields.txt

commands.rs holds a struct for each data structure (every message class is one
Message, every destination class one Destination), each with the fields of
every version, and visits the fields a version marshals in that version's
order. fields.txt lists the same per Java class, for Fixtures.java."""
import json, re, sys

schema_path, rust_path, fields_path = sys.argv[1:4]
raw = json.load(open(schema_path))
schema = {}
for key, s in raw.items():
    cls, v = key.split('/')
    schema.setdefault(cls, {})[int(v)] = s

MESSAGE_TYPES = {23: 'ActiveMQMessage', 24: 'ActiveMQBytesMessage', 25: 'ActiveMQMapMessage',
                 26: 'ActiveMQObjectMessage', 27: 'ActiveMQStreamMessage', 28: 'ActiveMQTextMessage',
                 29: 'ActiveMQBlobMessage'}
DEST_TYPES = {100: 'ActiveMQQueue', 101: 'ActiveMQTopic', 102: 'ActiveMQTempQueue', 103: 'ActiveMQTempTopic'}
RUST_NAME = {'XATransactionId': 'XaTransactionId'}
# Getters that are other names for a field.
ALIAS = {'SubcriptionName': 'SubscriptionName'}
KEYWORDS = {'type': 'type_'}


def snake(name):
    name = ALIAS.get(name, name)
    s = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', name)
    s = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s).lower()
    return KEYWORDS.get(s, s)


def obj_type(java):
    return {'ActiveMQDestination': 'Destination', 'TransactionId': 'TransactionId',
            'Message': 'Box<Message>', 'MessageAck': 'Box<MessageAck>',
            'DataStructure': 'Box<DataStructure>', 'BooleanExpression': 'Box<DataStructure>',
            }.get(java, RUST_NAME.get(java, java))


def elem_type(java):
    return {'DataStructure': 'DataStructure'}.get(java, RUST_NAME.get(java, java))


def rust_type(f):
    k, j = f['kind'], f['java']
    return {
        'bool': 'bool', 'byte': 'i8', 'short': 'i16', 'int': 'i32', 'long': 'i64',
        'string': 'Option<String>', 'bytes': 'Option<Vec<u8>>', 'byteseq': 'Option<Vec<u8>>',
        'constbytes': 'Vec<u8>', 'throwable': 'Option<Box<Throwable>>',
    }.get(k) or ({'nested': f'Option<{obj_type(j)}>', 'cached': f'Option<{obj_type(j)}>'}.get(k)
                 or f'Option<Vec<{elem_type(j[:-2])}>>')


def chain(cls, v):
    """The classes whose fields a version of cls marshals, root first."""
    out = []
    while cls:
        out.append(cls)
        cls = schema[cls][v]['parent']
    return out[::-1]


def fields_of(cls, v):
    return [f for c in chain(cls, v) for f in schema[c][v]['fields']]


def write_stmt(f):
    k, n = f['kind'], f['name']
    if n == 'NetworkTTL':
        return 'w.int("NetworkTTL", self.message_ttl)?;'
    fld = 'self.' + snake(n)
    return {
        'bool': f'w.bool("{n}", {fld})?;', 'byte': f'w.byte("{n}", {fld})?;',
        'short': f'w.short("{n}", {fld})?;', 'int': f'w.int("{n}", {fld})?;',
        'long': f'w.long("{n}", {fld})?;', 'string': f'w.string("{n}", {fld}.as_deref())?;',
        'bytes': f'w.bytes("{n}", {fld}.as_deref())?;', 'byteseq': f'w.bytes("{n}", {fld}.as_deref())?;',
        'constbytes': f'w.const_bytes("{n}", &{fld}, {f.get("size")})?;',
        'throwable': f'w.throwable("{n}", {fld}.as_deref())?;',
        'nested': f'w.nested("{n}", {fld}.as_ref())?;', 'cached': f'w.cached("{n}", {fld}.as_ref())?;',
        'array': f'w.array("{n}", {fld}.as_deref())?;',
    }[k]


def read_stmt(f):
    k, n = f['kind'], f['name']
    if n == 'NetworkTTL':
        return 'self.message_ttl = r.int()?;\n                self.consumer_ttl = self.message_ttl;'
    fld = 'self.' + snake(n)
    call = {
        'bool': 'r.bool()', 'byte': 'r.byte()', 'short': 'r.short()', 'int': 'r.int()',
        'long': 'r.long()', 'string': 'r.string()', 'bytes': 'r.bytes()', 'byteseq': 'r.bytes()',
        'constbytes': f'r.const_bytes({f.get("size")})', 'throwable': 'r.throwable()',
        'nested': 'r.nested()', 'cached': 'r.cached()', 'array': 'r.array()',
    }[k]
    return f'{fld} = {call}?;'


def version_arms(cls_versions, stmt, indent):
    """match arms over versions grouped by identical field lists."""
    groups = []
    for v, fs in cls_versions:
        key = [(f['name'], f['kind']) for f in fs]
        if groups and groups[-1][1] == key:
            groups[-1][0].append(v)
        else:
            groups.append(([v], key, fs))
    pad = ' ' * indent
    if len(groups) == 1:
        body = [stmt(f) for f in groups[0][2]]
        return [pad + b for b in body]
    lines = [pad + 'match version {']
    for i, (vs, _, fs) in enumerate(groups):
        pat = '_' if i == len(groups) - 1 else (f'{vs[0]}' if len(vs) == 1 else f'{vs[0]}..={vs[-1]}')
        lines.append(pad + f'    {pat} => {{')
        lines += [pad + '        ' + stmt(f) for f in fs]
        lines.append(pad + '    }')
    lines.append(pad + '}')
    return lines


concrete = {}  # rust struct -> {versions, java classes, type ids}
for cls, vs in schema.items():
    for v, s in vs.items():
        if s['abstract'] or s['type'] is None:
            continue
        t = s['type']
        name = 'Message' if t in MESSAGE_TYPES else 'Destination' if t in DEST_TYPES else RUST_NAME.get(cls, cls)
        c = concrete.setdefault(name, {'types': {}, 'versions': set()})
        c['types'][t] = cls
        c['versions'].add(v)

out = []
emit = out.append
emit('// Generated by tests/golden-gen/openwire/generate.py from schema.json; do not edit.')
emit('//')
emit('// The data structures of OpenWire versions 1 to 12, as the ActiveMQ 5.19.10')
emit('// marshallers (org.apache.activemq.openwire.v*) write them.')
emit('')
emit('#![allow(clippy::all)]')
emit('')
emit('use crate::codec::{CodecError, Reader, Structure, Throwable, TransactionId, Writer};')
emit('')

for name in sorted(concrete):
    c = concrete[name]
    rep_cls = c['types'][min(c['types'])]
    # every field of every version (and of every class folded into the struct)
    seen, struct_fields = set(), []
    for cls in sorted(set(c['types'].values()), key=lambda x: min(t for t, k in c['types'].items() if k == x)):
        for v in sorted(schema[cls]):
            for f in fields_of(cls, v):
                fname = 'message_ttl' if f['name'] == 'NetworkTTL' else snake(f['name'])
                if f['name'] == 'NetworkTTL':
                    continue
                if fname not in seen:
                    seen.add(fname)
                    struct_fields.append((fname, rust_type(f)))
    emit('#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]')
    emit(f'pub struct {name} {{')
    if len(c['types']) > 1:
        emit('    pub data_type: u8,')
    for fname, ty in struct_fields:
        emit(f'    pub {fname}: {ty},')
    emit('}')
    emit('')
    emit(f'impl {name} {{')
    emit('    pub fn write_fields<W: Writer>(&self, version: u32, w: &mut W) -> Result<(), CodecError> {')
    if name == 'Message':
        base = [(v, fields_of('ActiveMQMessage', v)) for v in sorted(schema['ActiveMQMessage'])]
        out.extend(version_arms(base, write_stmt, 8))
        emit('        if self.data_type == 29 {')
        blob = [(v, schema['ActiveMQBlobMessage'][v]['fields']) for v in sorted(schema['ActiveMQBlobMessage'])]
        out.extend(version_arms(blob, write_stmt, 12))
        emit('        }')
    else:
        out.extend(version_arms([(v, fields_of(rep_cls, v)) for v in sorted(schema[rep_cls])], write_stmt, 8))
    emit('        let _ = (version, w);')
    emit('        Ok(())')
    emit('    }')
    emit('')
    emit('    pub fn read_fields<R: Reader>(&mut self, version: u32, r: &mut R) -> Result<(), CodecError> {')
    if name == 'Message':
        out.extend(version_arms(base, read_stmt, 8))
        emit('        if self.data_type == 29 {')
        out.extend(version_arms(blob, read_stmt, 12))
        emit('        }')
    else:
        out.extend(version_arms([(v, fields_of(rep_cls, v)) for v in sorted(schema[rep_cls])], read_stmt, 8))
    emit('        let _ = (version, r);')
    emit('        Ok(())')
    emit('    }')
    emit('}')
    emit('')
    # Structure
    emit(f'impl Structure for {name} {{')
    if len(c['types']) > 1:
        emit('    fn data_type(&self) -> u8 {')
        emit('        self.data_type')
        emit('    }')
    else:
        emit('    fn data_type(&self) -> u8 {')
        emit(f'        {min(c["types"])}')
        emit('    }')
    emit('')
    emit('    fn write_fields<W: Writer>(&self, version: u32, w: &mut W) -> Result<(), CodecError> {')
    emit(f'        {name}::write_fields(self, version, w)')
    emit('    }')
    emit('')
    emit('    fn from_ds(ds: DataStructure) -> Result<Self, CodecError> {')
    emit('        match ds {')
    emit(f'            DataStructure::{name}(v) => Ok(v),')
    emit(f'            other => Err(CodecError::class_cast(other.class_name(), "{name}")),')
    emit('        }')
    emit('    }')
    emit('')
    emit('    fn to_ds(&self) -> DataStructure {')
    emit(f'        DataStructure::{name}(self.clone())')
    emit('    }')
    emit('}')
    emit('')

# DataStructure
names = sorted(concrete)
emit('/// Any OpenWire data structure.')
emit('#[derive(Debug, Clone, PartialEq, Eq, Hash)]')
emit('pub enum DataStructure {')
for n in names:
    emit(f'    {n}({n}),')
emit('}')
emit('')
emit('impl DataStructure {')
emit('    /// Reads the fields of a structure of a type this version knows.')
emit('    pub fn read_body<R: Reader>(data_type: u8, version: u32, r: &mut R) -> Result<Self, CodecError> {')
emit('        if !known_type(version, data_type) {')
emit('            return Err(CodecError::UnknownType(data_type as i8));')
emit('        }')
emit('        Ok(match data_type {')
for n in names:
    ts = sorted(concrete[n]['types'])
    pat = ' | '.join(str(t) for t in ts)
    if len(ts) > 1:
        emit(f'            {pat} => {{')
        emit(f'                let mut v = {n} {{ data_type, ..Default::default() }};')
    else:
        emit(f'            {pat} => {{')
        emit(f'                let mut v = {n}::default();')
    emit('                v.read_fields(version, r)?;')
    emit(f'                DataStructure::{n}(v)')
    emit('            }')
emit('            _ => return Err(CodecError::UnknownType(data_type as i8)),')
emit('        })')
emit('    }')
emit('')
emit('    pub fn data_type(&self) -> u8 {')
emit('        match self {')
for n in names:
    emit(f'            DataStructure::{n}(v) => v.data_type(),')
emit('        }')
emit('    }')
emit('')
emit('    pub fn write_fields<W: Writer>(&self, version: u32, w: &mut W) -> Result<(), CodecError> {')
emit('        match self {')
for n in names:
    emit(f'            DataStructure::{n}(v) => v.write_fields(version, w),')
emit('        }')
emit('    }')
emit('')
emit('    /// The simple name of the Java class.')
emit('    pub fn class_name(&self) -> &\'static str {')
emit('        class_name(self.data_type())')
emit('    }')
emit('}')
emit('')
emit('/// The simple name of the Java class of a data structure type.')
emit('pub fn class_name(data_type: u8) -> &\'static str {')
emit('    match data_type {')
allt = sorted((t, cls) for n in names for t, cls in concrete[n]['types'].items())
for t, cls in allt:
    emit(f'        {t} => "{cls}",')
emit('        _ => "DataStructure",')
emit('    }')
emit('}')
emit('')
emit('/// Whether a version has a marshaller for a data structure type.')
emit('pub fn known_type(version: u32, data_type: u8) -> bool {')
emit('    match data_type {')
first = {}
for n in names:
    for t, cls in concrete[n]['types'].items():
        vs = sorted(schema[cls])
        assert vs == list(range(vs[0], 13)), (cls, vs)
        first.setdefault(vs[0], []).append(t)
for v0 in sorted(first):
    pat = ' | '.join(str(t) for t in sorted(first[v0]))
    emit(f'        {pat} => (1..=12).contains(&version)' + (f' && version >= {v0}' if v0 > 1 else '') + ',')
emit('        _ => false,')
emit('    }')
emit('}')
emit('')
open(rust_path, 'w', encoding='utf-8', newline='\n').write('\n'.join(out))

# fields.txt for the Java oracle: <class> <version> <Name>...
with open(fields_path, 'w', newline='\n') as fo:
    fo.write('# Generated by generate.py: the fields each concrete class marshals, per version.\n')
    for n in names:
        for t, cls in sorted(concrete[n]['types'].items()):
            for v in sorted(schema[cls]):
                fo.write(f'{cls} {t} {v} ' + ' '.join(f['name'] for f in fields_of(cls, v)) + '\n')
