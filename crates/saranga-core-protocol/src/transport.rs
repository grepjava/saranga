//! `TransportConfiguration` as it appears inside packets (topology updates,
//! `DisconnectMessage_V3`).

use saranga_buffer::{Codec, DecodeError, JString, Reader, Result, Writer};

const TYPE_BOOLEAN: u8 = 0;
const TYPE_INT: u8 = 1;
const TYPE_LONG: u8 = 2;
const TYPE_STRING: u8 = 3;

/// A connector parameter value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParamValue {
    Bool(bool),
    Int(i32),
    Long(i64),
    String(JString),
}

/// `TransportConfiguration`. Parameters keep their wire order. Keys of
/// "extra properties" keep their `activemq.extraprops.` prefix, exactly as
/// encoded, so `params` alone reproduces the wire form.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TransportConfiguration {
    pub name: JString,
    pub factory_class_name: JString,
    pub params: Vec<(JString, ParamValue)>,
}

impl Codec for TransportConfiguration {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        let name = r.string()?;
        let factory_class_name = r.string()?;
        let count = r.i32()?;
        let mut params = Vec::new();
        for _ in 0..count.max(0) {
            let key = r.string()?;
            let value = match r.u8()? {
                TYPE_BOOLEAN => ParamValue::Bool(r.bool()?),
                TYPE_INT => ParamValue::Int(r.i32()?),
                TYPE_LONG => ParamValue::Long(r.i64()?),
                TYPE_STRING => ParamValue::String(r.string()?),
                t => {
                    return Err(DecodeError::Invalid(format!(
                        "invalid transport param type {t}"
                    )));
                }
            };
            params.push((key, value));
        }
        Ok(Self {
            name,
            factory_class_name,
            params,
        })
    }

    fn write(&self, w: &mut Writer) {
        w.string(&self.name)
            .string(&self.factory_class_name)
            .i32(self.params.len() as i32);
        for (key, value) in &self.params {
            w.string(key);
            match value {
                ParamValue::Bool(v) => w.u8(TYPE_BOOLEAN).bool(*v),
                ParamValue::Int(v) => w.u8(TYPE_INT).i32(*v),
                ParamValue::Long(v) => w.u8(TYPE_LONG).i64(*v),
                ParamValue::String(v) => w.u8(TYPE_STRING).string(v),
            };
        }
    }
}
