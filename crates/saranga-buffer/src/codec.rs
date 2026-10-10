use crate::{JString, Reader, Result, SimpleString, Writer};

/// A value with one fixed wire encoding. `Option<T>` is the Artemis nullable
/// form (a NULL / NOT_NULL marker byte, then the value).
pub trait Codec: Sized {
    fn read(r: &mut Reader<'_>) -> Result<Self>;
    fn write(&self, w: &mut Writer);
}

macro_rules! codec_by_copy {
    ($($ty:ty => $m:ident),* $(,)?) => {$(
        impl Codec for $ty {
            fn read(r: &mut Reader<'_>) -> Result<Self> {
                r.$m()
            }
            fn write(&self, w: &mut Writer) {
                w.$m(*self);
            }
        }
    )*};
}

codec_by_copy!(u8 => u8, i8 => i8, bool => bool, i16 => i16, u16 => u16, i32 => i32, i64 => i64, f32 => f32, f64 => f64);

impl Codec for SimpleString {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        r.simple_string()
    }
    fn write(&self, w: &mut Writer) {
        w.simple_string(self);
    }
}

impl Codec for JString {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        r.string()
    }
    fn write(&self, w: &mut Writer) {
        w.string(self);
    }
}

impl<T: Codec> Codec for Option<T> {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        r.nullable(T::read)
    }
    fn write(&self, w: &mut Writer) {
        w.nullable(self.as_ref(), |w, v| {
            v.write(w);
            w
        });
    }
}

/// An i32-length-prefixed byte array.
impl Codec for Vec<u8> {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        r.byte_array()
    }
    fn write(&self, w: &mut Writer) {
        w.byte_array(self);
    }
}

impl Codec for crate::TypedProperties {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        Self::decode(r)
    }
    fn write(&self, w: &mut Writer) {
        self.encode(w);
    }
}
