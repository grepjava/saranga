/// Declares a packet whose body is a fixed sequence of fields, each with a
/// single [`saranga_buffer::Codec`] encoding, and implements
/// [`crate::PacketCodec`] for it. Packets with version-dependent or optional
/// trailing fields implement `PacketCodec` by hand instead.
///
/// ```ignore
/// wire_packet! {
///     /// `Ping`
///     Ping = types::PING {
///         pub connection_ttl: i64,
///     }
/// }
/// ```
macro_rules! wire_packet {
    (
        $(#[$meta:meta])*
        $name:ident = $ty:expr => {
            $( $(#[$fmeta:meta])* pub $field:ident : $fty:ty ),* $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Default)]
        pub struct $name {
            $( $(#[$fmeta])* pub $field: $fty, )*
        }

        impl $crate::PacketCodec for $name {
            const TYPE: i8 = $ty;

            #[allow(unused_variables)]
            fn decode(
                r: &mut saranga_buffer::Reader<'_>,
                ctx: &$crate::Context,
            ) -> saranga_buffer::Result<Self> {
                Ok(Self {
                    $( $field: <$fty as saranga_buffer::Codec>::read(r)?, )*
                })
            }

            #[allow(unused_variables)]
            fn encode(&self, w: &mut saranga_buffer::Writer, ctx: &$crate::Context) {
                $( saranga_buffer::Codec::write(&self.$field, w); )*
            }
        }
    };
}
