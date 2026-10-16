#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| saranga_fuzz::amqp_value(data));
