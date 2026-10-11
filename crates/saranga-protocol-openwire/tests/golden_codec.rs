//! Frames the ActiveMQ 5.19.10 marshallers wrote (tests/golden/openwire/codec,
//! from tests/golden-gen/openwire/CodecFixtures.java), for every version in
//! tight and loose encoding, with and without the cache and stack traces:
//! each decodes to what Java dumped and encodes back to the same bytes.

use std::io::Read;

use saranga_protocol_openwire::WireFormat;
use saranga_protocol_openwire::codec::dump;

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

fn format(version: u32, tight: bool, cache: bool, stack: bool) -> WireFormat {
    let mut wf = WireFormat::new(version);
    wf.tight_encoding_enabled = tight;
    wf.set_cache_enabled(cache);
    wf.stack_trace_enabled = stack;
    wf
}

#[test]
fn every_version_round_trips() {
    let dir = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/golden/openwire/codec"
    );
    let mut frames = 0;
    for version in 1..=12u32 {
        let mut text = String::new();
        flate2::read::GzDecoder::new(
            std::fs::File::open(format!("{dir}/v{version}.txt.gz")).unwrap(),
        )
        .read_to_string(&mut text)
        .unwrap();
        let mut reader = None;
        let mut writer = None;
        let mut stack = false;
        let mut lines = text.lines();
        while let Some(line) = lines.next() {
            if let Some(cfg) = line.strip_prefix("config ") {
                let f: Vec<bool> = cfg.split(' ').map(|x| x == "true").collect();
                stack = f[2];
                reader = Some(format(version, f[0], f[1], f[2]));
                writer = Some(format(version, f[0], f[1], f[2]));
                continue;
            }
            let frame = unhex(line.strip_prefix("frame ").unwrap());
            let want = lines.next().unwrap().strip_prefix("dump ").unwrap();
            let ds = reader
                .as_mut()
                .unwrap()
                .unmarshal(&frame)
                .unwrap_or_else(|e| panic!("v{version}: {e}: {want}"))
                .unwrap();
            assert_eq!(dump(&ds, version, stack), want, "v{version} decode");
            let again = writer.as_mut().unwrap().marshal(Some(&ds)).unwrap();
            assert_eq!(hex(&again), hex(&frame), "v{version} encode: {want}");
            frames += 1;
        }
    }
    assert!(frames > 4000, "{frames}");
}
