//! A frame nesting structures or throwable causes past `codec::MAX_DEPTH`
//! is refused with an error, as Java's `StackOverflowError` fails the
//! connection, instead of overflowing the stack and ending the process.
//! Frames are read before the connection logs in.

use saranga_protocol_openwire::WireFormat;
use saranga_protocol_openwire::codec::{MAX_CAUSES, MAX_DEPTH, Throwable};
use saranga_protocol_openwire::commands::{DataResponse, DataStructure, ExceptionResponse};

fn format(tight: bool) -> WireFormat {
    let mut wf = WireFormat::new(12);
    wf.tight_encoding_enabled = tight;
    wf.stack_trace_enabled = true;
    wf
}

fn causes(n: usize) -> DataStructure {
    let mut t = Throwable::new("java.lang.Exception", Some("0".into()));
    for i in 1..n {
        let mut outer = Throwable::new("java.lang.Exception", Some(i.to_string()));
        outer.cause = Some(Box::new(t));
        t = outer;
    }
    DataStructure::ExceptionResponse(ExceptionResponse {
        exception: Some(Box::new(t)),
        ..Default::default()
    })
}

fn responses(n: usize) -> DataStructure {
    let mut ds = DataStructure::DataResponse(DataResponse::default());
    for _ in 1..n {
        ds = DataStructure::DataResponse(DataResponse {
            data: Some(Box::new(ds)),
            ..Default::default()
        });
    }
    ds
}

/// Reads `frame` on a thread with the stack the broker's threads have, so
/// that a limit too high for it shows as a crash of the test.
fn read(tight: bool, frame: Vec<u8>) -> Result<(), String> {
    std::thread::Builder::new()
        .stack_size(2 << 20)
        .spawn(move || {
            format(tight)
                .unmarshal(&frame)
                .map(|_| ())
                .map_err(|e| e.to_string())
        })
        .unwrap()
        .join()
        .expect("reading the frame crashed")
}

/// The frame the marshaller writes for `ds`.
fn frame(tight: bool, ds: &DataStructure) -> Vec<u8> {
    format(tight).marshal(Some(ds)).unwrap()
}

#[test]
fn nesting_within_the_limit_is_read() {
    for tight in [true, false] {
        read(tight, frame(tight, &causes(MAX_CAUSES))).unwrap();
        read(tight, frame(tight, &responses(MAX_DEPTH))).unwrap();
    }
}

#[test]
fn nesting_past_the_limit_is_refused() {
    let cases = [
        ("causes", MAX_CAUSES + 2),
        ("causes", 20_000),
        ("responses", MAX_DEPTH + 2),
        ("responses", 1_000),
    ];
    for tight in [true, false] {
        for (what, n) in cases {
            // Writing that deep needs a big stack too, and so does
            // dropping it.
            let bytes = std::thread::Builder::new()
                .stack_size(256 << 20)
                .spawn(move || {
                    let ds = if what == "causes" {
                        causes(n)
                    } else {
                        responses(n)
                    };
                    let f = frame(tight, &ds);
                    std::mem::forget(ds);
                    f
                })
                .unwrap()
                .join()
                .unwrap();
            let e = read(tight, bytes).unwrap_err();
            assert!(
                e.contains("nested more than"),
                "{what} {n} tight={tight}: {e}"
            );
        }
    }
}
