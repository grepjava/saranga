# Fuzz regressions

Every input a fuzz target failed on, once the failure is fixed, in
`<target>/`, named after what it found. `cargo test -p saranga-fuzz`
(crates/saranga-fuzz/tests/regressions.rs) runs each through its target.

To keep one, copy the input libFuzzer left in `fuzz/artifacts/<target>/`
here. To run a target on one input:

    cd fuzz && cargo +nightly fuzz run <target> regressions/<target>/<file>
