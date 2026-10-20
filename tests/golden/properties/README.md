# `artemis properties` outputs

Each `<case>.xml` is a `broker.xml`; `<case>.properties` is the file Artemis
2.57's `artemis properties` wrote for it, run on Windows with `-Xmx1g` from
an instance whose `etc/broker.xml` it was, exactly as written (CRLF line
ends, the lines before a failure for the cases that fail).
`crates/saranga-config/tests/properties_export.rs` exports each case and
compares.

`full-config.xml` is Artemis's `ConfigurationTest-full-config.xml` with the
system properties its test sets substituted; `empty.xml` holds an empty
`<core>`. The others were written for the port, each around a part of the
configuration.

Never edit the `.properties` files by hand. To record one again, copy the
`.xml` into an Artemis 2.57 instance's `etc/broker.xml` and run
`bin/artemis properties --output <case>.properties` with `JAVA_ARGS`
holding `-Xmx1g`.
