//! Acceptor URI parsing. The first three tests are ported from
//! `AcceptorParserTest`.

use saranga_config::AcceptorConfig;

fn parse(uri: &str) -> AcceptorConfig {
    AcceptorConfig::parse(Some("test"), uri).unwrap()
}

#[test]
fn acceptor() {
    let a = parse(
        "tcp://localhost:8080?tcpSendBufferSize=1048576&tcpReceiveBufferSize=1048576&protocols=openwire&banana=x",
    );
    assert_eq!(a.scheme, "tcp");
    assert_eq!(a.host.as_deref(), Some("localhost"));
    assert_eq!(a.port, Some(8080));
    assert_eq!(a.param("banana"), Some("x"));
    assert_eq!(a.protocols(), ["openwire"]);
}

#[test]
fn acceptor_shutdown_timeout() {
    let a = parse("tcp://localhost:8080?quietPeriod=33;shutdownTimeout=55");
    assert_eq!(a.param("quietPeriod"), Some("33"));
    assert_eq!(a.param("shutdownTimeout"), Some("55"));
}

#[test]
fn acceptor_with_query_param_escapes() {
    let a = parse(
        "tcp://0.0.0.0:5672?tcpSendBufferSize=1048576;tcpReceiveBufferSize=1048576;virtualTopicConsumerWildcards=Consumer.*.%3E%3B2",
    );
    assert_eq!(
        a.param("virtualTopicConsumerWildcards"),
        Some("Consumer.*.>;2")
    );
}

#[test]
fn whitespace_is_removed_and_params_keep_order() {
    let a = parse("\n   tcp://0.0.0.0:61616?a=1;\n  b=2&c=3\n");
    let keys: Vec<&str> = a.params.iter().map(|(k, _)| k.as_str()).collect();
    assert_eq!(keys, ["a", "b", "c"]);
    assert_eq!(a.uri, "\n   tcp://0.0.0.0:61616?a=1;\n  b=2&c=3\n");
}

#[test]
fn protocols_split_on_commas() {
    let a =
        parse("tcp://0.0.0.0:61616?protocols=CORE,AMQP,STOMP,HORNETQ,MQTT,OPENWIRE;useEpoll=true");
    assert_eq!(
        a.protocols(),
        ["CORE", "AMQP", "STOMP", "HORNETQ", "MQTT", "OPENWIRE"]
    );
    // The deprecated `protocol` comes first; duplicates are dropped; trailing
    // empties disappear as with `String.split`.
    let a = parse("tcp://h:1?protocol=MQTT;protocols=CORE,MQTT,");
    assert_eq!(a.protocols(), ["MQTT", "CORE"]);
    assert!(parse("tcp://h:1").protocols().is_empty());
}

#[test]
fn repeated_keys_last_value_wins() {
    let a = parse("tcp://h:1?x=1;y=2;x=3");
    assert_eq!(
        a.params,
        [
            ("x".into(), Some("3".into())),
            ("y".into(), Some("2".into()))
        ]
    );
}

#[test]
fn valueless_and_plus_decoding() {
    let a = parse("tcp://h:1?flag;v=a+b%2Bc");
    assert_eq!(a.params[0], ("flag".into(), None));
    // `getQuery` decodes %2B to `+`, then `URLDecoder` turns both into spaces.
    assert_eq!(a.param("v"), Some("a b c"));
}

#[test]
fn hosts() {
    assert_eq!(parse("tcp://[::1]:61616").host.as_deref(), Some("[::1]"));
    assert_eq!(parse("tcp://[::1]:61616").port, Some(61616));
    // `java.net.URI` reports no host for names it cannot parse as a server.
    let a = parse("tcp://my_host:61616");
    assert_eq!((a.host, a.port), (None, None));
    let a = parse("tcp://localhost");
    assert_eq!((a.host.as_deref(), a.port), (Some("localhost"), None));
    assert_eq!(parse("vm://0").host.as_deref(), Some("0"));
}

#[test]
fn multi_uri_form_uses_the_first() {
    let a = parse("(tcp://a:1,tcp://b:2)?x=1");
    assert_eq!(a.host.as_deref(), Some("a"));
    assert_eq!(a.port, Some(1));
    assert_eq!(a.param("x"), Some("1"));
}

#[test]
fn name_param() {
    let a = AcceptorConfig::parse(None, "tcp://h:1?name=fromQuery").unwrap();
    assert_eq!(a.name.as_deref(), Some("fromQuery"));
    assert!(AcceptorConfig::parse(Some("other"), "tcp://h:1?name=fromQuery").is_err());
    assert!(AcceptorConfig::parse(Some("same"), "tcp://h:1?name=same").is_ok());
}

#[test]
fn errors() {
    assert!(AcceptorConfig::parse(None, "ws://h:1").is_err());
    assert!(AcceptorConfig::parse(None, "TCP://h:1").is_err());
    assert!(AcceptorConfig::parse(None, "localhost").is_err());
    assert!(AcceptorConfig::parse(None, "tcp://h:99999").is_err());
    assert!(AcceptorConfig::parse(None, "tcp://h:1?x=%zz").is_err());
}
