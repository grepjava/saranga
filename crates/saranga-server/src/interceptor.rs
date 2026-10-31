//! Interceptors: Artemis's `BaseInterceptor`s as a Rust trait.
//!
//! An interceptor sees each packet of its protocol coming in from a client
//! or going out to one, may change it, and returns false to stop it:
//! Core's packets, STOMP's frames, MQTT's packets, OpenWire's commands and
//! AMQP's messages, each protocol taking the interceptors of its own packet
//! type (`Intercept<P>`). What a refusal does is the protocol's, as in
//! Artemis. An interceptor that fails is logged and the packet goes on to
//! the next.
//!
//! `broker.xml`'s `<remoting-incoming-interceptors>` (and the older
//! `<remoting-interceptors>`) and `<remoting-outgoing-interceptors>` name
//! Java classes: Saranga takes the interceptor the embedding application
//! registered under the class name in `BrokerConfig::interceptors`, and a
//! name with none stops the broker from starting, as a class Artemis cannot
//! load does. An application can also add and remove interceptors on a
//! running broker ([`Interceptors`]), as `RemotingService.add…Interceptor`
//! does.

use std::any::Any;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, RwLock};

use crate::session::ClientConnection;

/// What an interceptor that fails returns (a Java interceptor's exception).
pub type InterceptorError = Box<dyn std::error::Error + Send + Sync>;

/// `BaseInterceptor.intercept` for packets of type `P`: true lets the
/// packet go on, false stops it.
pub trait Intercept<P: ?Sized>: Send + Sync {
    fn intercept(
        &self,
        packet: &mut P,
        connection: &ClientConnection,
    ) -> Result<bool, InterceptorError>;
}

impl<P: ?Sized, F> Intercept<P> for F
where
    F: Fn(&mut P, &ClientConnection) -> Result<bool, InterceptorError> + Send + Sync,
{
    fn intercept(
        &self,
        packet: &mut P,
        connection: &ClientConnection,
    ) -> Result<bool, InterceptorError> {
        self(packet, connection)
    }
}

/// An interceptor: a name, which a refusal is reported by (Artemis's class
/// name), and what it does to each protocol's packets, one protocol or
/// several, as a Java class may implement several protocols' interfaces.
#[derive(Clone)]
pub struct Interceptor {
    name: Arc<str>,
    kinds: Vec<Arc<dyn Any + Send + Sync>>,
}

impl Interceptor {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into().into(),
            kinds: Vec::new(),
        }
    }

    /// Intercepts the packets of type `P`.
    pub fn on<P: ?Sized + 'static>(mut self, intercept: impl Intercept<P> + 'static) -> Self {
        let erased: Arc<dyn Intercept<P>> = Arc::new(intercept);
        self.kinds.push(Arc::new(erased));
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// What it does to packets of type `P`, if anything.
    fn of<P: ?Sized + 'static>(&self) -> Option<&Arc<dyn Intercept<P>>> {
        self.kinds
            .iter()
            .find_map(|k| k.downcast_ref::<Arc<dyn Intercept<P>>>())
    }

    /// Whether it intercepts packets of type `P`.
    pub fn intercepts<P: ?Sized + 'static>(&self) -> bool {
        self.of::<P>().is_some()
    }
}

/// Makes the interceptor a class name stands for.
pub type InterceptorFactory = Arc<dyn Fn() -> Interceptor + Send + Sync>;

type List = RwLock<Arc<[Interceptor]>>;

/// The broker's incoming and outgoing interceptors
/// (`RemotingServiceImpl.incomingInterceptors`, `outgoingInterceptors`):
/// every acceptor's connections use them, and a change applies to the next
/// packet.
#[derive(Default)]
pub struct Interceptors {
    incoming: List,
    outgoing: List,
}

fn read(list: &List) -> Arc<[Interceptor]> {
    list.read().unwrap_or_else(|e| e.into_inner()).clone()
}

fn update(list: &List, f: impl FnOnce(&mut Vec<Interceptor>) -> bool) -> bool {
    let mut guard = list.write().unwrap_or_else(|e| e.into_inner());
    let mut v = guard.to_vec();
    let changed = f(&mut v);
    *guard = Arc::from(v);
    changed
}

impl Interceptors {
    pub fn new(incoming: Vec<Interceptor>, outgoing: Vec<Interceptor>) -> Self {
        Self {
            incoming: RwLock::new(Arc::from(incoming)),
            outgoing: RwLock::new(Arc::from(outgoing)),
        }
    }

    /// `addIncomingInterceptor`: after those there are.
    pub fn add_incoming(&self, interceptor: Interceptor) {
        update(&self.incoming, |v| {
            v.push(interceptor);
            true
        });
    }

    /// `addOutgoingInterceptor`.
    pub fn add_outgoing(&self, interceptor: Interceptor) {
        update(&self.outgoing, |v| {
            v.push(interceptor);
            true
        });
    }

    /// `removeIncomingInterceptor`: the first of the name; false if there
    /// is none.
    pub fn remove_incoming(&self, name: &str) -> bool {
        update(&self.incoming, |v| remove(v, name))
    }

    /// `removeOutgoingInterceptor`.
    pub fn remove_outgoing(&self, name: &str) -> bool {
        update(&self.outgoing, |v| remove(v, name))
    }

    /// `clearInterceptors`.
    pub fn clear(&self) {
        update(&self.incoming, |v| {
            v.clear();
            true
        });
        update(&self.outgoing, |v| {
            v.clear();
            true
        });
    }

    pub fn incoming(&self) -> Arc<[Interceptor]> {
        read(&self.incoming)
    }

    pub fn outgoing(&self) -> Arc<[Interceptor]> {
        read(&self.outgoing)
    }

    /// Whether any outgoing interceptor takes packets of type `P`, for a
    /// protocol that writes its packets as bytes to make them only then.
    pub fn intercepts_outgoing<P: ?Sized + 'static>(&self) -> bool {
        self.outgoing
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .any(Interceptor::intercepts::<P>)
    }

    /// Whether any incoming interceptor takes packets of type `P`.
    pub fn intercepts_incoming<P: ?Sized + 'static>(&self) -> bool {
        self.incoming
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .any(Interceptor::intercepts::<P>)
    }

    /// `invokeInterceptors` with the incoming ones: the name of the one
    /// that stopped the packet, or `None`.
    pub fn invoke_incoming<P: ?Sized + 'static>(
        &self,
        packet: &mut P,
        connection: &ClientConnection,
        log: Log,
    ) -> Option<Arc<str>> {
        invoke(&self.incoming, packet, connection, log)
    }

    /// `invokeInterceptors` with the outgoing ones.
    pub fn invoke_outgoing<P: ?Sized + 'static>(
        &self,
        packet: &mut P,
        connection: &ClientConnection,
        log: Log,
    ) -> Option<Arc<str>> {
        invoke(&self.outgoing, packet, connection, log)
    }
}

fn remove(v: &mut Vec<Interceptor>, name: &str) -> bool {
    match v.iter().position(|i| &*i.name == name) {
        Some(at) => {
            v.remove(at);
            true
        }
        None => false,
    }
}

/// How an interceptor's failure is logged: Core's `ChannelImpl` logs it as
/// the client does, the other protocols' managers as the server does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Log {
    /// `ActiveMQClientLogger.errorCallingInterceptor` (AMQ212038).
    Core,
    /// `ActiveMQServerLogger.failedToInvokeAnInterceptor` (AMQ224082).
    Server,
}

fn invoke<P: ?Sized + 'static>(
    list: &List,
    packet: &mut P,
    connection: &ClientConnection,
    log: Log,
) -> Option<Arc<str>> {
    let interceptors = read(list);
    for interceptor in interceptors.iter() {
        let Some(intercept) = interceptor.of::<P>() else {
            continue;
        };
        let result = catch_unwind(AssertUnwindSafe(|| intercept.intercept(packet, connection)))
            .unwrap_or_else(|panic| {
                let text = panic
                    .downcast_ref::<&str>()
                    .map(|s| (*s).to_owned())
                    .or_else(|| panic.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "panicked".to_owned());
                Err(text.into())
            });
        match result {
            Ok(true) => {}
            Ok(false) => return Some(interceptor.name.clone()),
            Err(e) => match log {
                Log::Core => tracing::warn!(
                    "AMQ212038: Failure in calling interceptor: {}: {e}",
                    interceptor.name
                ),
                Log::Server => tracing::error!("AMQ224082: Failed to invoke an interceptor: {e}"),
            },
        }
    }
    None
}

/// The interceptors class names stand for (`instantiateInterceptors`): one
/// not registered stops the broker, as a class Artemis cannot load does.
pub fn resolve(
    names: &[String],
    factories: &std::collections::HashMap<String, InterceptorFactory>,
    what: &str,
) -> Result<Vec<Interceptor>, String> {
    names
        .iter()
        .map(|name| match factories.get(name) {
            Some(make) => Ok(make()),
            None => Err(format!(
                "{what}: {name}: no interceptor is registered under this class name, and Saranga cannot load Java classes"
            )),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::Transport;

    struct Packet(String);
    struct Other;

    fn connection() -> ClientConnection {
        ClientConnection::new(Transport::management(), Arc::default(), Vec::new())
    }

    #[test]
    fn each_protocol_takes_its_own_and_the_first_refusal_stops() {
        let first = Interceptor::new("a.First").on(|p: &mut Packet, _: &ClientConnection| {
            p.0.push('1');
            Ok(true)
        });
        let other = Interceptor::new("a.Other").on(|_: &mut Other, _: &ClientConnection| Ok(false));
        let failing = Interceptor::new("a.Failing")
            .on(|_: &mut Packet, _: &ClientConnection| Err("boom".into()));
        let refusing = Interceptor::new("a.Refusing").on(|p: &mut Packet, _: &ClientConnection| {
            p.0.push('2');
            Ok(p.0 != "x12")
        });
        let last = Interceptor::new("a.Last").on(|p: &mut Packet, _: &ClientConnection| {
            p.0.push('3');
            Ok(true)
        });
        let i = Interceptors::new(vec![first, other, failing, refusing, last], Vec::new());
        let c = connection();
        let mut p = Packet("y".into());
        assert_eq!(i.invoke_incoming(&mut p, &c, Log::Server), None);
        assert_eq!(p.0, "y123");
        let mut p = Packet("x".into());
        assert_eq!(
            i.invoke_incoming(&mut p, &c, Log::Server).as_deref(),
            Some("a.Refusing")
        );
        assert_eq!(p.0, "x12");
        assert!(i.intercepts_incoming::<Other>());
        assert!(!i.intercepts_outgoing::<Packet>());
    }

    #[test]
    fn added_and_removed_by_name() {
        let i = Interceptors::default();
        i.add_outgoing(Interceptor::new("n").on(|_: &mut Packet, _: &ClientConnection| Ok(false)));
        let c = connection();
        assert!(
            i.invoke_outgoing(&mut Packet(String::new()), &c, Log::Core)
                .is_some()
        );
        assert!(i.remove_outgoing("n"));
        assert!(!i.remove_outgoing("n"));
        assert!(
            i.invoke_outgoing(&mut Packet(String::new()), &c, Log::Core)
                .is_none()
        );
    }

    #[test]
    fn a_panic_is_a_failure() {
        let i = Interceptors::new(
            vec![Interceptor::new("p").on(
                |_: &mut Packet, _: &ClientConnection| -> Result<bool, InterceptorError> {
                    panic!("no")
                },
            )],
            Vec::new(),
        );
        assert!(
            i.invoke_incoming(&mut Packet(String::new()), &connection(), Log::Server)
                .is_none()
        );
    }

    #[test]
    fn unregistered_names_are_refused() {
        let mut f = std::collections::HashMap::new();
        f.insert(
            "a.B".to_owned(),
            Arc::new(|| Interceptor::new("a.B")) as InterceptorFactory,
        );
        assert_eq!(resolve(&["a.B".into()], &f, "x").unwrap()[0].name(), "a.B");
        assert!(resolve(&["a.C".into()], &f, "x").is_err());
    }
}
