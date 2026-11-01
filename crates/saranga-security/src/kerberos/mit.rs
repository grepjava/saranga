//! MIT Kerberos, loaded with `dlopen` (`LoadLibrary` for MIT Kerberos for
//! Windows): `libgssapi_krb5` for GSS-API, `libkrb5` for initial tickets.
//! Credentials go through the cred store extension
//! (`gss_acquire_cred_from`, MIT 1.11), so each subject keeps its own:
//! a TGT is held in a `MEMORY` ticket cache of its own, destroyed with it.
//!
//! The structures are MIT's: `gssapi.h` and `krb5.h` pack them to two bytes
//! on macOS, which is followed here. `krb5_creds` is only read for its
//! client, server and times, at MIT's offsets.

use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::ptr::{null, null_mut};
use std::sync::Arc;

use std::time::{SystemTime, UNIX_EPOCH};

use libloading::Library;

use super::{Acceptor, Backend, Error, Initiator, InitiatorCredential, Result, SecurityContext};

type Om = u32;
type Name = *mut c_void;
type Ctx = *mut c_void;
type Cred = *mut c_void;
type K = *mut c_void;
type Principal = *mut c_void;
type Ccache = *mut c_void;
type Keytab = *mut c_void;

#[repr(C)]
struct Buffer {
    length: usize,
    value: *mut c_void,
}

impl Buffer {
    fn empty() -> Self {
        Self {
            length: 0,
            value: null_mut(),
        }
    }

    fn of(data: &[u8]) -> Self {
        Self {
            length: data.len(),
            value: data.as_ptr() as *mut c_void,
        }
    }
}

#[cfg_attr(target_os = "macos", repr(C, packed(2)))]
#[cfg_attr(not(target_os = "macos"), repr(C))]
struct Oid {
    length: u32,
    elements: *const c_void,
}

#[repr(C)]
struct OidSet {
    count: usize,
    elements: *const Oid,
}

#[repr(C)]
struct KeyValue {
    key: *const c_char,
    value: *const c_char,
}

#[cfg_attr(target_os = "macos", repr(C, packed(2)))]
#[cfg_attr(not(target_os = "macos"), repr(C))]
struct KeyValueSet {
    count: u32,
    elements: *const KeyValue,
}

/// 1.2.840.113554.1.2.2, Kerberos V5.
static KRB5_MECH: &[u8] = b"\x2a\x86\x48\x86\xf7\x12\x01\x02\x02";
/// 1.2.840.113554.1.2.2.1, `GSS_KRB5_NT_PRINCIPAL_NAME`.
static KRB5_NT_PRINCIPAL: &[u8] = b"\x2a\x86\x48\x86\xf7\x12\x01\x02\x02\x01";
/// 1.2.840.113554.1.2.1.4, `GSS_C_NT_HOSTBASED_SERVICE`.
static NT_HOSTBASED_SERVICE: &[u8] = b"\x2a\x86\x48\x86\xf7\x12\x01\x02\x01\x04";

fn oid(bytes: &'static [u8]) -> Oid {
    Oid {
        length: bytes.len() as u32,
        elements: bytes.as_ptr() as *const c_void,
    }
}

const GSS_C_INITIATE: c_int = 1;
const GSS_C_ACCEPT: c_int = 2;
const GSS_C_MUTUAL_FLAG: Om = 2;
const GSS_C_REPLAY_FLAG: Om = 4;
const GSS_C_SEQUENCE_FLAG: Om = 8;
const GSS_C_INTEG_FLAG: Om = 32;
const GSS_S_CONTINUE_NEEDED: Om = 1;
const GSS_S_DUPLICATE_TOKEN: Om = 2;
const GSS_S_OLD_TOKEN: Om = 4;
const GSS_S_UNSEQ_TOKEN: Om = 8;
const GSS_S_GAP_TOKEN: Om = 16;
const GSS_C_GSS_CODE: c_int = 1;
const GSS_C_MECH_CODE: c_int = 2;

fn gss_error(major: Om) -> bool {
    major & 0xffff_0000 != 0
}

macro_rules! functions {
    ($g:ident, $k:ident; $($lib:ident . $name:ident : fn($($arg:ty),*) $(-> $ret:ty)?;)*) => {
        #[allow(non_snake_case)]
        struct Fns {
            $($name: unsafe extern "system" fn($($arg),*) $(-> $ret)?,)*
        }

        impl Fns {
            fn load($g: &Library, $k: &Library) -> std::result::Result<Self, String> {
                // SAFETY: each symbol is MIT's, with the signature of its header.
                unsafe {
                    Ok(Self {
                        $($name: *$lib.get(concat!(stringify!($name), "\0").as_bytes())
                            .map_err(|e| format!("{}: {e}", stringify!($name)))?,)*
                    })
                }
            }
        }
    };
}

functions! {
    gss, krb5;
    gss.gss_import_name: fn(*mut Om, *const Buffer, *const Oid, *mut Name) -> Om;
    gss.gss_display_name: fn(*mut Om, Name, *mut Buffer, *mut *const Oid) -> Om;
    gss.gss_release_name: fn(*mut Om, *mut Name) -> Om;
    gss.gss_release_buffer: fn(*mut Om, *mut Buffer) -> Om;
    gss.gss_release_cred: fn(*mut Om, *mut Cred) -> Om;
    gss.gss_acquire_cred_from: fn(*mut Om, Name, Om, *const OidSet, c_int, *const KeyValueSet, *mut Cred, *mut *mut OidSet, *mut Om) -> Om;
    gss.gss_accept_sec_context: fn(*mut Om, *mut Ctx, Cred, *const Buffer, *const c_void, *mut Name, *mut *const Oid, *mut Buffer, *mut Om, *mut Om, *mut Cred) -> Om;
    gss.gss_init_sec_context: fn(*mut Om, Cred, *mut Ctx, Name, *const Oid, Om, Om, *const c_void, *const Buffer, *mut *const Oid, *mut Buffer, *mut Om, *mut Om) -> Om;
    gss.gss_inquire_context: fn(*mut Om, Ctx, *mut Name, *mut Name, *mut Om, *mut *const Oid, *mut Om, *mut c_int, *mut c_int) -> Om;
    gss.gss_wrap: fn(*mut Om, Ctx, c_int, Om, *const Buffer, *mut c_int, *mut Buffer) -> Om;
    gss.gss_unwrap: fn(*mut Om, Ctx, *const Buffer, *mut Buffer, *mut c_int, *mut Om) -> Om;
    gss.gss_delete_sec_context: fn(*mut Om, *mut Ctx, *mut Buffer) -> Om;
    gss.gss_display_status: fn(*mut Om, Om, c_int, *const Oid, *mut Om, *mut Buffer) -> Om;
    krb5.krb5_init_context: fn(*mut K) -> i32;
    krb5.krb5_free_context: fn(K);
    krb5.krb5_parse_name: fn(K, *const c_char, *mut Principal) -> i32;
    krb5.krb5_unparse_name: fn(K, Principal, *mut *mut c_char) -> i32;
    krb5.krb5_free_unparsed_name: fn(K, *mut c_char);
    krb5.krb5_free_principal: fn(K, Principal);
    krb5.krb5_get_error_message: fn(K, i32) -> *const c_char;
    krb5.krb5_free_error_message: fn(K, *const c_char);
    krb5.krb5_cc_new_unique: fn(K, *const c_char, *const c_char, *mut Ccache) -> i32;
    krb5.krb5_cc_resolve: fn(K, *const c_char, *mut Ccache) -> i32;
    krb5.krb5_cc_default: fn(K, *mut Ccache) -> i32;
    krb5.krb5_cc_initialize: fn(K, Ccache, Principal) -> i32;
    krb5.krb5_cc_store_cred: fn(K, Ccache, *mut c_void) -> i32;
    krb5.krb5_cc_retrieve_cred: fn(K, Ccache, i32, *mut c_void, *mut c_void) -> i32;
    krb5.krb5_cc_get_principal: fn(K, Ccache, *mut Principal) -> i32;
    krb5.krb5_cc_get_name: fn(K, Ccache) -> *const c_char;
    krb5.krb5_cc_get_type: fn(K, Ccache) -> *const c_char;
    krb5.krb5_cc_close: fn(K, Ccache) -> i32;
    krb5.krb5_cc_destroy: fn(K, Ccache) -> i32;
    krb5.krb5_kt_resolve: fn(K, *const c_char, *mut Keytab) -> i32;
    krb5.krb5_kt_default: fn(K, *mut Keytab) -> i32;
    krb5.krb5_kt_close: fn(K, Keytab) -> i32;
    krb5.krb5_kt_get_entry: fn(K, Keytab, Principal, u32, i32, *mut c_void) -> i32;
    krb5.krb5_free_keytab_entry_contents: fn(K, *mut c_void) -> i32;
    krb5.krb5_get_init_creds_password: fn(K, *mut c_void, Principal, *const c_char, *const c_void, *mut c_void, i32, *const c_char, *const c_void) -> i32;
    krb5.krb5_get_init_creds_keytab: fn(K, *mut c_void, Principal, Keytab, i32, *const c_char, *const c_void) -> i32;
    krb5.krb5_get_renewed_creds: fn(K, *mut c_void, Principal, Ccache, *const c_char) -> i32;
    krb5.krb5_free_cred_contents: fn(K, *mut c_void);
}

/// Room for a `krb5_creds` or a `krb5_keytab_entry`, zeroed.
#[repr(C, align(8))]
struct Raw([u8; 512]);

impl Raw {
    fn new() -> Box<Self> {
        Box::new(Raw([0; 512]))
    }

    fn ptr(&mut self) -> *mut c_void {
        self.0.as_mut_ptr() as *mut c_void
    }
}

/// `krb5_creds`' fields, at MIT's offsets: `magic`, `client`, `server`,
/// `keyblock` (magic, enctype, length, contents), then `times`.
mod creds {
    const PTR: usize = std::mem::size_of::<usize>();
    #[cfg(target_os = "macos")]
    const ALIGN: usize = 2;
    #[cfg(not(target_os = "macos"))]
    const ALIGN: usize = PTR;
    const fn up(n: usize, a: usize) -> usize {
        n.div_ceil(a) * a
    }
    pub const CLIENT: usize = up(4, ALIGN);
    pub const SERVER: usize = CLIENT + PTR;
    const KEYBLOCK: usize = up(SERVER + PTR, ALIGN);
    const KEY_CONTENTS: usize = up(KEYBLOCK + 12, ALIGN);
    pub const TIMES: usize = up(KEY_CONTENTS + PTR, ALIGN);
    pub const AUTHTIME: usize = TIMES;
    pub const ENDTIME: usize = TIMES + 8;
    pub const RENEW_TILL: usize = TIMES + 12;
}

fn read_i32(raw: &Raw, at: usize) -> i32 {
    i32::from_ne_bytes(raw.0[at..at + 4].try_into().expect("4 bytes"))
}

fn write_ptr(raw: &mut Raw, at: usize, p: *mut c_void) {
    raw.0[at..at + std::mem::size_of::<usize>()].copy_from_slice(&(p as usize).to_ne_bytes());
}

pub(crate) struct Mit {
    path: String,
    f: Fns,
    _gss: Library,
    _krb5: Library,
}

#[cfg(all(unix, not(target_os = "macos")))]
const LIBRARIES: &[(&str, &str)] = &[
    ("libgssapi_krb5.so.2", "libkrb5.so.3"),
    ("libgssapi_krb5.so", "libkrb5.so"),
    (
        "/usr/local/lib/libgssapi_krb5.so.2",
        "/usr/local/lib/libkrb5.so.3",
    ),
];
#[cfg(target_os = "macos")]
const LIBRARIES: &[(&str, &str)] = &[
    (
        "/opt/homebrew/opt/krb5/lib/libgssapi_krb5.2.2.dylib",
        "/opt/homebrew/opt/krb5/lib/libkrb5.3.3.dylib",
    ),
    (
        "/usr/local/opt/krb5/lib/libgssapi_krb5.2.2.dylib",
        "/usr/local/opt/krb5/lib/libkrb5.3.3.dylib",
    ),
    (
        "/opt/local/lib/libgssapi_krb5.2.2.dylib",
        "/opt/local/lib/libkrb5.3.3.dylib",
    ),
];
#[cfg(windows)]
const LIBRARIES: &[(&str, &str)] = &[
    ("gssapi64.dll", "krb5_64.dll"),
    (
        r"C:\Program Files\MIT\Kerberos\bin\gssapi64.dll",
        r"C:\Program Files\MIT\Kerberos\bin\krb5_64.dll",
    ),
];

impl Mit {
    pub(crate) fn load() -> Result<Box<dyn Backend>> {
        let mut tried = Vec::new();
        for (gss, krb5) in LIBRARIES {
            // SAFETY: loading MIT Kerberos runs only its own initializers.
            let libs = unsafe { (Library::new(gss), Library::new(krb5)) };
            match libs {
                (Ok(g), Ok(k)) => match Fns::load(&g, &k) {
                    Ok(f) => {
                        return Ok(Box::new(Mit {
                            path: (*gss).to_owned(),
                            f,
                            _gss: g,
                            _krb5: k,
                        }));
                    }
                    Err(e) => tried.push(format!("{gss}: not MIT Kerberos 1.11 or later ({e})")),
                },
                (Err(e), _) | (_, Err(e)) => tried.push(format!("{gss}: {e}")),
            }
        }
        Err(Error(format!(
            "no Kerberos library could be loaded: {}",
            tried.join("; ")
        )))
    }

    fn context(&self) -> Result<Krb5<'_>> {
        let mut ctx = null_mut();
        // SAFETY: an out pointer for a new context.
        let code = unsafe { (self.f.krb5_init_context)(&mut ctx) };
        if code != 0 {
            return Err(Error(format!("krb5_init_context failed ({code})")));
        }
        Ok(Krb5 { mit: self, ctx })
    }

    /// The text of a GSS-API failure: the major and the minor statuses.
    fn status(&self, major: Om, minor: Om) -> Error {
        let mut parts = Vec::new();
        for (value, kind) in [(major, GSS_C_GSS_CODE), (minor, GSS_C_MECH_CODE)] {
            if kind == GSS_C_MECH_CODE && value == 0 {
                continue;
            }
            let mech = oid(KRB5_MECH);
            let mut more: Om = 0;
            loop {
                let mut m = 0;
                let mut text = Buffer::empty();
                // SAFETY: the status' text goes into a buffer we release.
                let r = unsafe {
                    (self.f.gss_display_status)(&mut m, value, kind, &mech, &mut more, &mut text)
                };
                if gss_error(r) {
                    break;
                }
                parts.push(self.take(&mut text).map_or_else(
                    |_| String::new(),
                    |b| String::from_utf8_lossy(&b).into_owned(),
                ));
                if more == 0 {
                    break;
                }
            }
        }
        Error(parts.join(", "))
    }

    /// The bytes of a buffer the library made, released.
    fn take(&self, buffer: &mut Buffer) -> Result<Vec<u8>> {
        let out = if buffer.value.is_null() {
            Vec::new()
        } else {
            // SAFETY: the library's buffer of `length` bytes.
            unsafe { std::slice::from_raw_parts(buffer.value as *const u8, buffer.length) }.to_vec()
        };
        let mut minor = 0;
        // SAFETY: a buffer the library allocated.
        unsafe { (self.f.gss_release_buffer)(&mut minor, buffer) };
        Ok(out)
    }

    fn import_name(&self, name: &str, kind: &'static [u8]) -> Result<GssName<'_>> {
        let buffer = Buffer::of(name.as_bytes());
        let kind = oid(kind);
        let (mut minor, mut out) = (0, null_mut());
        // SAFETY: the name's bytes and type, and an out pointer.
        let major = unsafe { (self.f.gss_import_name)(&mut minor, &buffer, &kind, &mut out) };
        if gss_error(major) {
            return Err(self.status(major, minor));
        }
        Ok(GssName {
            mit: self,
            name: out,
        })
    }

    fn display_name(&self, name: Name) -> Result<String> {
        let (mut minor, mut text) = (0, Buffer::empty());
        let mut kind = null();
        // SAFETY: a name the library made, and out pointers.
        let major = unsafe { (self.f.gss_display_name)(&mut minor, name, &mut text, &mut kind) };
        if gss_error(major) {
            return Err(self.status(major, minor));
        }
        Ok(String::from_utf8_lossy(&self.take(&mut text)?).into_owned())
    }

    /// `gss_acquire_cred_from` with a cred store.
    fn acquire(
        &self,
        name: Option<&str>,
        usage: c_int,
        store: &[(&str, &str)],
    ) -> Result<GssCred<'_>> {
        let name = name
            .map(|n| self.import_name(n, KRB5_NT_PRINCIPAL))
            .transpose()?;
        let mech = oid(KRB5_MECH);
        let mechs = OidSet {
            count: 1,
            elements: &mech,
        };
        let keys: Vec<(CString, CString)> = store
            .iter()
            .map(|(k, v)| Ok((cstring(k)?, cstring(v)?)))
            .collect::<Result<_>>()?;
        let elements: Vec<KeyValue> = keys
            .iter()
            .map(|(k, v)| KeyValue {
                key: k.as_ptr(),
                value: v.as_ptr(),
            })
            .collect();
        let set = KeyValueSet {
            count: elements.len() as u32,
            elements: elements.as_ptr(),
        };
        let (mut minor, mut cred) = (0, null_mut());
        // SAFETY: the name (or none), one mechanism, the store's strings,
        // all alive for the call, and an out pointer.
        let major = unsafe {
            (self.f.gss_acquire_cred_from)(
                &mut minor,
                name.as_ref().map_or(null_mut(), |n| n.name),
                0,
                &mechs,
                usage,
                &set,
                &mut cred,
                null_mut(),
                null_mut(),
            )
        };
        if gss_error(major) {
            return Err(self.status(major, minor));
        }
        Ok(GssCred { mit: self, cred })
    }
}

fn cstring(s: &str) -> Result<CString> {
    CString::new(s).map_err(|_| Error(format!("a NUL in {s:?}")))
}

/// A `krb5_context`, freed when dropped.
struct Krb5<'a> {
    mit: &'a Mit,
    ctx: K,
}

impl Krb5<'_> {
    fn check(&self, code: i32) -> Result<()> {
        if code == 0 {
            return Ok(());
        }
        // SAFETY: the context's message for the code, freed after copying.
        unsafe {
            let msg = (self.mit.f.krb5_get_error_message)(self.ctx, code);
            let text = if msg.is_null() {
                format!("Kerberos error {code}")
            } else {
                CStr::from_ptr(msg).to_string_lossy().into_owned()
            };
            (self.mit.f.krb5_free_error_message)(self.ctx, msg);
            Err(Error(text))
        }
    }

    fn parse(&self, name: &str) -> Result<Krb5Principal<'_, '_>> {
        let c = cstring(name)?;
        let mut p = null_mut();
        // SAFETY: a name and an out pointer.
        self.check(unsafe { (self.mit.f.krb5_parse_name)(self.ctx, c.as_ptr(), &mut p) })?;
        Ok(Krb5Principal { k: self, p })
    }

    fn unparse(&self, p: Principal) -> Result<String> {
        let mut out = null_mut();
        // SAFETY: a principal of this context, and an out pointer.
        self.check(unsafe { (self.mit.f.krb5_unparse_name)(self.ctx, p, &mut out) })?;
        // SAFETY: the library's string, freed after copying.
        unsafe {
            let s = CStr::from_ptr(out).to_string_lossy().into_owned();
            (self.mit.f.krb5_free_unparsed_name)(self.ctx, out);
            Ok(s)
        }
    }

    fn keytab(&self, name: Option<&str>) -> Result<Krb5Keytab<'_, '_>> {
        let mut kt = null_mut();
        let code = match name {
            Some(n) => {
                let c = cstring(n)?;
                // SAFETY: a keytab name and an out pointer.
                unsafe { (self.mit.f.krb5_kt_resolve)(self.ctx, c.as_ptr(), &mut kt) }
            }
            // SAFETY: an out pointer.
            None => unsafe { (self.mit.f.krb5_kt_default)(self.ctx, &mut kt) },
        };
        self.check(code)?;
        Ok(Krb5Keytab { k: self, kt })
    }

    /// A new memory ticket cache holding `creds`, for `client`.
    fn store(&self, client: Principal, creds: &mut Raw) -> Result<MemoryCache> {
        let mut cc = null_mut();
        let kind = c"MEMORY";
        // SAFETY: a new cache of the memory type, initialized for the
        // client and given the credentials.
        unsafe {
            self.check((self.mit.f.krb5_cc_new_unique)(
                self.ctx,
                kind.as_ptr(),
                null(),
                &mut cc,
            ))?;
            let name = format!(
                "{}:{}",
                CStr::from_ptr((self.mit.f.krb5_cc_get_type)(self.ctx, cc)).to_string_lossy(),
                CStr::from_ptr((self.mit.f.krb5_cc_get_name)(self.ctx, cc)).to_string_lossy()
            );
            let stored = self
                .check((self.mit.f.krb5_cc_initialize)(self.ctx, cc, client))
                .and_then(|_| {
                    self.check((self.mit.f.krb5_cc_store_cred)(self.ctx, cc, creds.ptr()))
                });
            if let Err(e) = stored {
                (self.mit.f.krb5_cc_destroy)(self.ctx, cc);
                return Err(e);
            }
            (self.mit.f.krb5_cc_close)(self.ctx, cc);
            Ok(MemoryCache {
                mit: self.mit as *const Mit,
                name,
            })
        }
    }
}

impl Drop for Krb5<'_> {
    fn drop(&mut self) {
        // SAFETY: the context made in `context`.
        unsafe { (self.mit.f.krb5_free_context)(self.ctx) }
    }
}

struct Krb5Principal<'a, 'b> {
    k: &'a Krb5<'b>,
    p: Principal,
}

impl Drop for Krb5Principal<'_, '_> {
    fn drop(&mut self) {
        // SAFETY: a principal of this context.
        unsafe { (self.k.mit.f.krb5_free_principal)(self.k.ctx, self.p) }
    }
}

struct Krb5Keytab<'a, 'b> {
    k: &'a Krb5<'b>,
    kt: Keytab,
}

impl Drop for Krb5Keytab<'_, '_> {
    fn drop(&mut self) {
        // SAFETY: a keytab of this context.
        unsafe { (self.k.mit.f.krb5_kt_close)(self.k.ctx, self.kt) };
    }
}

/// Credentials in a cache: one of our memory caches, destroyed with the
/// last subject that has it, or a ticket cache of the user's.
struct MemoryCache {
    mit: *const Mit,
    name: String,
}

// SAFETY: `mit` is the backend, which lives for the process.
unsafe impl Send for MemoryCache {}
unsafe impl Sync for MemoryCache {}

impl Drop for MemoryCache {
    fn drop(&mut self) {
        // SAFETY: the backend outlives every cache; the cache is ours.
        let mit = unsafe { &*self.mit };
        if let Ok(k) = mit.context()
            && let Ok(name) = cstring(&self.name)
        {
            let mut cc = null_mut();
            // SAFETY: our cache, by name.
            unsafe {
                if (mit.f.krb5_cc_resolve)(k.ctx, name.as_ptr(), &mut cc) == 0 {
                    (mit.f.krb5_cc_destroy)(k.ctx, cc);
                }
            }
        }
    }
}

enum CacheRef {
    Owned(MemoryCache),
    User(String),
}

struct MitInitiator {
    cache: CacheRef,
    principal: String,
}

impl MitInitiator {
    fn cache_name(&self) -> &str {
        match &self.cache {
            CacheRef::Owned(c) => &c.name,
            CacheRef::User(n) => n,
        }
    }
}

impl InitiatorCredential for MitInitiator {
    fn describe(&self) -> String {
        format!("{} in {}", self.principal, self.cache_name())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

struct GssName<'a> {
    mit: &'a Mit,
    name: Name,
}

impl Drop for GssName<'_> {
    fn drop(&mut self) {
        let mut minor = 0;
        // SAFETY: a name the library made.
        unsafe { (self.mit.f.gss_release_name)(&mut minor, &mut self.name) };
    }
}

struct GssCred<'a> {
    mit: &'a Mit,
    cred: Cred,
}

impl Drop for GssCred<'_> {
    fn drop(&mut self) {
        let mut minor = 0;
        // SAFETY: a credential the library made.
        unsafe { (self.mit.f.gss_release_cred)(&mut minor, &mut self.cred) };
    }
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64)
}

/// The realm of a full principal name.
fn realm_of(name: &str) -> &str {
    name.rsplit_once('@').map_or("", |(_, r)| r)
}

impl Backend for Mit {
    fn name(&self) -> &str {
        &self.path
    }

    fn canonical_name(&self, name: &str) -> Result<String> {
        let k = self.context()?;
        let p = k.parse(name)?;
        k.unparse(p.p)
    }

    fn initiate_with_password(&self, principal: &str, password: &str) -> Result<Initiator> {
        let k = self.context()?;
        let p = k.parse(principal)?;
        let pw = cstring(password)?;
        let mut creds = Raw::new();
        // SAFETY: room for the credentials, the client and its password.
        k.check(unsafe {
            (self.f.krb5_get_init_creds_password)(
                k.ctx,
                creds.ptr(),
                p.p,
                pw.as_ptr(),
                null(),
                null_mut(),
                0,
                null(),
                null(),
            )
        })?;
        let cache = k.store(p.p, &mut creds);
        // SAFETY: the credentials the call filled in.
        unsafe { (self.f.krb5_free_cred_contents)(k.ctx, creds.ptr()) };
        Ok(Initiator(Arc::new(MitInitiator {
            cache: CacheRef::Owned(cache?),
            principal: k.unparse(p.p)?,
        })))
    }

    fn keytab_has_keys(&self, keytab: Option<&str>, principal: &str) -> bool {
        let Ok(k) = self.context() else { return false };
        let (Ok(p), Ok(kt)) = (k.parse(principal), k.keytab(keytab)) else {
            return false;
        };
        let mut entry = Raw::new();
        // SAFETY: room for an entry, freed when found.
        unsafe {
            if (self.f.krb5_kt_get_entry)(k.ctx, kt.kt, p.p, 0, 0, entry.ptr()) == 0 {
                (self.f.krb5_free_keytab_entry_contents)(k.ctx, entry.ptr());
                true
            } else {
                false
            }
        }
    }

    fn initiate_with_keytab(&self, keytab: Option<&str>, principal: &str) -> Result<Initiator> {
        let k = self.context()?;
        let p = k.parse(principal)?;
        let kt = k.keytab(keytab)?;
        let mut creds = Raw::new();
        // SAFETY: room for the credentials, the client and the keytab.
        k.check(unsafe {
            (self.f.krb5_get_init_creds_keytab)(k.ctx, creds.ptr(), p.p, kt.kt, 0, null(), null())
        })?;
        let cache = k.store(p.p, &mut creds);
        // SAFETY: the credentials the call filled in.
        unsafe { (self.f.krb5_free_cred_contents)(k.ctx, creds.ptr()) };
        Ok(Initiator(Arc::new(MitInitiator {
            cache: CacheRef::Owned(cache?),
            principal: k.unparse(p.p)?,
        })))
    }

    fn ticket_cache(
        &self,
        principal: Option<&str>,
        cache: Option<&str>,
        renew: bool,
    ) -> Result<Option<(String, Initiator)>> {
        let k = self.context()?;
        let mut cc = null_mut();
        let code = match cache {
            Some(n) => {
                let c = cstring(n)?;
                // SAFETY: a cache name and an out pointer.
                unsafe { (self.f.krb5_cc_resolve)(k.ctx, c.as_ptr(), &mut cc) }
            }
            // SAFETY: an out pointer.
            None => unsafe { (self.f.krb5_cc_default)(k.ctx, &mut cc) },
        };
        if code != 0 {
            return Ok(None);
        }
        struct Closing<'a, 'b>(&'a Krb5<'b>, Ccache);
        impl Drop for Closing<'_, '_> {
            fn drop(&mut self) {
                // SAFETY: the cache opened above.
                unsafe { (self.0.mit.f.krb5_cc_close)(self.0.ctx, self.1) };
            }
        }
        let cc = Closing(&k, cc);
        // SAFETY: the cache's type and name.
        let full = unsafe {
            format!(
                "{}:{}",
                CStr::from_ptr((self.f.krb5_cc_get_type)(k.ctx, cc.1)).to_string_lossy(),
                CStr::from_ptr((self.f.krb5_cc_get_name)(k.ctx, cc.1)).to_string_lossy()
            )
        };
        let mut client = null_mut();
        // SAFETY: an out pointer.
        if unsafe { (self.f.krb5_cc_get_principal)(k.ctx, cc.1, &mut client) } != 0 {
            return Ok(None);
        }
        let client = Krb5Principal { k: &k, p: client };
        let client_name = k.unparse(client.p)?;
        if let Some(wanted) = principal
            && self.canonical_name(wanted)? != client_name
        {
            return Ok(None);
        }
        let realm = realm_of(&client_name);
        let tgt_name = k.parse(&format!("krbtgt/{realm}@{realm}"))?;
        let mut wanted = Raw::new();
        write_ptr(&mut wanted, creds::CLIENT, client.p);
        write_ptr(&mut wanted, creds::SERVER, tgt_name.p);
        let mut tgt = Raw::new();
        // SAFETY: matching credentials naming client and server, and room
        // for the TGT found.
        if unsafe { (self.f.krb5_cc_retrieve_cred)(k.ctx, cc.1, 0, wanted.ptr(), tgt.ptr()) } != 0 {
            return Ok(None);
        }
        let (auth, end) = (
            read_i32(&tgt, creds::AUTHTIME) as u32 as i64,
            read_i32(&tgt, creds::ENDTIME) as u32 as i64,
        );
        let renew_till = read_i32(&tgt, creds::RENEW_TILL) as u32 as i64;
        // SAFETY: the credentials retrieved.
        unsafe { (self.f.krb5_free_cred_contents)(k.ctx, tgt.ptr()) };
        let t = now();
        let mut end_time = end;
        let mut renewed = None;
        // `isOld`: past half its life.
        if renew && end != 0 && t - auth > end - t {
            if renew_till == 0 {
                tracing::debug!("Ticket could not be renewed : This ticket is not renewable");
            } else if t > renew_till {
                tracing::debug!(
                    "Ticket could not be renewed : This ticket is past its last renewal time."
                );
            } else {
                let mut new = Raw::new();
                // SAFETY: room for the renewed credentials.
                let code = unsafe {
                    (self.f.krb5_get_renewed_creds)(k.ctx, new.ptr(), client.p, cc.1, null())
                };
                if code == 0 {
                    end_time = read_i32(&new, creds::ENDTIME) as u32 as i64;
                    let stored = k.store(client.p, &mut new);
                    // SAFETY: the credentials the call filled in.
                    unsafe { (self.f.krb5_free_cred_contents)(k.ctx, new.ptr()) };
                    renewed = Some(stored?);
                    tracing::debug!("Renewed Kerberos Ticket");
                } else {
                    tracing::debug!(
                        "Ticket could not be renewed : {}",
                        k.check(code).unwrap_err()
                    );
                }
            }
        }
        // `isCurrent`.
        if end_time != 0 && t > end_time {
            return Ok(None);
        }
        let cache = match renewed {
            Some(c) => CacheRef::Owned(c),
            None => CacheRef::User(full),
        };
        Ok(Some((
            client_name.clone(),
            Initiator(Arc::new(MitInitiator {
                cache,
                principal: client_name,
            })),
        )))
    }

    fn accept(&self, acceptor: &Acceptor) -> Result<Box<dyn SecurityContext>> {
        let (keytab, principal) = match acceptor {
            Acceptor::Keytab { keytab, principal } => (keytab.as_deref(), principal.as_deref()),
            Acceptor::PasswordKeys { principal } => {
                return Err(Error(format!(
                    "No valid credentials provided: keys made from {principal}'s password cannot accept"
                )));
            }
        };
        let store: Vec<(&str, &str)> = keytab.map(|k| ("keytab", k)).into_iter().collect();
        let cred = self.acquire(principal, GSS_C_ACCEPT, &store)?;
        let cred = ManuallyCred::from(cred).0;
        Ok(Box::new(MitContext {
            mit: self as *const Mit,
            ctx: null_mut(),
            cred,
            acceptor: true,
            target: null_mut(),
            flags: 0,
            established: false,
        }))
    }

    fn initiate(
        &self,
        initiator: &Initiator,
        service: &str,
        mutual: bool,
    ) -> Result<Box<dyn SecurityContext>> {
        let mi = initiator
            .0
            .as_any()
            .downcast_ref::<MitInitiator>()
            .ok_or_else(|| Error("credentials of another Kerberos library".into()))?;
        let cred = self.acquire(
            Some(&mi.principal),
            GSS_C_INITIATE,
            &[("ccache", mi.cache_name())],
        )?;
        let cred = ManuallyCred::from(cred).0;
        let target = self.import_name(service, NT_HOSTBASED_SERVICE)?;
        let target = std::mem::replace(&mut std::mem::ManuallyDrop::new(target).name, null_mut());
        let mut flags = GSS_C_INTEG_FLAG | GSS_C_SEQUENCE_FLAG | GSS_C_REPLAY_FLAG;
        if mutual {
            flags |= GSS_C_MUTUAL_FLAG;
        }
        Ok(Box::new(MitContext {
            mit: self as *const Mit,
            ctx: null_mut(),
            cred,
            acceptor: false,
            target,
            flags,
            established: false,
        }))
    }
}

/// A credential taken out of its guard, to be released by the context.
struct ManuallyCred(Cred);

impl From<GssCred<'_>> for ManuallyCred {
    fn from(c: GssCred<'_>) -> Self {
        let c = std::mem::ManuallyDrop::new(c);
        ManuallyCred(c.cred)
    }
}

struct MitContext {
    mit: *const Mit,
    ctx: Ctx,
    cred: Cred,
    acceptor: bool,
    target: Name,
    flags: Om,
    established: bool,
}

// SAFETY: a context is used by one thread at a time; the backend lives for
// the process.
unsafe impl Send for MitContext {}

impl MitContext {
    fn mit(&self) -> &'static Mit {
        // SAFETY: the backend outlives every context.
        unsafe { &*self.mit }
    }
}

impl SecurityContext for MitContext {
    fn step(&mut self, token: &[u8]) -> Result<Option<Vec<u8>>> {
        let mit = self.mit();
        let input = Buffer::of(token);
        let (mut minor, mut output) = (0, Buffer::empty());
        // SAFETY: the context, credential and names are ours; input lives
        // for the call; output is released by `take`.
        let major = unsafe {
            if self.acceptor {
                (mit.f.gss_accept_sec_context)(
                    &mut minor,
                    &mut self.ctx,
                    self.cred,
                    &input,
                    null(),
                    null_mut(),
                    null_mut(),
                    &mut output,
                    null_mut(),
                    null_mut(),
                    null_mut(),
                )
            } else {
                let mech = oid(KRB5_MECH);
                (mit.f.gss_init_sec_context)(
                    &mut minor,
                    self.cred,
                    &mut self.ctx,
                    self.target,
                    &mech,
                    self.flags,
                    0,
                    null(),
                    if self.ctx.is_null() { null() } else { &input },
                    null_mut(),
                    &mut output,
                    null_mut(),
                    null_mut(),
                )
            }
        };
        let out = mit.take(&mut output)?;
        if gss_error(major) {
            return Err(mit.status(major, minor));
        }
        self.established = major & GSS_S_CONTINUE_NEEDED == 0;
        Ok((!out.is_empty()).then_some(out))
    }

    fn is_established(&self) -> bool {
        self.established
    }

    fn source_name(&self) -> Result<String> {
        let mit = self.mit();
        let (mut minor, mut src) = (0, null_mut());
        // SAFETY: an established context, and an out pointer for its
        // source name, released below.
        let major = unsafe {
            (mit.f.gss_inquire_context)(
                &mut minor,
                self.ctx,
                &mut src,
                null_mut(),
                null_mut(),
                null_mut(),
                null_mut(),
                null_mut(),
                null_mut(),
            )
        };
        if gss_error(major) {
            return Err(mit.status(major, minor));
        }
        let name = GssName { mit, name: src };
        mit.display_name(name.name)
    }

    fn wrap(&mut self, data: &[u8]) -> Result<Vec<u8>> {
        let mit = self.mit();
        let input = Buffer::of(data);
        let (mut minor, mut conf, mut output) = (0, 0, Buffer::empty());
        // SAFETY: an established context; output released by `take`.
        let major =
            unsafe { (mit.f.gss_wrap)(&mut minor, self.ctx, 0, 0, &input, &mut conf, &mut output) };
        let out = mit.take(&mut output)?;
        if gss_error(major) {
            return Err(mit.status(major, minor));
        }
        Ok(out)
    }

    fn unwrap(&mut self, data: &[u8]) -> Result<Vec<u8>> {
        let mit = self.mit();
        let input = Buffer::of(data);
        let (mut minor, mut conf, mut qop, mut output) = (0, 0, 0, Buffer::empty());
        // SAFETY: an established context; output released by `take`.
        let major = unsafe {
            (mit.f.gss_unwrap)(
                &mut minor,
                self.ctx,
                &input,
                &mut output,
                &mut conf,
                &mut qop,
            )
        };
        let out = mit.take(&mut output)?;
        if gss_error(major) {
            return Err(mit.status(major, minor));
        }
        // `checkMessageProp`.
        let bad =
            major & (GSS_S_DUPLICATE_TOKEN | GSS_S_OLD_TOKEN | GSS_S_UNSEQ_TOKEN | GSS_S_GAP_TOKEN);
        if bad != 0 {
            return Err(Error(format!("Handshake failure: token status {bad:#x}")));
        }
        Ok(out)
    }
}

impl Drop for MitContext {
    fn drop(&mut self) {
        let mit = self.mit();
        let mut minor = 0;
        // SAFETY: what the context holds, each released once.
        unsafe {
            if !self.ctx.is_null() {
                (mit.f.gss_delete_sec_context)(&mut minor, &mut self.ctx, null_mut());
            }
            if !self.cred.is_null() {
                (mit.f.gss_release_cred)(&mut minor, &mut self.cred);
            }
            if !self.target.is_null() {
                (mit.f.gss_release_name)(&mut minor, &mut self.target);
            }
        }
    }
}
