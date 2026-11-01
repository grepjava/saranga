//! Windows's SSPI Kerberos package, when MIT Kerberos for Windows is not
//! installed. SSPI has no keytabs and no ticket cache files: an acceptor
//! accepts as the account the broker runs as (registered for its service
//! principal names), the ticket cache is the logon session's, and a
//! password is only tried against the KDC when a context is first made
//! with it. COMPAT.md lists these differences.

use std::ffi::c_void;
use std::ptr::{null, null_mut};
use std::sync::Arc;

use windows_sys::Win32::Security::Authentication::Identity::{
    AcceptSecurityContext, AcquireCredentialsHandleW, DecryptMessage, DeleteSecurityContext,
    EncryptMessage, FreeContextBuffer, FreeCredentialsHandle, InitializeSecurityContextW,
    QueryContextAttributesW, QueryCredentialsAttributesW, SECBUFFER_DATA, SECBUFFER_PADDING,
    SECBUFFER_STREAM, SECBUFFER_TOKEN, SECBUFFER_VERSION, SECPKG_ATTR_NATIVE_NAMES,
    SECPKG_ATTR_SIZES, SECPKG_CRED_ATTR_NAMES, SECPKG_CRED_INBOUND, SECPKG_CRED_OUTBOUND,
    SECQOP_WRAP_NO_ENCRYPT, SECURITY_NATIVE_DREP, SecBuffer, SecBufferDesc,
    SecPkgContext_NativeNamesW, SecPkgContext_Sizes, SecPkgCredentials_NamesW,
};
use windows_sys::Win32::Security::Credentials::SecHandle;
use windows_sys::Win32::System::Rpc::{SEC_WINNT_AUTH_IDENTITY_UNICODE, SEC_WINNT_AUTH_IDENTITY_W};

use super::{Acceptor, Backend, Error, Initiator, InitiatorCredential, Result, SecurityContext};

const SEC_E_OK: i32 = 0;
const SEC_I_CONTINUE_NEEDED: i32 = 0x0009_0312;
const ISC_REQ_MUTUAL_AUTH: u32 = 0x2;
const ISC_REQ_REPLAY_DETECT: u32 = 0x4;
const ISC_REQ_SEQUENCE_DETECT: u32 = 0x8;
const ISC_REQ_ALLOCATE_MEMORY: u32 = 0x100;
const ISC_REQ_INTEGRITY: u32 = 0x10000;
const ASC_REQ_REPLAY_DETECT: u32 = 0x4;
const ASC_REQ_SEQUENCE_DETECT: u32 = 0x8;
const ASC_REQ_ALLOCATE_MEMORY: u32 = 0x100;
const ASC_REQ_INTEGRITY: u32 = 0x20000;

fn wide(s: &str) -> Vec<u16> {
    s.encode_utf16().chain(Some(0)).collect()
}

fn failed(what: &str, status: i32) -> Error {
    Error(format!(
        "{what} failed: {}",
        std::io::Error::from_raw_os_error(status)
    ))
}

/// A string SSPI allocated, freed after copying.
///
/// # Safety
/// `p` is null or a NUL-terminated wide string from SSPI.
unsafe fn take_wide(p: *mut u16) -> String {
    if p.is_null() {
        return String::new();
    }
    // SAFETY: as the caller promises.
    unsafe {
        let mut n = 0;
        while *p.add(n) != 0 {
            n += 1;
        }
        let s = String::from_utf16_lossy(std::slice::from_raw_parts(p, n));
        FreeContextBuffer(p as *mut c_void);
        s
    }
}

pub(crate) struct Sspi;

impl Sspi {
    pub(crate) fn load() -> Result<Box<dyn Backend>> {
        Ok(Box::new(Sspi))
    }
}

/// A credentials handle, freed with the last subject that has it.
struct Credentials {
    handle: SecHandle,
    principal: String,
}

// SAFETY: SSPI credentials handles may be used from any thread.
unsafe impl Send for Credentials {}
unsafe impl Sync for Credentials {}

impl Drop for Credentials {
    fn drop(&mut self) {
        // SAFETY: a handle `acquire` made.
        unsafe { FreeCredentialsHandle(&self.handle) };
    }
}

impl InitiatorCredential for Credentials {
    fn describe(&self) -> String {
        format!("{} (SSPI)", self.principal)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

fn acquire(usage: u32, identity: Option<(&str, &str, &str)>) -> Result<SecHandle> {
    let package = wide("Kerberos");
    let (user, domain, password) = match identity {
        Some((u, d, p)) => (wide(u), wide(d), wide(p)),
        None => (Vec::new(), Vec::new(), Vec::new()),
    };
    let auth = SEC_WINNT_AUTH_IDENTITY_W {
        User: user.as_ptr() as *mut u16,
        UserLength: user.len().saturating_sub(1) as u32,
        Domain: domain.as_ptr() as *mut u16,
        DomainLength: domain.len().saturating_sub(1) as u32,
        Password: password.as_ptr() as *mut u16,
        PasswordLength: password.len().saturating_sub(1) as u32,
        Flags: SEC_WINNT_AUTH_IDENTITY_UNICODE,
    };
    let mut handle = SecHandle {
        dwLower: 0,
        dwUpper: 0,
    };
    let mut expiry = 0i64;
    // SAFETY: the package name and identity live for the call.
    let status = unsafe {
        AcquireCredentialsHandleW(
            null(),
            package.as_ptr(),
            usage,
            null(),
            if identity.is_some() {
                &auth as *const _ as *const c_void
            } else {
                null()
            },
            None,
            null(),
            &mut handle,
            &mut expiry,
        )
    };
    if status != SEC_E_OK {
        return Err(failed("AcquireCredentialsHandle", status));
    }
    Ok(handle)
}

impl Backend for Sspi {
    fn name(&self) -> &str {
        "SSPI"
    }

    fn canonical_name(&self, name: &str) -> Result<String> {
        if name.contains('@') {
            return Ok(name.to_owned());
        }
        // The realm of the logon session's domain.
        match std::env::var("USERDNSDOMAIN") {
            Ok(realm) if !realm.is_empty() => Ok(format!("{name}@{}", realm.to_uppercase())),
            _ => Ok(name.to_owned()),
        }
    }

    fn initiate_with_password(&self, principal: &str, password: &str) -> Result<Initiator> {
        let principal = self.canonical_name(principal)?;
        let (user, realm) = principal.rsplit_once('@').unwrap_or((&principal, ""));
        let handle = acquire(SECPKG_CRED_OUTBOUND, Some((user, realm, password)))?;
        Ok(Initiator(Arc::new(Credentials { handle, principal })))
    }

    fn keytab_has_keys(&self, _keytab: Option<&str>, _principal: &str) -> bool {
        false
    }

    fn initiate_with_keytab(&self, _keytab: Option<&str>, _principal: &str) -> Result<Initiator> {
        Err(Error("keytabs are not supported by SSPI".into()))
    }

    fn ticket_cache(
        &self,
        principal: Option<&str>,
        cache: Option<&str>,
        _renew: bool,
    ) -> Result<Option<(String, Initiator)>> {
        if let Some(c) = cache {
            tracing::warn!("ticketCache {c} is not used by SSPI: the logon session's tickets are");
        }
        let Ok(handle) = acquire(SECPKG_CRED_OUTBOUND, None) else {
            return Ok(None);
        };
        let mut names = SecPkgCredentials_NamesW {
            sUserName: null_mut(),
        };
        // SAFETY: the handle just acquired, and room for its names.
        let status = unsafe {
            QueryCredentialsAttributesW(
                &handle,
                SECPKG_CRED_ATTR_NAMES,
                &mut names as *mut _ as *mut c_void,
            )
        };
        let creds = Credentials {
            handle,
            principal: String::new(),
        };
        if status != SEC_E_OK {
            return Ok(None);
        }
        // SAFETY: the name SSPI allocated.
        let user = unsafe { take_wide(names.sUserName) };
        // `DOMAIN\user` is the form of a domain account.
        let client = match user.split_once('\\') {
            Some((domain, u)) => format!("{u}@{}", domain.to_uppercase()),
            None => user,
        };
        if let Some(p) = principal
            && !self.canonical_name(p)?.eq_ignore_ascii_case(&client)
        {
            return Ok(None);
        }
        let creds = Credentials {
            principal: client.clone(),
            ..creds
        };
        Ok(Some((client, Initiator(Arc::new(creds)))))
    }

    fn accept(&self, acceptor: &Acceptor) -> Result<Box<dyn SecurityContext>> {
        if let Acceptor::Keytab {
            keytab: Some(k), ..
        } = acceptor
        {
            tracing::debug!(
                "keytab {k} is not used by SSPI: the broker's account accepts for its service principal names"
            );
        }
        let handle = acquire(SECPKG_CRED_INBOUND, None)?;
        Ok(Box::new(SspiContext::new(
            Arc::new(Credentials {
                handle,
                principal: String::new(),
            }),
            None,
            ASC_REQ_INTEGRITY | ASC_REQ_SEQUENCE_DETECT | ASC_REQ_REPLAY_DETECT,
        )))
    }

    fn initiate(
        &self,
        initiator: &Initiator,
        service: &str,
        mutual: bool,
    ) -> Result<Box<dyn SecurityContext>> {
        initiator
            .0
            .as_any()
            .downcast_ref::<Credentials>()
            .ok_or_else(|| Error("credentials of another Kerberos library".into()))?;
        // The initiator keeps the credentials alive through its Arc.
        let creds: Arc<dyn InitiatorCredential> = initiator.0.clone();
        let mut flags = ISC_REQ_INTEGRITY | ISC_REQ_SEQUENCE_DETECT | ISC_REQ_REPLAY_DETECT;
        if mutual {
            flags |= ISC_REQ_MUTUAL_AUTH;
        }
        Ok(Box::new(SspiContext::new(
            creds,
            Some(wide(&super::service_principal(service))),
            flags,
        )))
    }
}

struct SspiContext {
    creds: Arc<dyn InitiatorCredential>,
    ctx: Option<SecHandle>,
    /// The target, for an initiator.
    target: Option<Vec<u16>>,
    flags: u32,
    established: bool,
}

// SAFETY: a context is used by one thread at a time.
unsafe impl Send for SspiContext {}

impl SspiContext {
    fn new(creds: Arc<dyn InitiatorCredential>, target: Option<Vec<u16>>, flags: u32) -> Self {
        Self {
            creds,
            ctx: None,
            target,
            flags,
            established: false,
        }
    }

    fn handle(&self) -> &SecHandle {
        &self
            .creds
            .as_any()
            .downcast_ref::<Credentials>()
            .expect("SSPI credentials")
            .handle
    }

    fn ctx(&self) -> Result<&SecHandle> {
        self.ctx
            .as_ref()
            .ok_or_else(|| Error("no security context".into()))
    }

    fn sizes(&self) -> Result<SecPkgContext_Sizes> {
        let mut sizes = SecPkgContext_Sizes {
            cbMaxToken: 0,
            cbMaxSignature: 0,
            cbBlockSize: 0,
            cbSecurityTrailer: 0,
        };
        // SAFETY: an established context and room for its sizes.
        let status = unsafe {
            QueryContextAttributesW(
                self.ctx()?,
                SECPKG_ATTR_SIZES,
                &mut sizes as *mut _ as *mut c_void,
            )
        };
        if status != SEC_E_OK {
            return Err(failed("QueryContextAttributes", status));
        }
        Ok(sizes)
    }
}

impl SecurityContext for SspiContext {
    fn step(&mut self, token: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut input_data = token.to_vec();
        let mut input = SecBuffer {
            cbBuffer: input_data.len() as u32,
            BufferType: SECBUFFER_TOKEN,
            pvBuffer: input_data.as_mut_ptr() as *mut c_void,
        };
        let input_desc = SecBufferDesc {
            ulVersion: SECBUFFER_VERSION,
            cBuffers: 1,
            pBuffers: &mut input,
        };
        let mut output = SecBuffer {
            cbBuffer: 0,
            BufferType: SECBUFFER_TOKEN,
            pvBuffer: null_mut(),
        };
        let mut output_desc = SecBufferDesc {
            ulVersion: SECBUFFER_VERSION,
            cBuffers: 1,
            pBuffers: &mut output,
        };
        let mut new_ctx = SecHandle {
            dwLower: 0,
            dwUpper: 0,
        };
        let (mut attrs, mut expiry) = (0u32, 0i64);
        let old = self.ctx.as_ref().map_or(null(), |c| c as *const SecHandle);
        // SAFETY: the credentials, context and buffers live for the call;
        // the output token SSPI allocates is freed below.
        let status = unsafe {
            match &self.target {
                None => AcceptSecurityContext(
                    self.handle(),
                    old,
                    &input_desc,
                    self.flags | ASC_REQ_ALLOCATE_MEMORY,
                    SECURITY_NATIVE_DREP,
                    &mut new_ctx,
                    &mut output_desc,
                    &mut attrs,
                    &mut expiry,
                ),
                Some(target) => InitializeSecurityContextW(
                    self.handle(),
                    old,
                    target.as_ptr(),
                    self.flags | ISC_REQ_ALLOCATE_MEMORY,
                    0,
                    SECURITY_NATIVE_DREP,
                    if old.is_null() { null() } else { &input_desc },
                    0,
                    &mut new_ctx,
                    &mut output_desc,
                    &mut attrs,
                    &mut expiry,
                ),
            }
        };
        let out = if output.pvBuffer.is_null() {
            Vec::new()
        } else {
            // SAFETY: SSPI's token of `cbBuffer` bytes, freed after copying.
            unsafe {
                let v = std::slice::from_raw_parts(
                    output.pvBuffer as *const u8,
                    output.cbBuffer as usize,
                )
                .to_vec();
                FreeContextBuffer(output.pvBuffer);
                v
            }
        };
        if status != SEC_E_OK && status != SEC_I_CONTINUE_NEEDED {
            return Err(failed(
                if self.target.is_none() {
                    "AcceptSecurityContext"
                } else {
                    "InitializeSecurityContext"
                },
                status,
            ));
        }
        if self.ctx.is_none() {
            self.ctx = Some(new_ctx);
        }
        self.established = status == SEC_E_OK;
        Ok((!out.is_empty()).then_some(out))
    }

    fn is_established(&self) -> bool {
        self.established
    }

    fn source_name(&self) -> Result<String> {
        let mut names = SecPkgContext_NativeNamesW {
            sClientName: null_mut(),
            sServerName: null_mut(),
        };
        // SAFETY: an established context and room for its names.
        let status = unsafe {
            QueryContextAttributesW(
                self.ctx()?,
                SECPKG_ATTR_NATIVE_NAMES,
                &mut names as *mut _ as *mut c_void,
            )
        };
        if status != SEC_E_OK {
            return Err(failed("QueryContextAttributes", status));
        }
        // SAFETY: the names SSPI allocated.
        unsafe {
            take_wide(names.sServerName);
            Ok(take_wide(names.sClientName))
        }
    }

    fn wrap(&mut self, data: &[u8]) -> Result<Vec<u8>> {
        let sizes = self.sizes()?;
        let mut trailer = vec![0u8; sizes.cbSecurityTrailer as usize];
        let mut body = data.to_vec();
        let mut padding = vec![0u8; sizes.cbBlockSize as usize];
        let mut buffers = [
            SecBuffer {
                cbBuffer: trailer.len() as u32,
                BufferType: SECBUFFER_TOKEN,
                pvBuffer: trailer.as_mut_ptr() as *mut c_void,
            },
            SecBuffer {
                cbBuffer: body.len() as u32,
                BufferType: SECBUFFER_DATA,
                pvBuffer: body.as_mut_ptr() as *mut c_void,
            },
            SecBuffer {
                cbBuffer: padding.len() as u32,
                BufferType: SECBUFFER_PADDING,
                pvBuffer: padding.as_mut_ptr() as *mut c_void,
            },
        ];
        let desc = SecBufferDesc {
            ulVersion: SECBUFFER_VERSION,
            cBuffers: 3,
            pBuffers: buffers.as_mut_ptr(),
        };
        // SAFETY: the buffers live for the call.
        let status = unsafe { EncryptMessage(self.ctx()?, SECQOP_WRAP_NO_ENCRYPT, &desc, 0) };
        if status != SEC_E_OK {
            return Err(failed("EncryptMessage", status));
        }
        let mut out = trailer[..buffers[0].cbBuffer as usize].to_vec();
        out.extend_from_slice(&body[..buffers[1].cbBuffer as usize]);
        out.extend_from_slice(&padding[..buffers[2].cbBuffer as usize]);
        Ok(out)
    }

    fn unwrap(&mut self, data: &[u8]) -> Result<Vec<u8>> {
        let mut stream = data.to_vec();
        let mut buffers = [
            SecBuffer {
                cbBuffer: stream.len() as u32,
                BufferType: SECBUFFER_STREAM,
                pvBuffer: stream.as_mut_ptr() as *mut c_void,
            },
            SecBuffer {
                cbBuffer: 0,
                BufferType: SECBUFFER_DATA,
                pvBuffer: null_mut(),
            },
        ];
        let desc = SecBufferDesc {
            ulVersion: SECBUFFER_VERSION,
            cBuffers: 2,
            pBuffers: buffers.as_mut_ptr(),
        };
        let mut qop = 0u32;
        // SAFETY: the buffers live for the call; the data buffer points
        // into `stream`.
        let status = unsafe { DecryptMessage(self.ctx()?, &desc, 0, &mut qop) };
        if status != SEC_E_OK {
            return Err(failed("DecryptMessage", status));
        }
        if buffers[1].pvBuffer.is_null() {
            return Ok(Vec::new());
        }
        // SAFETY: the data SSPI found, inside `stream`.
        Ok(unsafe {
            std::slice::from_raw_parts(
                buffers[1].pvBuffer as *const u8,
                buffers[1].cbBuffer as usize,
            )
        }
        .to_vec())
    }
}

impl Drop for SspiContext {
    fn drop(&mut self) {
        if let Some(ctx) = &self.ctx {
            // SAFETY: the context SSPI made.
            unsafe { DeleteSecurityContext(ctx) };
        }
    }
}
