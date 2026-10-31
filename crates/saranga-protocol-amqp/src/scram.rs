//! SCRAM's exchanges (`ScramServerFunctionalityImpl` and
//! `ScramClientFunctionalityImpl`): the server's side for clients that
//! choose SCRAM-SHA-256 or SCRAM-SHA-512, the client's for broker
//! connections to a peer that offers them.
//!
//! Messages are read as ASCII and matched with Artemis's patterns, which
//! do not unescape `=2C` and `=3D` in a user name.

use std::sync::LazyLock;

use regex::Regex;
use saranga_security::scram::{self, Scram, ScramError, UserData};

/// Java's `.`: anything but a line terminator (the messages are ASCII).
const ANY: &str = r"[^\n\r]";

static CLIENT_FIRST_MESSAGE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(&format!(
        r"^(([pny])=?([^,]*),([^,]*),)(m?=?[^,]*,?n=([^,]*),r=([^,]*),?{ANY}*)$"
    ))
    .expect("pattern")
});
static CLIENT_FINAL_MESSAGE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(&format!(r"^(c=([^,]*),r=([^,]*)),p=({ANY}*)$")).expect("pattern"));
static SERVER_FIRST_MESSAGE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(&format!(r"^r=([^,]*),s=([^,]*),i=({ANY}*)$")).expect("pattern"));
static SERVER_FINAL_MESSAGE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^v=([^,]*)$").expect("pattern"));

/// `new String(bytes, US_ASCII)`: a byte past ASCII is U+FFFD.
pub(crate) fn ascii_text(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|&b| if b.is_ascii() { b as char } else { '\u{FFFD}' })
        .collect()
}

/// `ScramServerFunctionality.State`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ServerState {
    Initial,
    FirstClientMessageHandled,
    PreparedFirst,
    Ended,
}

/// `ScramServerFunctionalityImpl`.
pub(crate) struct ScramServer {
    scram: Scram,
    server_part_nonce: String,
    state: ServerState,
    successful: bool,
    client_first_message_bare: String,
    nonce: String,
    server_first_message: String,
    user_data: Option<UserData>,
}

impl ScramServer {
    pub(crate) fn new(scram: Scram, server_part_nonce: String) -> Self {
        Self {
            scram,
            server_part_nonce,
            state: ServerState::Initial,
            successful: false,
            client_first_message_bare: String::new(),
            nonce: String::new(),
            server_first_message: String::new(),
            user_data: None,
        }
    }

    pub(crate) fn state(&self) -> ServerState {
        self.state
    }

    pub(crate) fn is_ended(&self) -> bool {
        self.state == ServerState::Ended
    }

    pub(crate) fn is_successful(&self) -> bool {
        self.successful
    }

    /// `handleClientFirstMessage`: the user name.
    pub(crate) fn handle_client_first_message(
        &mut self,
        message: &str,
    ) -> Result<String, ScramError> {
        let Some(m) = CLIENT_FIRST_MESSAGE.captures(message) else {
            self.state = ServerState::Ended;
            return Err(ScramError::new("Invalid message received"));
        };
        self.client_first_message_bare = m[5].to_owned();
        let user = m[6].to_owned();
        self.nonce = format!("{}{}", &m[7], self.server_part_nonce);
        self.state = ServerState::FirstClientMessageHandled;
        Ok(user)
    }

    /// `prepareFirstMessage`: the challenge.
    pub(crate) fn prepare_first_message(&mut self, user_data: UserData) -> String {
        self.server_first_message = format!(
            "r={},s={},i={}",
            self.nonce, user_data.salt, user_data.iterations
        );
        self.user_data = Some(user_data);
        self.state = ServerState::PreparedFirst;
        self.server_first_message.clone()
    }

    /// `prepareFinalMessage`: the server's signature, when the client's
    /// proof is right.
    pub(crate) fn prepare_final_message(
        &mut self,
        client_final: &str,
    ) -> Result<String, ScramError> {
        let message = self.prepare_final_message_unchecked(client_final)?;
        if !self.successful {
            return Err(ScramError::new("client credentials missmatch"));
        }
        Ok(message)
    }

    fn prepare_final_message_unchecked(
        &mut self,
        client_final: &str,
    ) -> Result<String, ScramError> {
        self.state = ServerState::Ended;
        let Some(m) = CLIENT_FINAL_MESSAGE.captures(client_final) else {
            return Err(ScramError::new("Invalid message received"));
        };
        let without_proof = &m[1];
        if self.nonce != m[3] {
            return Err(ScramError::new("Nonce mismatch"));
        }
        let auth_message = format!(
            "{},{},{without_proof}",
            self.client_first_message_bare, self.server_first_message
        );
        let data = self
            .user_data
            .as_ref()
            .ok_or_else(|| ScramError::new("no user data"))?;
        // Base64 that does not decode is an IllegalArgumentException.
        let decode = |s: &str| {
            scram::base64_decode(s).ok_or_else(|| ScramError::new("Illegal base64 character"))
        };
        let stored_key = decode(&data.stored_key)?;
        let client_signature = self.scram.hmac(&stored_key, auth_message.as_bytes())?;
        let server_signature = self
            .scram
            .hmac(&decode(&data.server_key)?, auth_message.as_bytes())?;
        let proof = decode(&m[4])?;
        if proof.len() < client_signature.len() {
            return Err(ScramError::new("Index out of bounds"));
        }
        let client_key: Vec<u8> = client_signature
            .iter()
            .zip(&proof)
            .map(|(s, p)| s ^ p)
            .collect();
        self.successful = self.scram.digest(&client_key) == stored_key;
        Ok(format!("v={}", scram::base64_encode(&server_signature)))
    }
}

/// `ScramClientFunctionality.State`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClientState {
    Initial,
    FirstPrepared,
    FinalPrepared,
    Ended,
}

/// `ScramClientFunctionalityImpl`.
pub(crate) struct ScramClient {
    scram: Scram,
    client_nonce: String,
    state: ClientState,
    client_first_message_bare: String,
    salted_password: Vec<u8>,
    auth_message: String,
}

/// `GS2_HEADER`: no channel binding, no authorization identity.
const GS2_HEADER: &str = "n,,";

impl ScramClient {
    pub(crate) fn new(scram: Scram, client_nonce: String) -> Self {
        Self {
            scram,
            client_nonce,
            state: ClientState::Initial,
            client_first_message_bare: String::new(),
            salted_password: Vec::new(),
            auth_message: String::new(),
        }
    }

    pub(crate) fn state(&self) -> ClientState {
        self.state
    }

    /// `prepareFirstMessage`: the user name after SASLprep.
    pub(crate) fn prepare_first_message(&mut self, user: &str) -> Result<String, ScramError> {
        match scram::prep_as_query_string(user) {
            Ok(user) => {
                self.client_first_message_bare = format!("n={user},r={}", self.client_nonce);
                self.state = ClientState::FirstPrepared;
                Ok(format!("{GS2_HEADER}{}", self.client_first_message_bare))
            }
            Err(_) => {
                self.state = ClientState::Ended;
                Err(ScramError::new("Username contains prohibited character"))
            }
        }
    }

    /// `prepareFinalMessage`: the proof, or `None` where the server's first
    /// message is not one to answer.
    pub(crate) fn prepare_final_message(
        &mut self,
        password: &str,
        server_first: &str,
    ) -> Result<Option<String>, ScramError> {
        let Some(m) = SERVER_FIRST_MESSAGE.captures(server_first) else {
            self.state = ClientState::Ended;
            return Ok(None);
        };
        let nonce = &m[1];
        if !nonce.starts_with(&self.client_nonce) {
            self.state = ClientState::Ended;
            return Ok(None);
        }
        let iterations: i32 = m[3]
            .parse()
            .map_err(|_| ScramError::new(format!("For input string: \"{}\"", &m[3])))?;
        if iterations <= 0 {
            self.state = ClientState::Ended;
            return Ok(None);
        }
        let salt = scram::base64_decode(&m[2])
            .ok_or_else(|| ScramError::new("Illegal base64 character"))?;
        self.salted_password = scram::salted_password(self.scram, password, &salt, iterations)?;
        let without_proof = format!(
            "c={},r={nonce}",
            scram::base64_encode(GS2_HEADER.as_bytes())
        );
        self.auth_message = format!(
            "{},{server_first},{without_proof}",
            self.client_first_message_bare
        );
        let client_key = self.scram.hmac(&self.salted_password, b"Client Key")?;
        let stored_key = self.scram.digest(&client_key);
        let signature = self.scram.hmac(&stored_key, self.auth_message.as_bytes())?;
        let proof: Vec<u8> = client_key
            .iter()
            .zip(&signature)
            .map(|(k, s)| k ^ s)
            .collect();
        self.state = ClientState::FinalPrepared;
        Ok(Some(format!(
            "{without_proof},p={}",
            scram::base64_encode(&proof)
        )))
    }

    /// `checkServerFinalMessage`.
    pub(crate) fn check_server_final_message(
        &mut self,
        server_final: &str,
    ) -> Result<(), ScramError> {
        let Some(m) = SERVER_FINAL_MESSAGE.captures(server_final) else {
            self.state = ClientState::Ended;
            return Err(ScramError::new("invalid message format"));
        };
        let signature = scram::base64_decode(&m[1])
            .ok_or_else(|| ScramError::new("Illegal base64 character"))?;
        self.state = ClientState::Ended;
        let server_key = self.scram.hmac(&self.salted_password, b"Server Key")?;
        let expected = self.scram.hmac(&server_key, self.auth_message.as_bytes())?;
        if signature != expected {
            return Err(ScramError::new("Server signature missmatch"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The client and server of RFC 7677's example exchange.
    #[test]
    fn rfc_7677_exchange() {
        let mut client = ScramClient::new(Scram::Sha256, "rOprNGfwEbeRWgbNEkqO".into());
        let first = client.prepare_first_message("user").unwrap();
        assert_eq!(first, "n,,n=user,r=rOprNGfwEbeRWgbNEkqO");

        let mut server = ScramServer::new(Scram::Sha256, "%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0".into());
        assert_eq!(server.handle_client_first_message(&first).unwrap(), "user");
        let salt = scram::base64_decode("W22ZaJ0SNY7soEsUEjb6gQ==").unwrap();
        let data = scram::new_password(Scram::Sha256, "pencil", &salt, 4096)
            .unwrap()
            .user_data();
        let challenge = server.prepare_first_message(data);
        assert_eq!(
            challenge,
            "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"
        );

        let response = client
            .prepare_final_message("pencil", &challenge)
            .unwrap()
            .unwrap();
        assert_eq!(
            response,
            "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
        );
        let last = server.prepare_final_message(&response).unwrap();
        assert_eq!(last, "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=");
        assert!(server.is_ended() && server.is_successful());
        client.check_server_final_message(&last).unwrap();
    }

    #[test]
    fn a_wrong_password_fails_the_proof() {
        let mut client = ScramClient::new(Scram::Sha512, "abc".into());
        let mut server = ScramServer::new(Scram::Sha512, "def".into());
        let first = client.prepare_first_message("u").unwrap();
        server.handle_client_first_message(&first).unwrap();
        let data = scram::new_password(Scram::Sha512, "right", b"salt", 4096)
            .unwrap()
            .user_data();
        let challenge = server.prepare_first_message(data);
        let response = client
            .prepare_final_message("wrong", &challenge)
            .unwrap()
            .unwrap();
        assert_eq!(
            server.prepare_final_message(&response).unwrap_err().0,
            "client credentials missmatch"
        );
        assert!(server.is_ended() && !server.is_successful());
    }

    #[test]
    fn messages_are_matched_as_artemis_matches_them() {
        let mut server = ScramServer::new(Scram::Sha256, "s".into());
        assert!(server.handle_client_first_message("n=user,r=abc").is_err());
        assert!(server.is_ended());
        let mut server = ScramServer::new(Scram::Sha256, "s".into());
        // An authorization identity, and extensions after the nonce.
        assert_eq!(
            server
                .handle_client_first_message("y,a=admin,n=us=2Cer,r=abc,ext=1")
                .unwrap(),
            "us=2Cer"
        );
        assert_eq!(ascii_text(b"a\xffb"), "a\u{FFFD}b");
    }
}
