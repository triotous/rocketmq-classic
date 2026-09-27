use std::{collections::HashMap, fmt, io::BufReader, path::Path, sync::Arc};

use base64::Engine;
use cbc::cipher::{BlockDecryptMut, KeyIvInit, block_padding::Pkcs7};
use des::Des;
use md5::{Digest, Md5};
use pkcs8::{EncryptedPrivateKeyInfo, PrivateKeyInfo, der::Decode, der::zeroize::Zeroizing};

use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use tokio_rustls::rustls::pki_types::{ServerName, UnixTime};
use tokio_rustls::rustls::{
    ClientConfig, DigitallySignedStruct, Error, RootCertStore, SignatureScheme,
    pki_types::CertificateDer,
};

use crate::error::TransportError;

#[derive(Clone, Debug)]
pub enum TransportMode {
    Plain,
    Tls(TlsConfig),
}

impl TransportMode {
    /// Selects TLS using Java `tls.enable` and `tls.server.mode` defaults.
    pub fn from_java_properties(
        system_properties: &HashMap<String, String>,
        config_file: Option<&Path>,
    ) -> Result<Self, TransportError> {
        Self::from_java_properties_with_strategy(system_properties, config_file, None)
    }

    pub fn from_java_properties_with_strategy(
        system_properties: &HashMap<String, String>,
        config_file: Option<&Path>,
        strategy: Option<Arc<dyn PrivateKeyDecryptionStrategy>>,
    ) -> Result<Self, TransportError> {
        let tls_mode = system_properties
            .get("tls.server.mode")
            .map(String::as_str)
            .unwrap_or("permissive");
        let enabled = system_properties
            .get("tls.enable")
            .map(|value| value.eq_ignore_ascii_case("true"))
            .unwrap_or(tls_mode == "enforcing");
        if !enabled {
            return Ok(Self::Plain);
        }
        Ok(Self::Tls(TlsConfig::from_java_properties_with_strategy(
            system_properties,
            config_file,
            strategy,
        )?))
    }
}

#[derive(Clone)]
pub struct TlsConfig {
    pub ca_pem: Option<Vec<u8>>,
    pub client_cert_pem: Option<Vec<u8>>,
    pub client_key_pem: Option<Vec<u8>>,
    /// Mirrors Java's `tls.test.mode.enable` (default `true`); true skips server trust and hostname checks.
    /// This is insecure and should be disabled outside Java-compatible test configurations.
    pub test_mode: bool,
    /// Mirrors Java's `tls.client.authServer` (default `false`); effective only when test mode is disabled.
    /// False disables server trust and hostname checks, matching Java's trust-all behavior.
    pub auth_server: bool,
    /// Java `tls.client.keyPassword`; used for encrypted PKCS#8 files and configured strategies.
    pub key_password: Option<String>,
    /// Rust equivalent of Java's replaceable `TlsHelper.DecryptionStrategy`.
    pub decryption_strategy: Option<Arc<dyn PrivateKeyDecryptionStrategy>>,
}

/// Decrypts Java-compatible TLS key files when loading TLS properties.
pub trait PrivateKeyDecryptionStrategy: Send + Sync + 'static {
    fn decrypt_private_key(
        &self,
        path: &Path,
        for_client: bool,
        password: Option<&str>,
    ) -> Result<Vec<u8>, TransportError>;
}

impl<F> PrivateKeyDecryptionStrategy for F
where
    F: Fn(&Path, bool, Option<&str>) -> Result<Vec<u8>, TransportError> + Send + Sync + 'static,
{
    fn decrypt_private_key(
        &self,
        path: &Path,
        for_client: bool,
        password: Option<&str>,
    ) -> Result<Vec<u8>, TransportError> {
        self(path, for_client, password)
    }
}

impl fmt::Debug for TlsConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TlsConfig")
            .field("ca_pem_configured", &self.ca_pem.is_some())
            .field(
                "client_cert_pem_configured",
                &self.client_cert_pem.is_some(),
            )
            .field("client_key_pem_configured", &self.client_key_pem.is_some())
            .field("test_mode", &self.test_mode)
            .field("auth_server", &self.auth_server)
            .field("key_password_configured", &self.key_password.is_some())
            .field(
                "decryption_strategy_registered",
                &self.decryption_strategy.is_some(),
            )
            .finish()
    }
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            ca_pem: None,
            client_cert_pem: None,
            client_key_pem: None,
            test_mode: true,
            auth_server: false,
            key_password: None,
            decryption_strategy: None,
        }
    }
}

impl TlsConfig {
    /// Builds client TLS configuration from JVM properties and an optional `tls.config.file`.
    /// Values in the file override JVM properties, matching Java `TlsHelper` loading order.
    pub fn from_java_properties(
        system_properties: &HashMap<String, String>,
        config_file: Option<&Path>,
    ) -> Result<Self, TransportError> {
        Self::from_java_properties_with_strategy(system_properties, config_file, None)
    }

    /// Variant for applications that must decrypt Java-compatible private-key files.
    pub fn from_java_properties_with_strategy(
        system_properties: &HashMap<String, String>,
        config_file: Option<&Path>,
        strategy: Option<Arc<dyn PrivateKeyDecryptionStrategy>>,
    ) -> Result<Self, TransportError> {
        let mut properties = system_properties.clone();
        let configured_path = config_file
            .map(Path::to_owned)
            .or_else(|| {
                properties
                    .get("tls.config.file")
                    .map(|value| Path::new(value).to_owned())
            })
            .unwrap_or_else(|| Path::new("/etc/rocketmq/tls.properties").to_owned());
        if configured_path.is_file() {
            if let Ok(bytes) = std::fs::read(&configured_path) {
                properties.extend(parse_java_properties(&bytes)?);
            }
        }
        let mut config = Self {
            decryption_strategy: strategy,
            ..Self::default()
        };
        config.apply_properties(&properties, Path::new("."))?;
        Ok(config)
    }

    /// Loads client TLS settings from a Java-compatible properties file once.
    /// The file is not watched; new clients must explicitly load changed settings.
    pub fn load_properties_file(&mut self, path: &Path) -> Result<(), TransportError> {
        let properties = read_java_properties(path)?;
        self.apply_properties(&properties, Path::new("."))
    }

    /// Applies Java `TlsSystemConfig` client keys, resolving file paths relative to `base_dir`.
    pub fn apply_properties(
        &mut self,
        properties: &HashMap<String, String>,
        base_dir: &Path,
    ) -> Result<(), TransportError> {
        if let Some(value) = properties.get("tls.test.mode.enable") {
            self.test_mode = value.eq_ignore_ascii_case("true");
        }
        if let Some(value) = properties.get("tls.client.authServer") {
            self.auth_server = value.eq_ignore_ascii_case("true");
        }
        if let Some(value) = properties.get("tls.client.keyPassword") {
            self.key_password = Some(value.clone());
        }
        if !self.test_mode {
            if let Some(path) = properties.get("tls.client.trustCertPath") {
                self.ca_pem = if path.is_empty() {
                    None
                } else {
                    Some(read_tls_file(base_dir, path)?)
                };
            }
            if let Some(path) = properties.get("tls.client.certPath") {
                self.client_cert_pem = if path.is_empty() {
                    None
                } else {
                    Some(read_tls_file(base_dir, path)?)
                };
            }
            if let Some(path) = properties.get("tls.client.keyPath") {
                self.client_key_pem = if path.is_empty() {
                    None
                } else {
                    let path = resolve_tls_path(base_dir, path);
                    Some(match &self.decryption_strategy {
                        Some(strategy) => strategy.decrypt_private_key(
                            &path,
                            true,
                            self.key_password.as_deref(),
                        )?,
                        None => decrypt_private_key_file(&path, self.key_password.as_deref())?,
                    })
                };
            }
        }
        Ok(())
    }

    /// Registers the Java `TlsHelper.DecryptionStrategy` equivalent for subsequent property loads.
    pub fn set_decryption_strategy(
        &mut self,
        strategy: Option<Arc<dyn PrivateKeyDecryptionStrategy>>,
    ) {
        self.decryption_strategy = strategy;
    }

    pub fn from_ca_file(path: &std::path::Path) -> Result<Self, TransportError> {
        let ca_pem = std::fs::read(path).map_err(|error| TransportError::Tls(error.to_string()))?;
        Ok(Self {
            ca_pem: Some(ca_pem),
            test_mode: false,
            auth_server: true,
            ..Self::default()
        })
    }

    pub(crate) fn connector(&self) -> Result<TlsConnector, TransportError> {
        let verify_server = !self.test_mode && self.auth_server;
        let builder = ClientConfig::builder();
        let builder = if verify_server {
            let mut roots = RootCertStore::empty();
            if let Some(pem) = &self.ca_pem {
                let certificates = rustls_pemfile::certs(&mut BufReader::new(pem.as_slice()))
                    .collect::<Result<Vec<CertificateDer<'static>>, _>>()
                    .map_err(|error| TransportError::Tls(error.to_string()))?;
                roots.add_parsable_certificates(certificates);
            } else {
                let certificates = rustls_native_certs::load_native_certs();
                for cert in certificates.certs {
                    roots
                        .add(cert)
                        .map_err(|error| TransportError::Tls(error.to_string()))?;
                }
                if !certificates.errors.is_empty() && roots.is_empty() {
                    return Err(TransportError::Tls(
                        "no system trust roots available".into(),
                    ));
                }
            }
            builder.with_root_certificates(roots)
        } else {
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(TrustAllServerVerifier))
        };
        let config = match (self.test_mode, &self.client_cert_pem, &self.client_key_pem) {
            (true, _, _) => builder.with_no_client_auth(),
            (false, Some(cert_pem), Some(key_pem)) => {
                let certs = rustls_pemfile::certs(&mut BufReader::new(cert_pem.as_slice()))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| TransportError::Tls(error.to_string()))?;
                let mut key_reader = BufReader::new(key_pem.as_slice());
                let key = rustls_pemfile::private_key(&mut key_reader)
                    .map_err(|error| TransportError::Tls(error.to_string()))?
                    .ok_or_else(|| {
                        TransportError::Tls("client key PEM has no private key".into())
                    })?;
                builder
                    .with_client_auth_cert(certs, key)
                    .map_err(|error| TransportError::Tls(error.to_string()))?
            }
            (false, None, None) => builder.with_no_client_auth(),
            _ => {
                return Err(TransportError::Tls(
                    "client certificate and key must be set together".into(),
                ));
            }
        };
        Ok(TlsConnector::from(Arc::new(config)))
    }
}

fn decrypt_private_key_file(
    path: &Path,
    password: Option<&str>,
) -> Result<Vec<u8>, TransportError> {
    let pem = std::fs::read(path).map_err(|error| TransportError::Tls(error.to_string()))?;
    decrypt_private_key_pem(&pem, password)
}

fn decrypt_private_key_pem(pem: &[u8], password: Option<&str>) -> Result<Vec<u8>, TransportError> {
    let pem = std::str::from_utf8(pem)
        .map_err(|error| TransportError::Tls(format!("invalid private key PEM: {error}")))?;
    let (label, body) = parse_private_key_pem(pem)?;
    if label != "ENCRYPTED PRIVATE KEY" {
        return Ok(pem.as_bytes().to_vec());
    }
    let Some(password) = password else {
        return Err(TransportError::Tls(
            "tls.client.keyPassword is required for encrypted private keys".into(),
        ));
    };
    let encrypted = EncryptedPrivateKeyInfo::try_from(body.as_slice())
        .map_err(|error| TransportError::Tls(format!("invalid encrypted private key: {error}")))?;
    let decrypted = Zeroizing::new(match encrypted.encryption_algorithm.pbes1() {
        Some(algorithm)
            if algorithm.encryption
                == pkcs8::pkcs5::pbes1::EncryptionScheme::PbeWithMd5AndDesCbc =>
        {
            decrypt_pbes1_md5_des(&encrypted, password)?
        }
        Some(_) => {
            return Err(TransportError::Tls(
                "unsupported encrypted private key algorithm".into(),
            ));
        }
        None => encrypted
            .decrypt(password)
            .map_err(|error| {
                TransportError::Tls(format!("unable to decrypt private key: {error}"))
            })?
            .to_bytes()
            .to_vec(),
    });
    let private_key = PrivateKeyInfo::from_der(&decrypted)
        .map_err(|error| TransportError::Tls(format!("invalid decrypted private key: {error}")))?;
    let (label, der) = if private_key.algorithm.oid.to_string() == "1.2.840.113549.1.1.1" {
        ("RSA PRIVATE KEY", private_key.private_key)
    } else {
        ("PRIVATE KEY", decrypted.as_slice())
    };
    let mut output = format!("-----BEGIN {label}-----\n").into_bytes();
    let encoded = Zeroizing::new(base64::engine::general_purpose::STANDARD.encode(der));
    for chunk in encoded.as_bytes().chunks(64) {
        output.extend_from_slice(chunk);
        output.push(b'\n');
    }
    output.extend_from_slice(format!("-----END {label}-----\n").as_bytes());
    Ok(output)
}

fn parse_private_key_pem(pem: &str) -> Result<(&str, Vec<u8>), TransportError> {
    let mut lines = pem.lines();
    let first = lines.next().unwrap_or_default();
    let label = first
        .strip_prefix("-----BEGIN ")
        .and_then(|line| line.strip_suffix("-----"))
        .ok_or_else(|| TransportError::Tls("invalid private key PEM header".into()))?;
    let mut encoded = String::new();
    let mut ended = false;
    for line in lines {
        if line == format!("-----END {label}-----") {
            ended = true;
            break;
        }
        encoded.push_str(line.trim());
    }
    if !ended {
        return Err(TransportError::Tls("unterminated private key PEM".into()));
    }
    let der = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|error| TransportError::Tls(format!("invalid private key PEM body: {error}")))?;
    Ok((label, der))
}

fn decrypt_pbes1_md5_des(
    encrypted: &EncryptedPrivateKeyInfo<'_>,
    password: &str,
) -> Result<Vec<u8>, TransportError> {
    let Some(algorithm) = encrypted.encryption_algorithm.pbes1() else {
        return Err(TransportError::Tls(
            "unsupported encrypted private key algorithm".into(),
        ));
    };
    if algorithm.encryption != pkcs8::pkcs5::pbes1::EncryptionScheme::PbeWithMd5AndDesCbc {
        return Err(TransportError::Tls(
            "unsupported encrypted private key algorithm".into(),
        ));
    }

    let password = password.as_bytes();
    if algorithm.parameters.iteration_count == 0 {
        return Err(TransportError::Tls(
            "invalid encrypted private key iteration count".into(),
        ));
    }
    let mut digest = Md5::new();
    digest.update(password);
    digest.update(algorithm.parameters.salt);
    let mut key_and_iv = Zeroizing::new(<[u8; 16]>::from(digest.finalize()));
    for _ in 1..algorithm.parameters.iteration_count {
        *key_and_iv = Md5::digest(*key_and_iv).into();
    }
    let (key, iv) = key_and_iv.split_at(8);
    type DesCbcDecryptor = cbc::Decryptor<Des>;
    let mut plaintext = Zeroizing::new(encrypted.encrypted_data.to_vec());
    let plaintext = DesCbcDecryptor::new_from_slices(key, iv)
        .map_err(|error| TransportError::Tls(error.to_string()))?
        .decrypt_padded_mut::<Pkcs7>(&mut plaintext)
        .map_err(|_| {
            TransportError::Tls("unable to decrypt private key with configured password".into())
        })?;
    Ok(plaintext.to_vec())
}

fn resolve_tls_path(base_dir: &Path, value: &str) -> std::path::PathBuf {
    let path = Path::new(value);
    if path.is_absolute() {
        path.to_owned()
    } else {
        base_dir.join(path)
    }
}

fn read_tls_file(base_dir: &Path, value: &str) -> Result<Vec<u8>, TransportError> {
    std::fs::read(resolve_tls_path(base_dir, value))
        .map_err(|error| TransportError::Tls(error.to_string()))
}

fn read_java_properties(path: &Path) -> Result<HashMap<String, String>, TransportError> {
    let bytes = std::fs::read(path).map_err(|error| TransportError::Tls(error.to_string()))?;
    parse_java_properties(&bytes)
}

fn parse_java_properties(bytes: &[u8]) -> Result<HashMap<String, String>, TransportError> {
    let source = bytes
        .iter()
        .map(|byte| char::from(*byte))
        .collect::<String>();
    let mut logical_lines = Vec::new();
    let mut current = String::new();
    for line in source.lines() {
        let line = if current.is_empty() {
            line
        } else {
            line.trim_start_matches(char::is_whitespace)
        };
        let continuation = line
            .chars()
            .rev()
            .take_while(|character| *character == '\\')
            .count()
            % 2
            == 1;
        if continuation {
            current.push_str(&line[..line.len() - 1]);
        } else {
            current.push_str(line);
            logical_lines.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        logical_lines.push(current);
    }

    let mut properties = HashMap::new();
    for line in logical_lines {
        let line = line.trim_start_matches(char::is_whitespace);
        if line.is_empty() || line.starts_with(['#', '!']) {
            continue;
        }
        let mut escaped = false;
        let separator = line.char_indices().find_map(|(offset, character)| {
            if escaped {
                escaped = false;
                None
            } else if character == '\\' {
                escaped = true;
                None
            } else if character.is_whitespace() || character == '=' || character == ':' {
                Some(offset)
            } else {
                None
            }
        });
        let (key, value_start) = match separator {
            Some(offset) => {
                let mut cursor = offset;
                while cursor < line.len() {
                    let character = line[cursor..].chars().next().unwrap();
                    if !character.is_whitespace() && character != '=' && character != ':' {
                        break;
                    }
                    cursor += character.len_utf8();
                }
                (&line[..offset], cursor)
            }
            None => (line, line.len()),
        };
        properties.insert(
            unescape_java_property(key)?,
            unescape_java_property(&line[value_start..])?,
        );
    }
    Ok(properties)
}

fn read_java_unicode_escape(chars: &mut impl Iterator<Item = char>) -> Result<u32, TransportError> {
    let digits = chars.take(4).collect::<String>();
    if digits.len() != 4 {
        return Err(TransportError::Tls(
            "invalid Unicode escape in TLS properties".into(),
        ));
    }
    u32::from_str_radix(&digits, 16).map_err(|error| TransportError::Tls(error.to_string()))
}

fn unescape_java_property(value: &str) -> Result<String, TransportError> {
    let mut result = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(character) = chars.next() {
        if character != '\\' {
            result.push(character);
            continue;
        }
        match chars.next() {
            Some('t') => result.push('\t'),
            Some('n') => result.push('\n'),
            Some('r') => result.push('\r'),
            Some('f') => result.push('\u{000c}'),
            Some('u') => {
                let code = read_java_unicode_escape(&mut chars)?;
                let character = if (0xD800..=0xDBFF).contains(&code) {
                    if chars.next() != Some('\\') || chars.next() != Some('u') {
                        return Err(TransportError::Tls(
                            "high surrogate must be followed by a low surrogate escape".into(),
                        ));
                    }
                    let low = read_java_unicode_escape(&mut chars)?;
                    if !(0xDC00..=0xDFFF).contains(&low) {
                        return Err(TransportError::Tls(
                            "high surrogate must be followed by a low surrogate escape".into(),
                        ));
                    }
                    0x10000 + ((code - 0xD800) << 10) + (low - 0xDC00)
                } else {
                    code
                };
                result.push(char::from_u32(character).ok_or_else(|| {
                    TransportError::Tls("invalid Unicode scalar in TLS properties".into())
                })?);
            }
            Some(other) => result.push(other),
            None => result.push('\\'),
        }
    }
    Ok(result)
}

#[derive(Debug)]
struct TrustAllServerVerifier;

impl ServerCertVerifier for TrustAllServerVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        let provider = tokio_rustls::rustls::crypto::CryptoProvider::get_default()
            .expect("rustls crypto provider is installed");
        tokio_rustls::rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        let provider = tokio_rustls::rustls::crypto::CryptoProvider::get_default()
            .expect("rustls crypto provider is installed");
        tokio_rustls::rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        tokio_rustls::rustls::crypto::CryptoProvider::get_default()
            .expect("rustls crypto provider is installed")
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        io::BufReader,
        path::PathBuf,
        sync::{Arc, Mutex},
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::{PrivateKeyDecryptionStrategy, TlsConfig, decrypt_private_key_pem};
    use std::collections::HashMap;

    fn test_dir() -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("rocketmq-tls-{unique}"));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn java_tls_properties_apply_file_overrides_and_resolve_cert_paths_from_working_dir() {
        let directory = test_dir();
        let ca_path = directory.join("ca.pem");
        fs::write(&ca_path, b"ca bytes").unwrap();
        let properties_file = directory.join("tls.properties");
        fs::write(
            &properties_file,
            format!(
                "tls.test.mode.enable=false\ntls.client.authServer=true\ntls.client.trustCertPath={}\ntls.client.keyPassword=secret\n",
                ca_path.display()
            ),
        )
        .unwrap();
        let properties = std::collections::HashMap::from([(
            "tls.test.mode.enable".to_string(),
            "true".to_string(),
        )]);
        let config = TlsConfig::from_java_properties(&properties, Some(&properties_file)).unwrap();
        assert!(!config.test_mode);
        assert!(config.auth_server);
        assert_eq!(config.ca_pem.as_deref(), Some(b"ca bytes".as_slice()));
        assert_eq!(config.key_password.as_deref(), Some("secret"));
        let debug = format!("{config:?}");
        assert!(!debug.contains("secret"));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn java_tls_test_mode_ignores_client_material_and_decryption_strategy() {
        let calls = Arc::new(Mutex::new(0));
        let observed = calls.clone();
        let strategy: Arc<dyn PrivateKeyDecryptionStrategy> =
            Arc::new(move |_: &std::path::Path, _: bool, _: Option<&str>| {
                *observed.lock().unwrap() += 1;
                panic!("test mode must not decrypt client key material")
            });
        let properties = HashMap::from([
            ("tls.test.mode.enable".into(), "true".into()),
            ("tls.client.trustCertPath".into(), "missing-ca.pem".into()),
            ("tls.client.certPath".into(), "missing-cert.pem".into()),
            ("tls.client.keyPath".into(), "missing-key.pem".into()),
        ]);
        let config =
            TlsConfig::from_java_properties_with_strategy(&properties, None, Some(strategy))
                .unwrap();

        assert!(config.ca_pem.is_none());
        assert!(config.client_cert_pem.is_none());
        assert!(config.client_key_pem.is_none());
        assert_eq!(*calls.lock().unwrap(), 0);
        assert!(config.connector().is_ok());
        let config_with_unused_material = TlsConfig {
            client_cert_pem: Some(b"not a certificate".to_vec()),
            client_key_pem: Some(b"not a private key".to_vec()),
            ..config
        };
        assert!(config_with_unused_material.connector().is_ok());
    }

    #[test]
    fn java_tls_properties_constructor_ignores_missing_file_but_rejects_malformed_escapes() {
        let missing = test_dir().join("missing.properties");
        assert!(TlsConfig::from_java_properties(&HashMap::new(), Some(&missing)).is_ok());

        let directory = test_dir();
        let properties_file = directory.join("tls.properties");
        fs::write(&properties_file, r"tls.client.keyPassword=\uZZZZ").unwrap();
        assert!(TlsConfig::from_java_properties(&HashMap::new(), Some(&properties_file)).is_err());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn java_tls_properties_decode_surrogate_pair_unicode_escapes() {
        let directory = test_dir();
        let properties_file = directory.join("tls.properties");
        fs::write(&properties_file, r"tls.client.keyPassword=\uD83D\uDE80").unwrap();
        let config =
            TlsConfig::from_java_properties(&HashMap::new(), Some(&properties_file)).unwrap();

        assert_eq!(config.key_password.as_deref(), Some("🚀"));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn java_tls_transport_selection_matches_tls_enable_and_mode_defaults() {
        use super::TransportMode;

        let empty = HashMap::new();
        assert!(matches!(
            TransportMode::from_java_properties(&empty, None).unwrap(),
            TransportMode::Plain
        ));
        assert!(matches!(
            TransportMode::from_java_properties(
                &HashMap::from([("tls.server.mode".into(), "enforcing".into())]),
                None,
            )
            .unwrap(),
            TransportMode::Tls(_)
        ));
        assert!(matches!(
            TransportMode::from_java_properties(
                &HashMap::from([
                    ("tls.server.mode".into(), "enforcing".into()),
                    ("tls.enable".into(), "false".into()),
                ]),
                None,
            )
            .unwrap(),
            TransportMode::Plain
        ));
        assert!(matches!(
            TransportMode::from_java_properties(
                &HashMap::from([("tls.enable".into(), "true".into())]),
                None,
            )
            .unwrap(),
            TransportMode::Tls(_)
        ));
    }

    #[test]
    fn java_pbes1_md5_des_encrypted_private_key_is_decrypted_with_java_password() {
        let java_pem = include_bytes!("../../tests/fixtures/tls/java-pbes1-client.key");
        let decrypted = decrypt_private_key_pem(java_pem, Some("1234")).unwrap();
        assert!(decrypted.starts_with(b"-----BEGIN RSA PRIVATE KEY-----"));
        let mut reader = BufReader::new(decrypted.as_slice());
        assert!(rustls_pemfile::private_key(&mut reader).unwrap().is_some());
        assert!(decrypt_private_key_pem(java_pem, Some("wrong-password")).is_err());
        assert!(decrypt_private_key_pem(java_pem, None).is_err());
        assert!(decrypt_private_key_pem(java_pem, Some("")).is_err());

        let pem = include_bytes!("../../tests/fixtures/tls/pbes1-2048.key");
        let decrypted = decrypt_private_key_pem(pem, Some("java-pbes1-password")).unwrap();
        let mut reader = BufReader::new(decrypted.as_slice());
        assert!(rustls_pemfile::private_key(&mut reader).unwrap().is_some());
    }

    #[test]
    fn pbes1_key_is_loaded_from_java_properties_with_key_password() {
        let directory = test_dir();
        let key_path = directory.join("client.key");
        fs::write(
            &key_path,
            include_bytes!("../../tests/fixtures/tls/pbes1-2048.key"),
        )
        .unwrap();
        let cert_path = directory.join("client.pem");
        fs::write(
            &cert_path,
            include_bytes!("../../tests/fixtures/tls/pbes1-2048-cert.pem"),
        )
        .unwrap();
        let properties = HashMap::from([
            ("tls.test.mode.enable".into(), "false".into()),
            ("tls.client.authServer".into(), "false".into()),
            (
                "tls.client.certPath".into(),
                cert_path.display().to_string(),
            ),
            ("tls.client.keyPath".into(), key_path.display().to_string()),
            (
                "tls.client.keyPassword".into(),
                "java-pbes1-password".into(),
            ),
        ]);
        let config = TlsConfig::from_java_properties(&properties, None).unwrap();
        let decrypted = config.client_key_pem.as_ref().unwrap();
        assert!(decrypted.starts_with(b"-----BEGIN RSA PRIVATE KEY-----"));
        let mut reader = BufReader::new(decrypted.as_slice());
        assert!(rustls_pemfile::private_key(&mut reader).unwrap().is_some());
        let connector = config.connector();
        assert!(connector.is_ok(), "{}", connector.err().unwrap());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn pbes2_encrypted_private_key_is_decrypted() {
        let pem = include_bytes!("../../tests/fixtures/tls/pbes2-aes256.key");
        let decrypted = decrypt_private_key_pem(pem, Some("rust-test-password")).unwrap();
        assert!(decrypted.starts_with(b"-----BEGIN RSA PRIVATE KEY-----"));
        let mut reader = BufReader::new(decrypted.as_slice());
        assert!(rustls_pemfile::private_key(&mut reader).unwrap().is_some());
        assert!(decrypt_private_key_pem(pem, Some("wrong-password")).is_err());

        let directory = test_dir();
        let key_path = directory.join("server.key");
        let cert_path = directory.join("server.pem");
        fs::write(&key_path, pem).unwrap();
        fs::write(
            &cert_path,
            include_bytes!("../../tests/fixtures/tls/pbes2-cert.pem"),
        )
        .unwrap();
        let properties = HashMap::from([
            ("tls.test.mode.enable".into(), "false".into()),
            ("tls.client.authServer".into(), "false".into()),
            (
                "tls.client.certPath".into(),
                cert_path.display().to_string(),
            ),
            ("tls.client.keyPath".into(), key_path.display().to_string()),
            ("tls.client.keyPassword".into(), "rust-test-password".into()),
        ]);
        let config = TlsConfig::from_java_properties(&properties, None).unwrap();
        let connector = config.connector();
        assert!(connector.is_ok(), "{}", connector.err().unwrap());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn java_pbes2_3des_encrypted_private_key_is_decrypted_and_loaded() {
        let pem = include_bytes!("../../tests/fixtures/tls/java-pbes2-3des.key");
        let decrypted = decrypt_private_key_pem(pem, Some("1234")).unwrap();
        assert!(decrypted.starts_with(b"-----BEGIN RSA PRIVATE KEY-----"));
        let mut reader = BufReader::new(decrypted.as_slice());
        assert!(rustls_pemfile::private_key(&mut reader).unwrap().is_some());

        let config = TlsConfig {
            test_mode: false,
            client_cert_pem: Some(
                include_bytes!("../../tests/fixtures/tls/java-pbes2-3des-cert.pem").to_vec(),
            ),
            client_key_pem: Some(decrypted),
            ..TlsConfig::default()
        };
        let connector = config.connector();
        assert!(connector.is_ok(), "{}", connector.err().unwrap());
        assert!(decrypt_private_key_pem(pem, Some("wrong-password")).is_err());
    }

    #[test]
    fn unencrypted_private_key_is_preserved() {
        let pem = b"-----BEGIN PRIVATE KEY-----\nAQID\n-----END PRIVATE KEY-----\n";
        assert_eq!(
            decrypt_private_key_pem(pem, Some("irrelevant")).unwrap(),
            pem
        );
    }

    #[test]
    fn java_tls_key_decryption_strategy_receives_client_and_password_metadata() {
        let directory = test_dir();
        fs::write(directory.join("encrypted.key"), b"ciphertext").unwrap();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let observed = calls.clone();
        let strategy: Arc<dyn PrivateKeyDecryptionStrategy> = Arc::new(
            move |path: &std::path::Path, for_client: bool, password: Option<&str>| {
                observed.lock().unwrap().push((
                    path.file_name().unwrap().to_owned(),
                    for_client,
                    password.map(str::to_owned),
                ));
                Ok(b"decrypted-key".to_vec())
            },
        );
        let properties = std::collections::HashMap::from([
            ("tls.test.mode.enable".into(), "false".into()),
            ("tls.client.keyPath".into(), "encrypted.key".into()),
            ("tls.client.keyPassword".into(), "secret".into()),
        ]);
        let config =
            TlsConfig::from_java_properties_with_strategy(&properties, None, Some(strategy))
                .unwrap();
        assert_eq!(
            config.client_key_pem.as_deref(),
            Some(b"decrypted-key".as_slice())
        );
        assert_eq!(
            *calls.lock().unwrap(),
            vec![("encrypted.key".into(), true, Some("secret".into()))]
        );
        fs::remove_dir_all(directory).unwrap();
    }
}
