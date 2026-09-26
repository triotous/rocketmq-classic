use std::{io::BufReader, sync::Arc};

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

#[derive(Clone, Debug)]
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
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            ca_pem: None,
            client_cert_pem: None,
            client_key_pem: None,
            test_mode: true,
            auth_server: false,
        }
    }
}

impl TlsConfig {
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
        let config = match (!self.test_mode, &self.client_cert_pem, &self.client_key_pem) {
            (true, Some(cert_pem), Some(key_pem)) => {
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
            (false, _, _) | (true, None, None) => builder.with_no_client_auth(),
            _ => {
                return Err(TransportError::Tls(
                    "client certificate and key must be set together".into(),
                ));
            }
        };
        Ok(TlsConnector::from(Arc::new(config)))
    }
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
