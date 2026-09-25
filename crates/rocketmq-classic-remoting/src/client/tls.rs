use std::{io::BufReader, sync::Arc};

use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::{ClientConfig, RootCertStore, pki_types::CertificateDer};

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
}

impl TlsConfig {
    pub fn from_ca_file(path: &std::path::Path) -> Result<Self, TransportError> {
        let ca_pem = std::fs::read(path).map_err(|error| TransportError::Tls(error.to_string()))?;
        Ok(Self {
            ca_pem: Some(ca_pem),
            client_cert_pem: None,
            client_key_pem: None,
        })
    }

    pub(crate) fn connector(&self) -> Result<TlsConnector, TransportError> {
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
        let builder = ClientConfig::builder().with_root_certificates(roots);
        let config = match (&self.client_cert_pem, &self.client_key_pem) {
            (Some(cert_pem), Some(key_pem)) => {
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
            (None, None) => builder.with_no_client_auth(),
            _ => {
                return Err(TransportError::Tls(
                    "client certificate and key must be set together".into(),
                ));
            }
        };
        Ok(TlsConnector::from(Arc::new(config)))
    }
}
