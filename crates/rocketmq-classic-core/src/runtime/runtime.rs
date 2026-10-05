use std::{collections::HashMap, sync::Arc};

use rocketmq_classic_remoting::client::{
    ClientConfig as TransportConfig, RemotingClient, RpcHook, TransportMode,
};

use super::RuntimeError;
use crate::{api::MQClientApi, config::ClientConfig};

/// A manager-owned transport and typed API. Construction does not start tasks.
pub struct ClientRuntime {
    config: ClientConfig,
    client_id: String,
    remoting: RemotingClient,
    api: MQClientApi,
    routes: crate::route::RouteCache,
}

impl ClientRuntime {
    pub(super) fn construct(
        config: ClientConfig,
        properties: &HashMap<String, String>,
        hook: Option<Arc<dyn RpcHook>>,
    ) -> Result<Self, RuntimeError> {
        let callback_executor_threads = usize::try_from(config.client_callback_executor_threads)
            .map_err(|_| {
                rocketmq_classic_remoting::TransportError::InvalidConfiguration(
                    "client callback executor thread count must be nonnegative".into(),
                )
            })?;
        let transport = if config.use_tls {
            let mut properties = properties.clone();
            properties.insert("tls.enable".into(), "true".into());
            TransportMode::from_java_properties(&properties, None)?
        } else {
            TransportMode::Plain
        };
        let remoting = RemotingClient::with_config(TransportConfig {
            transport,
            callback_executor_threads,
            socks_proxy_config: config.socks_proxy_config.clone(),
            scan_available_nameserver: false,
            ..TransportConfig::default()
        })?;
        if let Some(addresses) = &config.namesrv_addr {
            remoting
                .update_name_server_address_list(addresses.split(';').map(str::to_owned).collect());
        }
        if let Some(hook) = hook {
            remoting.add_rpc_hook(hook);
        }
        Ok(Self {
            client_id: config.build_client_id(),
            config,
            api: MQClientApi::new(remoting.clone()),
            remoting,
            routes: crate::route::RouteCache::default(),
        })
    }

    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    pub fn api(&self) -> &MQClientApi {
        &self.api
    }

    pub fn remoting(&self) -> &RemotingClient {
        &self.remoting
    }

    pub fn route_snapshot(&self) -> Result<Arc<crate::route::RouteSnapshot>, RuntimeError> {
        self.routes.snapshot()
    }

    pub async fn update_topic_route(
        &self,
        topic: &str,
        deadline: tokio::time::Instant,
    ) -> Result<bool, RuntimeError> {
        let route = self.api.get_topic_route(topic, deadline).await?;
        self.routes.replace(topic, route)
    }
}
