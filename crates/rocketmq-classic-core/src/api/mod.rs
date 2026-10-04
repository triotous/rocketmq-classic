pub mod route;

use rocketmq_classic_remoting::client::RemotingClient;

#[derive(Clone)]
pub struct MQClientApi {
    remoting: RemotingClient,
}

impl MQClientApi {
    pub fn new(remoting: RemotingClient) -> Self {
        Self { remoting }
    }
}
