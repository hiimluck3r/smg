//! The probe the controller dials the engines with: one gRPC client per
//! engine family (the client crate's public clients, the same `GetLoads`
//! the gateway asks, answers normalised by the same conversions), kept per
//! endpoint across polls and dropped on a transport failure so the next
//! poll redials, and one HTTP client for the scrape.

use std::{collections::HashMap, time::Duration};

use openai_protocol::worker::{RuntimeType, WorkerLoadResponse};
use parking_lot::Mutex;
use rustls::crypto::ring;
use smg_grpc_client::{
    engine_load::LOAD_SECTIONS, is_transport_failure, SglangSchedulerClient,
    TokenSpeedSchedulerClient, TrtllmServiceClient, VllmEngineClient,
};
use tokio::time::timeout;
use tonic::Code;

use super::loads::{LoadProbe, ReadError, Target};

/// The engines' probe.
pub struct EngineProbe {
    clients: Mutex<HashMap<String, Family>>,
    http: reqwest::Client,
}

/// One connected client, by family.
#[derive(Clone)]
enum Family {
    Sglang(SglangSchedulerClient),
    Vllm(VllmEngineClient),
    Trtllm(TrtllmServiceClient),
    TokenSpeed(TokenSpeedSchedulerClient),
}

impl Family {
    /// A connected client of the target's family, or the reason there is
    /// none: a family without the call, or a failed dial.
    async fn connect(target: &Target) -> Result<Self, ReadError> {
        let endpoint = target.endpoint.as_str();
        let dial = |error: Box<dyn std::error::Error + Send + Sync>| {
            ReadError::Transport(error.to_string())
        };
        match target.kind {
            RuntimeType::Sglang => SglangSchedulerClient::connect(endpoint)
                .await
                .map(Self::Sglang)
                .map_err(dial),
            RuntimeType::Vllm => VllmEngineClient::connect(endpoint)
                .await
                .map(Self::Vllm)
                .map_err(dial),
            RuntimeType::Trtllm => TrtllmServiceClient::connect(endpoint)
                .await
                .map(Self::Trtllm)
                .map_err(dial),
            RuntimeType::TokenSpeed => TokenSpeedSchedulerClient::connect(endpoint)
                .await
                .map(Self::TokenSpeed)
                .map_err(dial),
            kind => Err(ReadError::Unsupported(kind.as_str().to_owned())),
        }
    }

    /// The engine's `GetLoads` answer, normalised.
    async fn get_loads(&self) -> Result<WorkerLoadResponse, tonic::Status> {
        let include = LOAD_SECTIONS
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>();
        match self {
            Self::Sglang(client) => client
                .get_loads(include)
                .await
                .map(WorkerLoadResponse::from),
            Self::Vllm(client) => client
                .get_loads(include)
                .await
                .map(WorkerLoadResponse::from),
            Self::Trtllm(client) => client
                .get_loads(include)
                .await
                .map(WorkerLoadResponse::from),
            Self::TokenSpeed(client) => client
                .get_loads(include)
                .await
                .map(WorkerLoadResponse::from),
        }
    }
}

/// Installs the process TLS crypto provider (ring) when none is installed
/// yet: the HTTP client below and the listeners' TLS both take the process
/// one, and whichever comes first installs it. The gateway, the servicer and
/// the mock worker install the same provider.
pub fn install_crypto_provider() {
    let _ = ring::default_provider().install_default();
}

/// The crate's HTTP client factory: the provider first, then the client,
/// whose timeouts are per call.
pub fn http_client() -> Result<reqwest::Client, reqwest::Error> {
    install_crypto_provider();
    #[expect(
        clippy::disallowed_methods,
        reason = "the crate's client factory: the process TLS crypto provider is installed on the line above"
    )]
    reqwest::Client::builder().build()
}

impl EngineProbe {
    /// A probe with no connection yet.
    pub fn new() -> Result<Self, reqwest::Error> {
        Ok(Self {
            clients: Mutex::new(HashMap::new()),
            http: http_client()?,
        })
    }

    /// The client of the target's endpoint, dialed on the first poll and
    /// kept until a failure drops it.
    async fn client(&self, target: &Target) -> Result<Family, ReadError> {
        if let Some(client) = self.clients.lock().get(&target.endpoint) {
            return Ok(client.clone());
        }
        let client = Family::connect(target).await?;
        self.clients
            .lock()
            .insert(target.endpoint.clone(), client.clone());
        Ok(client)
    }

    /// How many endpoints hold a connection.
    pub fn connections(&self) -> usize {
        self.clients.lock().len()
    }
}

impl LoadProbe for EngineProbe {
    async fn poll(
        &self,
        target: &Target,
        deadline: Duration,
    ) -> Result<WorkerLoadResponse, ReadError> {
        let call = async {
            let client = self.client(target).await?;
            // A failure below gRPC and an engine that could not be reached
            // (the channel's `UNAVAILABLE` over a refused or timed-out dial)
            // are the transport's; any other status is the engine's answer.
            client.get_loads().await.map_err(|status| {
                if is_transport_failure(&status) || status.code() == Code::Unavailable {
                    ReadError::Transport(status.message().to_owned())
                } else {
                    ReadError::Answer(format!("{:?}: {}", status.code(), status.message()))
                }
            })
        };
        let outcome = match timeout(deadline, call).await {
            Ok(outcome) => outcome,
            Err(_elapsed) => Err(ReadError::Deadline(deadline)),
        };
        // A connection that failed or stalled is not asked again: the next
        // poll redials. An answer, even a refusal, keeps it.
        if matches!(
            outcome,
            Err(ReadError::Transport(_) | ReadError::Deadline(_))
        ) {
            self.forget(&target.endpoint);
        }
        outcome
    }

    async fn scrape(&self, url: &str, timeout: Duration) -> Result<String, ReadError> {
        let response = self
            .http
            .get(format!("{}/metrics", url.trim_end_matches('/')))
            .timeout(timeout)
            .send()
            .await
            .map_err(|error| {
                if error.is_timeout() {
                    ReadError::Deadline(timeout)
                } else {
                    ReadError::Transport(error.to_string())
                }
            })?;
        let status = response.status();
        if !status.is_success() {
            return Err(ReadError::Answer(format!("HTTP {status}")));
        }
        response
            .text()
            .await
            .map_err(|error| ReadError::Transport(error.to_string()))
    }

    fn forget(&self, endpoint: &str) {
        self.clients.lock().remove(endpoint);
    }
}
