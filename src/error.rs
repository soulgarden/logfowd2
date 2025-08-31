use std::time::Duration;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("Configuration error: {0}")]
    Config(String),

    #[error("File system error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Notification error: {0}")]
    Notify(#[from] notify::Error),

    #[error("Metrics initialization failed: {0}")]
    Metrics(#[from] prometheus::Error),

    #[error("Elasticsearch error: {0}")]
    Elasticsearch(#[from] EsError),

    #[error("Component failed to start: {component}")]
    ComponentStartup { component: String },

    #[error("Task join error: {0}")]
    TaskJoin(String),
}

#[derive(Error, Debug, Clone)]
pub enum EsError {
    #[error("ES request failed: {0}")]
    RequestFailed(String),

    #[error("ES request timed out")]
    Timeout,

    #[error("Serialization failed: {0}")]
    SerializationFailed(String),

    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    #[error("DNS resolution failed: {0}")]
    DnsResolutionFailed(String),

    #[error("TLS handshake failed: {0}")]
    TlsHandshakeFailed(String),

    #[error("HTTP {status} error: {body}")]
    HttpStatusError { status: u16, body: String },

    #[error("Network unreachable: {0}")]
    NetworkUnreachable(String),

    #[error("Rate limited{}", match .retry_after {
        Some(duration) => format!(", retry after {}s", duration.as_secs()),
        None => String::new(),
    })]
    RateLimited { retry_after: Option<Duration> },

    #[error("Elasticsearch service unavailable")]
    ServiceUnavailable,
}

impl From<tokio::task::JoinError> for AppError {
    fn from(error: tokio::task::JoinError) -> Self {
        AppError::TaskJoin(error.to_string())
    }
}

pub type Result<T> = std::result::Result<T, AppError>;
