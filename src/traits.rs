use anyhow::Result;
use async_trait::async_trait;
use crate::domain::event::Event;

#[async_trait]
#[allow(dead_code)]
pub trait EventProcessor: Send + Sync {
    async fn process(&mut self, event: Event) -> Result<()>;
    fn can_process(&self) -> bool;
}

#[async_trait]
#[allow(dead_code)]
pub trait HealthCheck: Send + Sync {
    async fn is_healthy(&self) -> bool;
    async fn health_status(&self) -> HealthStatus;
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct HealthStatus {
    pub healthy: bool,
    pub message: String,
    pub details: Option<serde_json::Value>,
}