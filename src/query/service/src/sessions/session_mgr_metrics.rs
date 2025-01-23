use std::sync::Arc;

use prometheus_client::collector::Collector;

use super::SessionManager;

pub struct SessionManagerMetricsCollector {
    session_mgr: Arc<SessionManager>,
}

impl SessionMetricsCollector {
    pub fn new(session_mgr: Arc<SessionManager>) -> Box<Self> {
        Box::new(Self { session_mgr })
    }
}

impl std::fmt::Debug for SessionMetricsCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SessionMetricsCollector")
    }
}

impl Collector for SessionMetricsCollector {
    fn encode(
        &self,
        mut encoder: prometheus_client::encoding::DescriptorEncoder,
    ) -> Result<(), std::fmt::Error> {
        Ok(())
    }
}
