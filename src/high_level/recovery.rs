use std::time::Duration;

/// Opt-in automatic recovery for a [`Client`](crate::Client) (ADR-0002).
#[derive(Clone, Debug)]
pub struct RecoveryConfig {
    /// The wait between failed recovery attempts (at least 100 ms), and the
    /// longest a new operation waits for recovery in progress. Default
    /// 250 ms.
    pub timeout: Duration,
}

impl RecoveryConfig {
    /// The wait between failed recovery attempts: the timeout, but never so
    /// short that recovery becomes a tight connect loop.
    pub(crate) fn retry_delay(&self) -> Duration {
        self.timeout.max(Duration::from_millis(100))
    }
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_millis(250),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_timeout_is_250ms() {
        assert_eq!(
            RecoveryConfig::default().timeout,
            Duration::from_millis(250)
        );
    }
}
