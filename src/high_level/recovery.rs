use std::time::Duration;

/// Opt-in automatic recovery for a [`Client`](crate::Client) (ADR-0002).
#[derive(Clone, Debug)]
pub struct RecoveryConfig {
    /// The wait between failed recovery attempts, and the longest a new
    /// operation waits for recovery in progress. Default 250 ms.
    pub timeout: Duration,
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
