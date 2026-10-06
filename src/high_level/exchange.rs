/// The type of an exchange.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExchangeKind {
    Direct,
    Fanout,
    Topic,
    Headers,
    /// A plugin-provided type, e.g. `x-consistent-hash`.
    Custom(String),
}

impl ExchangeKind {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Direct => "direct",
            Self::Fanout => "fanout",
            Self::Topic => "topic",
            Self::Headers => "headers",
            Self::Custom(kind) => kind,
        }
    }
}

/// An exchange declared through a [`Client`](crate::Client); declared again
/// after recovery.
#[derive(Clone, Debug)]
pub struct Exchange {
    name: String,
}

impl Exchange {
    pub(crate) fn new(name: &str) -> Self {
        Self { name: name.into() }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl AsRef<str> for Exchange {
    fn as_ref(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kind_as_str() {
        assert_eq!(ExchangeKind::Direct.as_str(), "direct");
        assert_eq!(ExchangeKind::Fanout.as_str(), "fanout");
        assert_eq!(ExchangeKind::Topic.as_str(), "topic");
        assert_eq!(ExchangeKind::Headers.as_str(), "headers");
        assert_eq!(ExchangeKind::Custom("x-foo".into()).as_str(), "x-foo");
    }
}
