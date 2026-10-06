use std::fmt;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Protocol error: {0}")]
    Protocol(#[from] ProtocolError),

    #[error("Server closed connection: code={code}, reason={reason}")]
    ConnectionClosed { code: u16, reason: String },

    #[error("Server closed channel {channel_id}: code={code}, reason={reason}")]
    ChannelClosed {
        channel_id: u16,
        code: u16,
        reason: String,
    },

    #[error("Connection timeout")]
    Timeout,

    #[error("Invalid URI: {0}")]
    InvalidUri(String),

    #[error("Authentication failed: {0}")]
    AuthFailed(String),

    #[error("Frame too large: {size} > {max}")]
    FrameTooLarge { size: u32, max: u32 },

    #[error("Channel limit reached")]
    ChannelLimitReached,

    #[error("Codec error: {0}")]
    Codec(String),

    #[error("Channel.Flow has held publishes suspended longer than allowed")]
    FlowSuspendedTooLong,

    #[error("Channel::returns() has already been called on this channel")]
    ReturnsAlreadySubscribed,

    #[error("publisher confirms not enabled — call Channel::confirm_select first")]
    ConfirmsNotEnabled,
}

#[derive(Debug)]
pub enum ProtocolError {
    InvalidFrameType(u8),
    InvalidFrameEnd(u8),
    UnknownMethod { class_id: u16, method_id: u16 },
    UnexpectedMethod { expected: &'static str, got: String },
    InsufficientData { needed: usize, available: usize },
    InvalidFieldType(u8),
    ShortStrTooLong(usize),
    InvalidUtf8,
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidFrameType(t) => write!(f, "invalid frame type: {t}"),
            Self::InvalidFrameEnd(b) => write!(f, "invalid frame end: expected 0xCE, got {b:#x}"),
            Self::UnknownMethod {
                class_id,
                method_id,
            } => write!(f, "unknown method: class={class_id}, method={method_id}"),
            Self::UnexpectedMethod { expected, got } => {
                write!(f, "unexpected method: expected {expected}, got {got}")
            }
            Self::InsufficientData { needed, available } => {
                write!(
                    f,
                    "insufficient data: need {needed} bytes, have {available}"
                )
            }
            Self::InvalidFieldType(t) => write!(f, "invalid table field type: {t:#x}"),
            Self::ShortStrTooLong(len) => {
                write!(f, "short string too long: {len} bytes (max 255)")
            }
            Self::InvalidUtf8 => write!(f, "invalid UTF-8 in string"),
        }
    }
}

impl std::error::Error for ProtocolError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
        let err: Error = io_err.into();
        assert!(matches!(err, Error::Io(_)));
    }

    #[test]
    fn error_from_protocol() {
        let proto_err = ProtocolError::InvalidUtf8;
        let err: Error = proto_err.into();
        assert!(matches!(err, Error::Protocol(_)));
    }

    #[test]
    fn protocol_error_display() {
        let err = ProtocolError::ShortStrTooLong(300);
        let msg = err.to_string();
        assert!(msg.contains("300"));
    }

    #[test]
    fn error_codec() {
        let err = Error::Codec("bad json".into());
        let msg = err.to_string();
        assert!(msg.contains("bad json"));
    }
}
