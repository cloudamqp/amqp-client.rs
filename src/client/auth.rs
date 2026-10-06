use crate::protocol::types::LongStr;

/// Build a SASL PLAIN response: \0username\0password
pub fn plain_response(username: &str, password: &str) -> LongStr {
    let mut buf = Vec::with_capacity(1 + username.len() + 1 + password.len());
    buf.push(0);
    buf.extend_from_slice(username.as_bytes());
    buf.push(0);
    buf.extend_from_slice(password.as_bytes());
    LongStr::from(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_response_format() {
        let resp = plain_response("guest", "guest");
        assert_eq!(resp.as_bytes(), b"\x00guest\x00guest");
    }

    #[test]
    fn plain_response_empty() {
        let resp = plain_response("", "");
        assert_eq!(resp.as_bytes(), b"\x00\x00");
    }
}
