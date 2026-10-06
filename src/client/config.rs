use std::time::Duration;

use url::Url;

use crate::error::Error;

/// Connection configuration parsed from an AMQP URI or built manually.
#[derive(Clone, Debug)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub vhost: String,
    pub channel_max: u16,
    pub frame_max: u32,
    pub heartbeat: u16,
    pub tls: bool,
    /// Maximum total time allowed for the AMQP handshake (TCP connect through
    /// `Connection.OpenOk`). A stalled broker that accepts the TCP connection
    /// but never responds will be aborted at this limit.
    pub connect_timeout: Duration,
    /// Per-RPC timeout for synchronous channel methods (queue_declare,
    /// exchange_declare, basic_get, etc.). The library cleans up its own
    /// in-flight slot when this fires, so the next RPC isn't blocked.
    pub rpc_timeout: Duration,
    /// Maximum bytes of publish content allowed in-flight (queued in the
    /// writer mpsc + kernel send buffer + on the wire) before
    /// `basic_publish` and `basic_publish_confirmed` start blocking.
    /// Backpressure: publishers wait for the writer to flush bytes onto
    /// the socket before sending more.
    ///
    /// Acks, heartbeats, and RPC method frames are NOT throttled — they
    /// flow through the same unbounded mpsc and bypass the semaphore.
    /// Bodies larger than this cap acquire the full semaphore (becoming
    /// the only in-flight publish until they finish), rather than
    /// deadlocking.
    pub publish_admission_bytes: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: "localhost".into(),
            port: 5672,
            username: "guest".into(),
            password: "guest".into(),
            vhost: "/".into(),
            channel_max: 0, // 0 = no limit, server decides
            frame_max: 0,   // 0 = no limit, server decides
            heartbeat: 0,   // 0 = no limit, server decides
            tls: false,
            connect_timeout: Duration::from_secs(60),
            rpc_timeout: Duration::from_secs(30),
            publish_admission_bytes: 16 * 1024 * 1024, // 16 MiB
        }
    }
}

impl Config {
    /// Parse an AMQP URI into a Config.
    ///
    /// Supported formats:
    /// - `amqp://user:pass@host:port/vhost`
    /// - `amqp://host` (defaults: guest/guest, port 5672, vhost /)
    /// - `amqp://host/vhost`
    pub fn from_uri(uri: &str) -> Result<Self, Error> {
        let url = Url::parse(uri).map_err(|e| Error::InvalidUri(e.to_string()))?;

        let scheme = url.scheme();
        if scheme != "amqp" && scheme != "amqps" {
            return Err(Error::InvalidUri(format!("unsupported scheme: {scheme}")));
        }

        let tls = scheme == "amqps";
        let default_port = if tls { 5671 } else { 5672 };

        let host = url.host_str().unwrap_or("localhost").to_string();
        let port = url.port().unwrap_or(default_port);

        let username = if url.username().is_empty() {
            "guest".to_string()
        } else {
            percent_decode(url.username())
        };

        let password = url
            .password()
            .map(percent_decode)
            .unwrap_or_else(|| "guest".to_string());

        // The vhost is the path minus the leading slash.
        // An empty path or "/" means vhost "/".
        // "/myvhost" means vhost "myvhost".
        let vhost = match url.path() {
            "" | "/" => "/".to_string(),
            path => percent_decode(&path[1..]),
        };

        Ok(Self {
            host,
            port,
            username,
            password,
            vhost,
            tls,
            ..Default::default()
        })
    }
}

fn percent_decode(s: &str) -> String {
    percent_encoding::percent_decode_str(s)
        .decode_utf8_lossy()
        .into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_full_uri() {
        let cfg = Config::from_uri("amqp://user:pass@myhost:5673/myvhost").unwrap();
        assert_eq!(cfg.host, "myhost");
        assert_eq!(cfg.port, 5673);
        assert_eq!(cfg.username, "user");
        assert_eq!(cfg.password, "pass");
        assert_eq!(cfg.vhost, "myvhost");
    }

    #[test]
    fn parse_minimal_uri() {
        let cfg = Config::from_uri("amqp://localhost").unwrap();
        assert_eq!(cfg.host, "localhost");
        assert_eq!(cfg.port, 5672);
        assert_eq!(cfg.username, "guest");
        assert_eq!(cfg.password, "guest");
        assert_eq!(cfg.vhost, "/");
    }

    #[test]
    fn parse_uri_with_default_vhost() {
        let cfg = Config::from_uri("amqp://user:pass@host/").unwrap();
        assert_eq!(cfg.vhost, "/");
    }

    #[test]
    fn parse_uri_with_encoded_vhost() {
        let cfg = Config::from_uri("amqp://localhost/%2F").unwrap();
        assert_eq!(cfg.vhost, "/");
    }

    #[test]
    fn parse_invalid_scheme() {
        assert!(Config::from_uri("http://localhost").is_err());
    }

    #[test]
    fn default_config() {
        let cfg = Config::default();
        assert_eq!(cfg.host, "localhost");
        assert_eq!(cfg.port, 5672);
        assert_eq!(cfg.username, "guest");
        assert_eq!(cfg.password, "guest");
        assert_eq!(cfg.vhost, "/");
    }

    #[test]
    fn parse_uri_only_host_and_vhost() {
        let cfg = Config::from_uri("amqp://localhost/production").unwrap();
        assert_eq!(cfg.host, "localhost");
        assert_eq!(cfg.vhost, "production");
        assert_eq!(cfg.username, "guest");
        assert_eq!(cfg.password, "guest");
    }

    #[test]
    fn parse_uri_with_port_no_vhost() {
        let cfg = Config::from_uri("amqp://localhost:5673").unwrap();
        assert_eq!(cfg.port, 5673);
        assert_eq!(cfg.vhost, "/");
    }

    #[test]
    fn parse_amqps_scheme() {
        let cfg = Config::from_uri("amqps://localhost").unwrap();
        assert_eq!(cfg.host, "localhost");
        assert_eq!(cfg.port, 5671);
        assert!(cfg.tls);
    }

    #[test]
    fn parse_amqp_not_tls() {
        let cfg = Config::from_uri("amqp://localhost").unwrap();
        assert!(!cfg.tls);
        assert_eq!(cfg.port, 5672);
    }

    #[test]
    fn parse_amqps_explicit_port() {
        let cfg = Config::from_uri("amqps://localhost:5672").unwrap();
        assert_eq!(cfg.port, 5672);
        assert!(cfg.tls);
    }

    #[test]
    fn default_config_no_tls() {
        let cfg = Config::default();
        assert!(!cfg.tls);
    }

    #[test]
    fn parse_uri_not_a_uri() {
        assert!(Config::from_uri("not a uri at all").is_err());
    }

    #[test]
    fn parse_uri_empty() {
        assert!(Config::from_uri("").is_err());
    }

    #[test]
    fn parse_uri_with_special_password() {
        let cfg = Config::from_uri("amqp://user:p%40ss%23word@host").unwrap();
        assert_eq!(cfg.username, "user");
        assert_eq!(cfg.password, "p@ss#word");
    }

    #[test]
    fn parse_uri_password_with_ampersand_and_equals() {
        // The previous form_urlencoded-based decoder split on `&` and `=`,
        // mangling passwords that contained either. Verify literal preservation.
        let cfg = Config::from_uri("amqp://user:p%40ss%26word%3D1@host").unwrap();
        assert_eq!(cfg.password, "p@ss&word=1");
    }

    #[test]
    fn parse_uri_username_percent_decoded() {
        let cfg = Config::from_uri("amqp://us%40er:pw@host").unwrap();
        assert_eq!(cfg.username, "us@er");
    }

    #[test]
    fn default_negotiation_params() {
        let cfg = Config::default();
        assert_eq!(cfg.channel_max, 0);
        assert_eq!(cfg.frame_max, 0);
        assert_eq!(cfg.heartbeat, 0);
    }
}
