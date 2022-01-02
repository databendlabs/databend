// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::borrow::Cow;
#[cfg(feature = "tls")]
use std::convert;
use std::fmt;
#[cfg(feature = "tls")]
use std::fmt::Formatter;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

#[cfg(feature = "tls")]
use native_tls;
use url::Url;

use crate::errors::Error;
use crate::errors::Result;
use crate::errors::UrlError;

const DEFAULT_MIN_CONNS: usize = 10;

const DEFAULT_MAX_CONNS: usize = 20;

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
enum State {
    Raw(Options),
    Url(String),
}

#[derive(Clone)]
pub struct OptionsSource {
    state: Arc<Mutex<State>>,
}

impl fmt::Debug for OptionsSource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let guard = self.state.lock().unwrap();
        match *guard {
            State::Url(ref url) => write!(f, "Url({})", url),
            State::Raw(ref options) => write!(f, "{:?}", options),
        }
    }
}

#[allow(dead_code)]
impl OptionsSource {
    pub(crate) fn get(&self) -> Result<Cow<Options>> {
        let mut state = self.state.lock().unwrap();
        loop {
            let new_state = match &*state {
                State::Raw(ref options) => {
                    let ptr = options as *const Options;
                    return unsafe { Ok(Cow::Borrowed(ptr.as_ref().unwrap())) };
                }
                State::Url(url) => {
                    let options = from_url(url)?;
                    State::Raw(options)
                }
            };
            *state = new_state;
        }
    }
}

impl Default for OptionsSource {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::Raw(Options::default()))),
        }
    }
}

pub trait IntoOptions {
    fn into_options_src(self) -> OptionsSource;
}

impl IntoOptions for Options {
    fn into_options_src(self) -> OptionsSource {
        OptionsSource {
            state: Arc::new(Mutex::new(State::Raw(self))),
        }
    }
}

impl IntoOptions for &str {
    fn into_options_src(self) -> OptionsSource {
        OptionsSource {
            state: Arc::new(Mutex::new(State::Url(self.into()))),
        }
    }
}

impl IntoOptions for String {
    fn into_options_src(self) -> OptionsSource {
        OptionsSource {
            state: Arc::new(Mutex::new(State::Url(self))),
        }
    }
}

/// An X509 certificate.
#[cfg(feature = "tls")]
#[derive(Clone)]
pub struct Certificate(Arc<native_tls::Certificate>);

#[cfg(feature = "tls")]
impl Certificate {
    /// Parses a DER-formatted X509 certificate.
    pub fn from_der(der: &[u8]) -> Result<Certificate> {
        let inner = match native_tls::Certificate::from_der(der) {
            Ok(certificate) => certificate,
            Err(err) => return Err(Error::Other(err.to_string().into())),
        };
        Ok(Certificate(Arc::new(inner)))
    }

    /// Parses a PEM-formatted X509 certificate.
    pub fn from_pem(der: &[u8]) -> Result<Certificate> {
        let inner = match native_tls::Certificate::from_pem(der) {
            Ok(certificate) => certificate,
            Err(err) => return Err(Error::Other(err.to_string().into())),
        };
        Ok(Certificate(Arc::new(inner)))
    }
}

#[cfg(feature = "tls")]
impl fmt::Debug for Certificate {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "[Certificate]")
    }
}

#[cfg(feature = "tls")]
impl PartialEq for Certificate {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

#[cfg(feature = "tls")]
impl convert::From<Certificate> for native_tls::Certificate {
    fn from(value: Certificate) -> Self {
        value.0.as_ref().clone()
    }
}

/// Clickhouse connection options.
#[derive(Clone, PartialEq)]
pub struct Options {
    /// Address of clickhouse server (defaults to `127.0.0.1:9000`).
    pub addr: Url,

    /// Database name. (defaults to `default`).
    pub database: String,
    /// User name (defaults to `default`).
    pub username: String,
    /// Access password (defaults to `""`).
    pub password: String,

    /// Enable compression (defaults to `false`).
    pub compression: bool,

    /// Lower bound of opened connections for `Pool` (defaults to 10).
    pub pool_min: usize,
    /// Upper bound of opened connections for `Pool` (defaults to 20).
    pub pool_max: usize,

    /// Whether to enable `TCP_NODELAY` (defaults to `true`).
    pub nodelay: bool,
    /// TCP keep alive timeout in milliseconds (defaults to `None`).
    pub keepalive: Option<Duration>,

    /// Ping server every time before execute any query. (defaults to `true`)
    pub ping_before_query: bool,
    /// Count of retry to send request to server. (defaults to `3`)
    pub send_retries: usize,
    /// Amount of time to wait before next retry. (defaults to `5 sec`)
    pub retry_timeout: Duration,
    /// Timeout for ping (defaults to `500 ms`)
    pub ping_timeout: Duration,

    /// Timeout for connection (defaults to `500 ms`)
    pub connection_timeout: Duration,

    /// Timeout for queries (defaults to `180 sec`)
    pub query_timeout: Duration,

    /// Timeout for inserts (defaults to `180 sec`)
    pub insert_timeout: Option<Duration>,

    /// Timeout for execute (defaults to `180 sec`)
    pub execute_timeout: Option<Duration>,

    /// Enable TLS encryption (defaults to `false`)
    #[cfg(feature = "tls")]
    pub secure: bool,

    /// Skip certificate verification (default is `false`).
    #[cfg(feature = "tls")]
    pub skip_verify: bool,

    /// An X509 certificate.
    #[cfg(feature = "tls")]
    pub certificate: Option<Certificate>,

    /// Restricts permissions for read data, write data and change settings queries.
    pub readonly: Option<u8>,

    /// Comma separated list of single address host for load-balancing.
    pub alt_hosts: Vec<Url>,
}

impl fmt::Debug for Options {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Options")
            .field("addr", &self.addr)
            .field("database", &self.database)
            .field("compression", &self.compression)
            .field("pool_min", &self.pool_min)
            .field("pool_max", &self.pool_max)
            .field("nodelay", &self.nodelay)
            .field("keepalive", &self.keepalive)
            .field("ping_before_query", &self.ping_before_query)
            .field("send_retries", &self.send_retries)
            .field("retry_timeout", &self.retry_timeout)
            .field("ping_timeout", &self.ping_timeout)
            .field("connection_timeout", &self.connection_timeout)
            .field("readonly", &self.readonly)
            .field("alt_hosts", &self.alt_hosts)
            .finish()
    }
}

impl Default for Options {
    fn default() -> Self {
        Self {
            addr: Url::parse("tcp://default@127.0.0.1:9000").unwrap(),
            database: "default".into(),
            username: "default".into(),
            password: "".into(),
            compression: false,
            pool_min: DEFAULT_MIN_CONNS,
            pool_max: DEFAULT_MAX_CONNS,
            nodelay: true,
            keepalive: None,
            ping_before_query: true,
            send_retries: 3,
            retry_timeout: Duration::from_secs(5),
            ping_timeout: Duration::from_millis(500),
            connection_timeout: Duration::from_millis(500),
            query_timeout: Duration::from_secs(180),
            insert_timeout: Some(Duration::from_secs(180)),
            execute_timeout: Some(Duration::from_secs(180)),
            #[cfg(feature = "tls")]
            secure: false,
            #[cfg(feature = "tls")]
            skip_verify: false,
            #[cfg(feature = "tls")]
            certificate: None,
            readonly: None,
            alt_hosts: Vec::new(),
        }
    }
}

macro_rules! property {
    ( $k:ident: $t:ty ) => {
        pub fn $k(self, $k: $t) -> Self {
            Self {
                $k: $k.into(),
                ..self
            }
        }
    };
    ( $(#[$attr:meta])* => $k:ident: $t:ty ) => {
        $(#[$attr])*
        #[must_use]
        pub fn $k(self, $k: $t) -> Self {
            Self {
                $k: $k.into(),
                ..self
            }
        }
    }
}

impl Options {
    /// Constructs a new Options.
    pub fn new<A>(addr: A) -> Self
    where A: Into<Url> {
        Self {
            addr: addr.into(),
            ..Self::default()
        }
    }

    property! {
        /// Database name. (defaults to `default`).
        => database: &str
    }

    property! {
        /// User name (defaults to `default`).
        => username: &str
    }

    property! {
        /// Access password (defaults to `""`).
        => password: &str
    }

    /// Enable compression (defaults to `false`).
    #[must_use]
    pub fn with_compression(self) -> Self {
        Self {
            compression: true,
            ..self
        }
    }

    property! {
        /// Lower bound of opened connections for `Pool` (defaults to `10`).
        => pool_min: usize
    }

    property! {
        /// Upper bound of opened connections for `Pool` (defaults to `20`).
        => pool_max: usize
    }

    property! {
        /// Whether to enable `TCP_NODELAY` (defaults to `true`).
        => nodelay: bool
    }

    property! {
        /// TCP keep alive timeout in milliseconds (defaults to `None`).
        => keepalive: Option<Duration>
    }

    property! {
        /// Ping server every time before execute any query. (defaults to `true`).
        => ping_before_query: bool
    }

    property! {
        /// Count of retry to send request to server. (defaults to `3`).
        => send_retries: usize
    }

    property! {
        /// Amount of time to wait before next retry. (defaults to `5 sec`).
        => retry_timeout: Duration
    }

    property! {
        /// Timeout for ping (defaults to `500 ms`).
        => ping_timeout: Duration
    }

    property! {
        /// Timeout for connection (defaults to `500 ms`).
        => connection_timeout: Duration
    }

    property! {
        /// Timeout for query (defaults to `180,000 ms`).
        => query_timeout: Duration
    }

    property! {
        /// Timeout for insert (defaults to `180,000 ms`).
        => insert_timeout: Option<Duration>
    }

    property! {
        /// Timeout for execute (defaults to `180 sec`).
        => execute_timeout: Option<Duration>
    }

    #[cfg(feature = "tls")]
    property! {
        /// Establish secure connection (default is `false`).
        => secure: bool
    }

    #[cfg(feature = "tls")]
    property! {
        /// Skip certificate verification (default is `false`).
        => skip_verify: bool
    }

    #[cfg(feature = "tls")]
    property! {
        /// An X509 certificate.
        => certificate: Option<Certificate>
    }

    property! {
        /// Restricts permissions for read data, write data and change settings queries.
        => readonly: Option<u8>
    }

    property! {
        /// Comma separated list of single address host for load-balancing.
        => alt_hosts: Vec<Url>
    }
}

impl FromStr for Options {
    type Err = Error;

    fn from_str(url: &str) -> Result<Self> {
        from_url(url)
    }
}

pub fn from_url(url_str: &str) -> Result<Options> {
    let url = Url::parse(url_str)?;

    if url.scheme() != "tcp" {
        return Err(UrlError::UnsupportedScheme {
            scheme: url.scheme().to_string(),
        }
        .into());
    }

    if url.cannot_be_a_base() || !url.has_host() {
        return Err(UrlError::Invalid.into());
    }

    let mut options = Options::default();

    if let Some(username) = get_username_from_url(&url) {
        options.username = username.into();
    }

    if let Some(password) = get_password_from_url(&url) {
        options.password = password.into()
    }

    let mut addr = url.clone();
    addr.set_path("");
    addr.set_query(None);

    let port = url.port().or(Some(9000));
    addr.set_port(port).map_err(|_| UrlError::Invalid)?;
    options.addr = addr;

    if let Some(database) = get_database_from_url(&url)? {
        options.database = database.into();
    }

    set_params(&mut options, url.query_pairs())?;

    Ok(options)
}

fn set_params<'a, I>(options: &mut Options, iter: I) -> std::result::Result<(), UrlError>
where I: Iterator<Item = (Cow<'a, str>, Cow<'a, str>)> {
    for (key, value) in iter {
        match key.as_ref() {
            "pool_min" => options.pool_min = parse_param(key, value, usize::from_str)?,
            "pool_max" => options.pool_max = parse_param(key, value, usize::from_str)?,
            "nodelay" => options.nodelay = parse_param(key, value, bool::from_str)?,
            "keepalive" => options.keepalive = parse_param(key, value, parse_opt_duration)?,
            "ping_before_query" => {
                options.ping_before_query = parse_param(key, value, bool::from_str)?
            }
            "send_retries" => options.send_retries = parse_param(key, value, usize::from_str)?,
            "retry_timeout" => options.retry_timeout = parse_param(key, value, parse_duration)?,
            "ping_timeout" => options.ping_timeout = parse_param(key, value, parse_duration)?,
            "connection_timeout" => {
                options.connection_timeout = parse_param(key, value, parse_duration)?
            }
            "query_timeout" => options.query_timeout = parse_param(key, value, parse_duration)?,
            "insert_timeout" => {
                options.insert_timeout = parse_param(key, value, parse_opt_duration)?
            }
            "execute_timeout" => {
                options.execute_timeout = parse_param(key, value, parse_opt_duration)?
            }
            "compression" => options.compression = parse_param(key, value, parse_compression)?,
            #[cfg(feature = "tls")]
            "secure" => options.secure = parse_param(key, value, bool::from_str)?,
            #[cfg(feature = "tls")]
            "skip_verify" => options.skip_verify = parse_param(key, value, bool::from_str)?,
            "readonly" => options.readonly = parse_param(key, value, parse_opt_u8)?,
            "alt_hosts" => options.alt_hosts = parse_param(key, value, parse_hosts)?,
            _ => return Err(UrlError::UnknownParameter { param: key.into() }),
        };
    }

    Ok(())
}

fn parse_param<'a, F, T, E>(
    param: Cow<'a, str>,
    value: Cow<'a, str>,
    parse: F,
) -> std::result::Result<T, UrlError>
where
    F: Fn(&str) -> std::result::Result<T, E>,
{
    match parse(value.as_ref()) {
        Ok(value) => Ok(value),
        Err(_) => Err(UrlError::InvalidParamValue {
            param: param.into(),
            value: value.into(),
        }),
    }
}

pub fn get_username_from_url(url: &Url) -> Option<&str> {
    let user = url.username();
    if user.is_empty() {
        return None;
    }
    Some(user)
}

pub fn get_password_from_url(url: &Url) -> Option<&str> {
    url.password()
}

pub fn get_database_from_url(url: &Url) -> Result<Option<&str>> {
    match url.path_segments() {
        None => Ok(None),
        Some(mut segments) => {
            let head = segments.next();

            if segments.next().is_some() {
                return Err(Error::Url(UrlError::Invalid));
            }

            match head {
                Some(database) if !database.is_empty() => Ok(Some(database)),
                _ => Ok(None),
            }
        }
    }
}

#[allow(clippy::result_unit_err)]
pub fn parse_duration(source: &str) -> std::result::Result<Duration, ()> {
    let digits_count = source.chars().take_while(|c| c.is_digit(10)).count();

    let left: String = source.chars().take(digits_count).collect();
    let right: String = source.chars().skip(digits_count).collect();

    let num = match u64::from_str(&left) {
        Ok(value) => value,
        Err(_) => return Err(()),
    };

    match right.as_str() {
        "s" => Ok(Duration::from_secs(num)),
        "ms" => Ok(Duration::from_millis(num)),
        _ => Err(()),
    }
}

#[allow(clippy::result_unit_err)]
pub fn parse_opt_duration(source: &str) -> std::result::Result<Option<Duration>, ()> {
    if source == "none" {
        return Ok(None);
    }

    let duration = parse_duration(source)?;
    Ok(Some(duration))
}

#[allow(clippy::result_unit_err)]
pub fn parse_opt_u8(source: &str) -> std::result::Result<Option<u8>, ()> {
    if source == "none" {
        return Ok(None);
    }

    let duration: u8 = match source.parse() {
        Ok(value) => value,
        Err(_) => return Err(()),
    };

    Ok(Some(duration))
}

#[allow(clippy::result_unit_err)]
pub fn parse_compression(source: &str) -> std::result::Result<bool, ()> {
    match source {
        "none" => Ok(false),
        "lz4" => Ok(true),
        _ => Err(()),
    }
}

#[allow(clippy::result_unit_err)]
pub fn parse_hosts(source: &str) -> std::result::Result<Vec<Url>, ()> {
    let mut result = Vec::new();
    for host in source.split(',') {
        match Url::from_str(&format!("tcp://{}", host)) {
            Ok(url) => result.push(url),
            Err(_) => return Err(()),
        }
    }
    Ok(result)
}
