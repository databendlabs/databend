use anyhow::anyhow;
use anyhow::Result;
use serde::de::DeserializeOwned;
use serfig::Parser;

/// Toml parser which used with serfig.
///
/// This parser will ignore unknown fields and call the handler with the path of the unknown field.
pub struct TomlIgnored {
    handler: TomlUnknownFieldHandler,
}

type TomlUnknownFieldHandler = Box<dyn Fn(&str) + Send + Sync + 'static>;

impl TomlIgnored {
    pub fn new(handler: TomlUnknownFieldHandler) -> Self {
        Self { handler }
    }
}

impl std::fmt::Debug for TomlIgnored {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TomlIgnored").finish()
    }
}

impl Parser for TomlIgnored {
    fn parse<T: DeserializeOwned>(&mut self, bs: &[u8]) -> Result<T> {
        let s = std::str::from_utf8(bs)
            .map_err(|err| anyhow!("input value is not valid utf-8: {err:?}"))?;
        let de = toml::Deserializer::new(s);
        let handler = &self.handler;
        Ok(serde_ignored::deserialize(de, move |path| {
            handler(path.to_string().as_str());
        })?)
    }
}
