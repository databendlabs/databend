// Copyright 2021 Datafuse Labs
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
