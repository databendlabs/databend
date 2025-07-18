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

use std::fmt::Debug;
use std::marker::PhantomData;

use log::debug;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_bridge::IntoValue;
use serde_bridge::Value;
use serfig::collectors::IntoCollector;
use serfig::Collector;

pub fn from_env<V>() -> Environment<V>
where V: DeserializeOwned + Serialize + Debug {
    Environment {
        _v: Default::default(),
    }
}

#[derive(Debug)]
pub struct Environment<V: DeserializeOwned + Serialize + Debug> {
    _v: PhantomData<V>,
}

impl<V> Collector<V> for Environment<V>
where V: DeserializeOwned + Serialize + Debug
{
    fn collect(&mut self) -> anyhow::Result<Value> {
        let v: V = serde_env::from_env()?;
        debug!("value parsed from env: {:?}", v);
        Ok(v.into_value()?)
    }
}

impl<V> IntoCollector<V> for Environment<V>
where V: DeserializeOwned + Serialize + Debug + 'static
{
    fn into_collector(self) -> Box<dyn Collector<V>> {
        Box::new(self)
    }
}

#[cfg(test)]
mod tests {
    use crate::Config;

    #[test]
    fn test_env() {
        // see https://github.com/Xuanwo/serde-env/pull/48
        let cfg: Config = serde_env::from_iter([
            ("LOG_TRACING_OTLP_ENDPOINT", "http://127.0.2.1:1111"),
            ("LOG_TRACING_CAPTURE_LOG_LEVEL", "DebuG"),
        ])
        .unwrap();

        assert_eq!(
            cfg.log.tracing.tracing_otlp.endpoint,
            "http://127.0.2.1:1111"
        );
        assert_eq!(cfg.log.tracing.tracing_capture_log_level, "DebuG");
    }
}
