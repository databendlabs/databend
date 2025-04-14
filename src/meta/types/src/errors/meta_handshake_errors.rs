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

use std::fmt;

use anyerror::AnyError;

/// Error raised by meta service client.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub struct MetaHandshakeError {
    msg: String,
    #[source]
    source: Option<AnyError>,
}

impl fmt::Display for MetaHandshakeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HandshakeError with databend-meta: {}", self.msg,)?;

        if let Some(ref source) = self.source {
            write!(f, "; cause: {}", source)?;
        }

        Ok(())
    }
}

impl MetaHandshakeError {
    pub fn new(msg: impl fmt::Display) -> Self {
        Self {
            msg: msg.to_string(),
            source: None,
        }
    }

    pub fn with_source(self, source: &(impl std::error::Error + 'static)) -> Self {
        Self {
            msg: self.msg,
            source: Some(AnyError::new(source)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_meta_handshake_error() {
        let error = MetaHandshakeError::new("test");
        assert_eq!(error.to_string(), "HandshakeError with databend-meta: test");

        let error = error.with_source(&std::io::Error::other("test"));
        assert_eq!(
            error.to_string(),
            "HandshakeError with databend-meta: test; cause: std::io::error::Error: test"
        );
    }
}
