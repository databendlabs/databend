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

use std::fmt::Display;
use std::fmt::Formatter;

use databend_meta_kvapi::kvapi::KeyBuilder;
use databend_meta_kvapi::kvapi::KeyCodec;
use databend_meta_kvapi::kvapi::KeyError;
use databend_meta_kvapi::kvapi::KeyParser;

/// Uniquely identifies a procedure with a name and a args vec(string).
#[derive(Clone, Debug, Eq, PartialEq, Hash, Default)]
pub struct ProcedureIdentity {
    pub name: String,
    pub args: String,
}

impl ProcedureIdentity {
    const ESCAPE_CHARS: [u8; 1] = [b'\''];

    pub fn new(name: impl ToString, args: impl ToString) -> Self {
        Self {
            name: name.to_string(),
            args: args.to_string(),
        }
    }
}

impl Display for ProcedureIdentity {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}({})",
            KeyBuilder::escape_specified(&self.name, &Self::ESCAPE_CHARS),
            KeyBuilder::escape_specified(&self.args, &Self::ESCAPE_CHARS),
        )
    }
}

impl KeyCodec for ProcedureIdentity {
    fn encode_key(&self, b: KeyBuilder) -> KeyBuilder {
        b.push_str(&self.name).push_str(&self.args)
    }

    fn decode_key(parser: &mut KeyParser) -> Result<Self, KeyError>
    where Self: Sized {
        let name = parser.next_str()?;
        let args = parser.next_str()?;
        Ok(Self { name, args })
    }
}

impl From<databend_common_ast::ast::ProcedureIdentity> for ProcedureIdentity {
    fn from(procedure: databend_common_ast::ast::ProcedureIdentity) -> Self {
        let args_type_str = procedure
            .args_type
            .iter()
            .map(|t| t.to_string())
            .collect::<Vec<_>>()
            .join(",");
        ProcedureIdentity::new(procedure.name, args_type_str)
    }
}
