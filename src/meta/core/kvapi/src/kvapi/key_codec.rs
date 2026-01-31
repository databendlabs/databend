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

use crate::kvapi;
use crate::kvapi::KeyError;

/// Encode or decode part of a meta-service key.
pub trait KeyCodec {
    /// Encode fields of the structured key into a key builder.
    fn encode_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder;

    /// Decode fields of the structured key from a key parser.
    fn decode_key(parser: &mut kvapi::KeyParser) -> Result<Self, KeyError>
    where Self: Sized;
}

mod impls {
    use crate::kvapi::KeyBuilder;
    use crate::kvapi::KeyCodec;
    use crate::kvapi::KeyError;
    use crate::kvapi::KeyParser;

    impl KeyCodec for String {
        fn encode_key(&self, b: KeyBuilder) -> KeyBuilder {
            b.push_str(self)
        }

        fn decode_key(p: &mut KeyParser) -> Result<Self, KeyError>
        where Self: Sized {
            let s = p.next_str()?;
            Ok(s)
        }
    }

    impl KeyCodec for u64 {
        fn encode_key(&self, b: KeyBuilder) -> KeyBuilder {
            b.push_u64(*self)
        }

        fn decode_key(p: &mut KeyParser) -> Result<Self, KeyError>
        where Self: Sized {
            let s = p.next_u64()?;
            Ok(s)
        }
    }

    impl KeyCodec for () {
        fn encode_key(&self, b: KeyBuilder) -> KeyBuilder {
            b
        }

        fn decode_key(_p: &mut KeyParser) -> Result<Self, KeyError>
        where Self: Sized {
            Ok(())
        }
    }
}
