use std::convert::Infallible;

use crate::kvapi;
use crate::kvapi::Key;
use crate::kvapi::KeyCodec;
use crate::kvapi::KeyError;
use crate::kvapi::KeyParser;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FooKey {
    pub(crate) a: u64,
    pub(crate) b: String,
    pub(crate) c: u64,
}

impl KeyCodec for FooKey {
    fn encode_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder {
        b.push_u64(self.a).push_str(&self.b).push_u64(self.c)
    }

    fn decode_key(parser: &mut KeyParser) -> Result<Self, KeyError>
    where Self: Sized {
        let a = parser.next_u64()?;
        let b = parser.next_str()?;
        let c = parser.next_u64()?;

        Ok(FooKey { a, b, c })
    }
}

impl Key for FooKey {
    const PREFIX: &'static str = "pref";
    type ValueType = Infallible;

    fn parent(&self) -> Option<String> {
        None
    }
}
