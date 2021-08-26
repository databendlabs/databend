use common_datablocks::{HashMethod, HashMethodFixedKeys};
use common_datavalues::arrays::{ArrayBuilder, PrimitiveArrayBuilder};
use common_datavalues::DFNumericType;
use common_datavalues::prelude::{IntoSeries, Series};
use crate::pipelines::transforms::group_by::keys_ref::KeysRef;
use toml::map::Keys;

pub trait KeysArrayBuilder<Value> {
    fn finish(self) -> Series;
    fn append_value(&mut self, v: &Value);
}

pub struct FixedKeysArrayBuilder<T> where T: DFNumericType {
    pub inner_builder: PrimitiveArrayBuilder<T>,
}

impl<T> KeysArrayBuilder<T::Native> for FixedKeysArrayBuilder<T> where
    T: DFNumericType,
    HashMethodFixedKeys<T>: HashMethod<HashKey=T::Native>,
{
    #[inline]
    fn finish(mut self) -> Series {
        self.inner_builder.finish().array.into_series()
    }

    #[inline]
    fn append_value(&mut self, v: &T::Native) {
        self.inner_builder.append_value(*v)
    }
}

// TODO:

pub struct SerializedKeysArrayBuilder {}

impl KeysArrayBuilder<KeysRef> for SerializedKeysArrayBuilder {
    fn finish(self) -> Series {
        unimplemented!()
    }

    fn append_value(&mut self, v: &KeysRef) {
        unimplemented!()
    }
}
