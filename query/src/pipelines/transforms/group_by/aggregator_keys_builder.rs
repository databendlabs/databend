use common_datablocks::{HashMethod, HashMethodFixedKeys};
use common_datavalues::arrays::{ArrayBuilder, PrimitiveArrayBuilder, BinaryArrayBuilder};
use common_datavalues::DFNumericType;
use common_datavalues::prelude::{IntoSeries, Series};
use crate::pipelines::transforms::group_by::keys_ref::KeysRef;
use toml::map::Keys;
use crate::pipelines::transforms::group_by::aggregator_state::SerializedKeysAggregatorState;

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

pub struct SerializedKeysArrayBuilder {
    pub inner_builder: BinaryArrayBuilder,
}

impl KeysArrayBuilder<KeysRef> for SerializedKeysArrayBuilder {
    fn finish(mut self) -> Series {
        self.inner_builder.finish().array.into_series()
    }

    fn append_value(&mut self, v: &KeysRef) {
        unsafe {
            let value = std::slice::from_raw_parts(v.address as *const u8, v.length);
            self.inner_builder.append_value(value);
        }
    }
}
