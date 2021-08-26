use common_datablocks::{HashMethod, HashMethodFixedKeys};
use common_datavalues::arrays::{ArrayBuilder, PrimitiveArrayBuilder};
use common_datavalues::DFNumericType;
use common_datavalues::prelude::{IntoSeries, Series};

pub trait BinaryKeysArrayBuilder<Value> {
    fn finish(self) -> Series;
    fn append_value(&mut self, v: &Value);
}

pub struct NativeBinaryKeysArrayBuilder<T> where T: DFNumericType {
    pub inner_builder: PrimitiveArrayBuilder<T>,
}

impl<T> BinaryKeysArrayBuilder<T::Native> for NativeBinaryKeysArrayBuilder<T> where
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

pub struct SerializedBinaryKeysArrayBuilder {}

impl BinaryKeysArrayBuilder<Vec<u8>> for SerializedBinaryKeysArrayBuilder {
    fn finish(self) -> Series {
        unimplemented!()
    }

    fn append_value(&mut self, v: &Vec<u8>) {
        unimplemented!()
    }
}
