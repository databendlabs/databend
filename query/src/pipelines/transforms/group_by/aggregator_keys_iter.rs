use common_arrow::arrow::types::Index;
use common_datavalues2::{PrimitiveColumn, PrimitiveType, StringColumn};
use crate::pipelines::transforms::group_by::keys_ref::KeysRef;
use common_exception::Result;

pub trait KeysColumnIter<T> {
    fn get_slice(&self) -> &[T];
}

pub struct FixedKeysColumnIter<T> where T: PrimitiveType {
    pub inner: PrimitiveColumn<T>,
}

impl<T> FixedKeysColumnIter<T> where T: PrimitiveType {
    pub fn create(inner: &PrimitiveColumn<T>) -> Result<Self> {
        Ok(Self { inner: inner.clone() })
    }
}

impl<T> KeysColumnIter<T> for FixedKeysColumnIter<T> where T: PrimitiveType {
    fn get_slice(&self) -> &[T] {
        self.inner.values()
    }
}

pub struct SerializedKeysColumnIter {
    inner: Vec<KeysRef>,
    column: StringColumn,
}

impl SerializedKeysColumnIter {
    pub fn create(column: &StringColumn) -> Result<SerializedKeysColumnIter> {
        let values = column.values();
        let offsets = column.offsets();

        let mut inner = Vec::with_capacity(offsets.len() - 1);
        for index in 0..(offsets.len() - 1) {
            let offset = offsets[index] as usize;
            let offset_1 = offsets[index + 1] as usize;
            let address = values[offset] as *mut u8 as usize;
            inner.push(KeysRef::create(address, offset_1 - offset));
        }

        Ok(SerializedKeysColumnIter { inner, column: column.clone() })
    }
}

impl KeysColumnIter<KeysRef> for SerializedKeysColumnIter {
    fn get_slice(&self) -> &[KeysRef] {
        self.inner.as_slice()
    }
}

