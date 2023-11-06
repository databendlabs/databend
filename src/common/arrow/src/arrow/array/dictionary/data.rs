use arrow_data::ArrayData;
use arrow_data::ArrayDataBuilder;

use crate::arrow::array::from_data;
use crate::arrow::array::to_data;
use crate::arrow::array::Arrow2Arrow;
use crate::arrow::array::DictionaryArray;
use crate::arrow::array::DictionaryKey;
use crate::arrow::array::PrimitiveArray;
use crate::arrow::datatypes::DataType;
use crate::arrow::datatypes::PhysicalType;

impl<K: DictionaryKey> Arrow2Arrow for DictionaryArray<K> {
    fn to_data(&self) -> ArrayData {
        let keys = self.keys.to_data();
        let builder = keys
            .into_builder()
            .data_type(self.data_type.clone().into())
            .child_data(vec![to_data(self.values.as_ref())]);

        // Safety: Dictionary is valid
        unsafe { builder.build_unchecked() }
    }

    fn from_data(data: &ArrayData) -> Self {
        let key = match data.data_type() {
            arrow_schema::DataType::Dictionary(k, _) => k.as_ref(),
            d => panic!("unsupported dictionary type {d}"),
        };

        let data_type = DataType::from(data.data_type().clone());
        assert_eq!(
            data_type.to_physical_type(),
            PhysicalType::Dictionary(K::KEY_TYPE)
        );

        let key_builder = ArrayDataBuilder::new(key.clone())
            .buffers(vec![data.buffers()[0].clone()])
            .offset(data.offset())
            .len(data.len())
            .nulls(data.nulls().cloned());

        // Safety: Dictionary is valid
        let key_data = unsafe { key_builder.build_unchecked() };
        let keys = PrimitiveArray::from_data(&key_data);
        let values = from_data(&data.child_data()[0]);

        Self {
            data_type,
            keys,
            values,
        }
    }
}
