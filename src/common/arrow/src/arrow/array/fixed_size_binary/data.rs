use arrow_data::ArrayData;
use arrow_data::ArrayDataBuilder;

use crate::arrow::array::Arrow2Arrow;
use crate::arrow::array::FixedSizeBinaryArray;
use crate::arrow::bitmap::Bitmap;
use crate::arrow::buffer::Buffer;
use crate::arrow::datatypes::DataType;

impl Arrow2Arrow for FixedSizeBinaryArray {
    fn to_data(&self) -> ArrayData {
        let data_type = self.data_type.clone().into();
        let builder = ArrayDataBuilder::new(data_type)
            .len(self.len())
            .buffers(vec![self.values.clone().into()])
            .nulls(self.validity.as_ref().map(|b| b.clone().into()));

        // Safety: Array is valid
        unsafe { builder.build_unchecked() }
    }

    fn from_data(data: &ArrayData) -> Self {
        let data_type: DataType = data.data_type().clone().into();
        let size = match data_type {
            DataType::FixedSizeBinary(size) => size,
            _ => unreachable!("must be FixedSizeBinary"),
        };

        let mut values: Buffer<u8> = data.buffers()[0].clone().into();
        values.slice(data.offset() * size, data.len() * size);

        Self {
            size,
            data_type,
            values,
            validity: data.nulls().map(|n| Bitmap::from_null_buffer(n.clone())),
        }
    }
}
