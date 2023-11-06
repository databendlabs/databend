use arrow_data::ArrayData;
use arrow_data::ArrayDataBuilder;

use crate::arrow::array::from_data;
use crate::arrow::array::to_data;
use crate::arrow::array::Arrow2Arrow;
use crate::arrow::array::FixedSizeListArray;
use crate::arrow::bitmap::Bitmap;
use crate::arrow::datatypes::DataType;

impl Arrow2Arrow for FixedSizeListArray {
    fn to_data(&self) -> ArrayData {
        let data_type = self.data_type.clone().into();
        let builder = ArrayDataBuilder::new(data_type)
            .len(self.len())
            .nulls(self.validity.as_ref().map(|b| b.clone().into()))
            .child_data(vec![to_data(self.values.as_ref())]);

        // Safety: Array is valid
        unsafe { builder.build_unchecked() }
    }

    fn from_data(data: &ArrayData) -> Self {
        let data_type: DataType = data.data_type().clone().into();
        let size = match data_type {
            DataType::FixedSizeList(_, size) => size,
            _ => unreachable!("must be FixedSizeList type"),
        };

        let mut values = from_data(&data.child_data()[0]);
        values.slice(data.offset() * size, data.len() * size);

        Self {
            size,
            data_type,
            values,
            validity: data.nulls().map(|n| Bitmap::from_null_buffer(n.clone())),
        }
    }
}
