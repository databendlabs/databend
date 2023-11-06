use arrow_data::ArrayData;
use arrow_data::ArrayDataBuilder;

use crate::arrow::array::from_data;
use crate::arrow::array::to_data;
use crate::arrow::array::Arrow2Arrow;
use crate::arrow::array::StructArray;
use crate::arrow::bitmap::Bitmap;

impl Arrow2Arrow for StructArray {
    fn to_data(&self) -> ArrayData {
        let data_type = self.data_type.clone().into();

        let builder = ArrayDataBuilder::new(data_type)
            .len(self.len())
            .nulls(self.validity.as_ref().map(|b| b.clone().into()))
            .child_data(self.values.iter().map(|x| to_data(x.as_ref())).collect());

        // Safety: Array is valid
        unsafe { builder.build_unchecked() }
    }

    fn from_data(data: &ArrayData) -> Self {
        let data_type = data.data_type().clone().into();

        Self {
            data_type,
            values: data.child_data().iter().map(from_data).collect(),
            validity: data.nulls().map(|n| Bitmap::from_null_buffer(n.clone())),
        }
    }
}
