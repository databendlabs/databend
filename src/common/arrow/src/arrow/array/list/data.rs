use arrow_data::ArrayData;
use arrow_data::ArrayDataBuilder;

use crate::arrow::array::from_data;
use crate::arrow::array::to_data;
use crate::arrow::array::Arrow2Arrow;
use crate::arrow::array::ListArray;
use crate::arrow::bitmap::Bitmap;
use crate::arrow::offset::Offset;
use crate::arrow::offset::OffsetsBuffer;

impl<O: Offset> Arrow2Arrow for ListArray<O> {
    fn to_data(&self) -> ArrayData {
        let data_type = self.data_type.clone().into();

        let builder = ArrayDataBuilder::new(data_type)
            .len(self.len())
            .buffers(vec![self.offsets.clone().into_inner().into()])
            .nulls(self.validity.as_ref().map(|b| b.clone().into()))
            .child_data(vec![to_data(self.values.as_ref())]);

        // Safety: Array is valid
        unsafe { builder.build_unchecked() }
    }

    fn from_data(data: &ArrayData) -> Self {
        let data_type = data.data_type().clone().into();
        if data.is_empty() {
            // Handle empty offsets
            return Self::new_empty(data_type);
        }

        let mut offsets = unsafe { OffsetsBuffer::new_unchecked(data.buffers()[0].clone().into()) };
        offsets.slice(data.offset(), data.len() + 1);

        Self {
            data_type,
            offsets,
            values: from_data(&data.child_data()[0]),
            validity: data.nulls().map(|n| Bitmap::from_null_buffer(n.clone())),
        }
    }
}
