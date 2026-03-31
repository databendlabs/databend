// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use arrow_array::{make_array, Array, RecordBatch};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer};
use arrow_data::{transform::MutableArrayData, ArrayData, ArrayDataBuilder};

pub fn deep_copy_buffer(buffer: &Buffer) -> Buffer {
    Buffer::from(buffer.as_slice())
}

pub fn deep_copy_nulls(nulls: Option<&NullBuffer>) -> Option<NullBuffer> {
    let nulls = nulls?;
    let bit_buffer = deep_copy_buffer(nulls.inner().inner());
    Some(unsafe {
        NullBuffer::new_unchecked(
            BooleanBuffer::new(bit_buffer, nulls.offset(), nulls.len()),
            nulls.null_count(),
        )
    })
}

pub fn deep_copy_array_data(data: &ArrayData) -> ArrayData {
    let data_type = data.data_type().clone();
    let len = data.len();
    let nulls = deep_copy_nulls(data.nulls());
    let offset = data.offset();
    let buffers = data
        .buffers()
        .iter()
        .map(deep_copy_buffer)
        .collect::<Vec<_>>();
    let child_data = data
        .child_data()
        .iter()
        .map(deep_copy_array_data)
        .collect::<Vec<_>>();
    unsafe {
        ArrayDataBuilder::new(data_type)
            .len(len)
            .nulls(nulls)
            .offset(offset)
            .buffers(buffers)
            .child_data(child_data)
            .build_unchecked()
    }
}

pub fn deep_copy_array(array: &dyn Array) -> Arc<dyn Array> {
    let data = array.to_data();
    let data = deep_copy_array_data(&data);
    make_array(data)
}

pub fn deep_copy_batch(batch: &RecordBatch) -> crate::Result<RecordBatch> {
    let arrays = batch
        .columns()
        .iter()
        .map(|array| deep_copy_array(array))
        .collect::<Vec<_>>();
    RecordBatch::try_new(batch.schema(), arrays)
}

/// Deep copy array data, extracting only the sliced portion using MutableArrayData
/// This is the most efficient and correct way to copy just the sliced data
pub fn deep_copy_array_data_sliced(data: &ArrayData) -> ArrayData {
    // Use MutableArrayData to efficiently copy just the slice
    let mut mutable = MutableArrayData::new(vec![data], false, data.len());

    // Copy from offset to offset+len (the visible slice)
    mutable.extend(0, data.offset(), data.offset() + data.len());

    // Freeze into immutable ArrayData
    mutable.freeze()
}

/// Deep copy an array, extracting only the sliced portion using MutableArrayData
pub fn deep_copy_array_sliced(array: &dyn Array) -> Arc<dyn Array> {
    let data = array.to_data();
    let data = deep_copy_array_data_sliced(&data);
    make_array(data)
}

/// Deep copy a RecordBatch, extracting only the sliced portion using MutableArrayData
pub fn deep_copy_batch_sliced(batch: &RecordBatch) -> crate::Result<RecordBatch> {
    let arrays = batch
        .columns()
        .iter()
        .map(|array| deep_copy_array_sliced(array))
        .collect::<Vec<_>>();
    RecordBatch::try_new(batch.schema(), arrays)
}

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;

    use arrow_array::{Array, Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    #[test]
    fn test_deep_copy_sliced_array_with_nulls() {
        let array = Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(3),
            None,
            Some(5),
        ]));
        let sliced_array = array.slice(1, 3);
        let copied_array = super::deep_copy_array(&sliced_array);
        assert_eq!(sliced_array.len(), copied_array.len());
        assert_eq!(sliced_array.nulls(), copied_array.nulls());
    }

    #[test]
    fn test_deep_copy_array_data_sliced() {
        let array = Int32Array::from((0..1000).collect::<Vec<i32>>());
        let sliced = array.slice(100, 10);

        let sliced_data = sliced.to_data();
        let copied_data = super::deep_copy_array_data_sliced(&sliced_data);

        assert_eq!(copied_data.len(), 10);
        assert_eq!(copied_data.offset(), 0);

        // Verify data correctness
        let copied_array = Int32Array::from(copied_data);
        for i in 0..10 {
            assert_eq!(copied_array.value(i), 100 + i as i32);
        }
    }

    #[test]
    fn test_deep_copy_array_sliced() {
        let array = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        let sliced = array.slice(1, 3);

        let copied = super::deep_copy_array_sliced(&sliced);

        assert_eq!(copied.len(), 3);
        let copied_int = copied.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(copied_int.value(0), 2);
        assert_eq!(copied_int.value(1), 3);
        assert_eq!(copied_int.value(2), 4);
    }

    #[test]
    fn test_deep_copy_batch_sliced() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let id_array = Arc::new(Int32Array::from((0..100).collect::<Vec<i32>>()));
        let name_array = Arc::new(StringArray::from(
            (0..100)
                .map(|i| format!("name_{}", i))
                .collect::<Vec<String>>(),
        ));

        let batch = RecordBatch::try_new(
            schema,
            vec![id_array as Arc<dyn Array>, name_array as Arc<dyn Array>],
        )
        .unwrap();

        let sliced = batch.slice(10, 5);
        let copied = super::deep_copy_batch_sliced(&sliced).unwrap();

        assert_eq!(copied.num_rows(), 5);
        assert_eq!(copied.num_columns(), 2);

        // Verify data correctness
        let id_col = copied
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let name_col = copied
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        for i in 0..5 {
            assert_eq!(id_col.value(i), 10 + i as i32);
            assert_eq!(name_col.value(i), format!("name_{}", 10 + i));
        }
    }

    #[test]
    fn test_deep_copy_array_sliced_with_nulls() {
        let array = Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(3),
            None,
            Some(5),
        ]));
        let sliced = array.slice(1, 3); // [None, Some(3), None]

        let copied = super::deep_copy_array_sliced(&sliced);

        assert_eq!(copied.len(), 3);
        assert_eq!(copied.null_count(), 2); // Two nulls in the slice

        let copied_int = copied.as_any().downcast_ref::<Int32Array>().unwrap();
        assert!(!copied_int.is_valid(0)); // None
        assert!(copied_int.is_valid(1)); // Some(3)
        assert!(!copied_int.is_valid(2)); // None
        assert_eq!(copied_int.value(1), 3);
    }
}
