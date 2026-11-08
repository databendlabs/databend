// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::str::FromStr;
use std::sync::Arc;

use arrow_array::BooleanArray;
use arrow_array::RecordBatch;
use arrow_schema::Field;
use arrow_schema::Schema;
use databend_common_ast::ast::quote::QuotedIdent;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::TableSchema;
use databend_common_expression::types::Bitmap;
use parquet::arrow::ARROW_SCHEMA_META_KEY;
use parquet::arrow::ProjectionMask;
use parquet::arrow::encode_arrow_schema;
use parquet::arrow::parquet_to_arrow_schema_by_columns;
use parquet::file::metadata::KeyValue;
use parquet::schema::types::SchemaDescriptor;

use crate::read_schema::ParquetReadSchema;
use crate::transformer::FieldMatching;
use crate::transformer::RecordBatchTransformer;

pub type DataBlockIterator = Box<dyn Iterator<Item = Result<DataBlock>> + Send>;

/// A cached conversion for one read stage's exact projected batch. Predicate,
/// TopK, remaining columns and output each have their own plan. A plan being
/// present says nothing about whether the projection contains nested fields.
#[derive(Clone, Debug)]
pub struct BatchReadPlan {
    data_schema: DataSchema,
    transformer: RecordBatchTransformer,
}

impl BatchReadPlan {
    pub fn convert(&self, batch: &RecordBatch) -> Result<DataBlock> {
        let batch = self.transformer.transform_record_batch(batch)?;
        DataBlock::from_record_batch(&self.data_schema, &batch)
    }
}

pub fn transform_record_batch(
    data_schema: &DataSchema,
    batch: &RecordBatch,
    read_plan: &Option<BatchReadPlan>,
) -> Result<DataBlock> {
    match read_plan {
        Some(plan) => plan.convert(batch),
        None => DataBlock::from_record_batch(data_schema, batch),
    }
}

pub fn bitmap_to_boolean_array(bitmap: Bitmap) -> BooleanArray {
    BooleanArray::from(bitmap.into_array_data())
}

pub fn compute_output_read_plan(
    schema_desc: &SchemaDescriptor,
    projection: &ProjectionMask,
    expected_schema: &TableSchema,
    inner_projection: bool,
    read_schema: Option<&ParquetReadSchema>,
    arrow_schema: Option<&Schema>,
) -> Result<Option<BatchReadPlan>> {
    // Ordinary top-level reads already convert directly to the runtime types.
    // Preserve that fast path, including Arrow representations such as binary
    // views and fixed-size lists. Only nested extraction or explicit identities
    // need a compiled alignment plan.
    if read_schema.is_none() && !inner_projection {
        return Ok(None);
    }
    // Use the same Arrow hints as the decoder, while keeping the file's
    // physical descriptor authoritative. This is performed once per read stage.
    let metadata = arrow_schema.map(|schema| {
        vec![KeyValue::new(
            ARROW_SCHEMA_META_KEY.to_string(),
            Some(encode_arrow_schema(schema)),
        )]
    });
    let source = Arc::new(parquet_to_arrow_schema_by_columns(
        schema_desc,
        projection.clone(),
        metadata.as_ref(),
    )?);
    // Use TableSchema, not DataSchema: named tuple children must survive.
    let fields = expected_schema
        .fields()
        .iter()
        .map(|field| match read_schema {
            Some(schema) => schema.field_for_read(field),
            None => Ok(Field::from(field)),
        })
        .collect::<Result<Vec<_>>>()?;
    let target = Arc::new(Schema::new(fields));
    let matching = if read_schema.is_some() {
        FieldMatching::FieldId
    } else {
        FieldMatching::Path
    };
    Ok(Some(BatchReadPlan {
        data_schema: DataSchema::from(expected_schema),
        transformer: RecordBatchTransformer::try_create(source, target, matching)?,
    }))
}

pub(crate) fn parquet_field_name_matches_path_name(
    parquet_field_name: &str,
    path_name: &str,
) -> bool {
    parquet_field_name.eq_ignore_ascii_case(path_name)
        || QuotedIdent::<String>::from_str(path_name)
            .is_ok_and(|quoted| parquet_field_name.eq_ignore_ascii_case(&quoted.0))
}

#[cfg(test)]
mod tests {
    use super::parquet_field_name_matches_path_name;

    #[test]
    fn projected_stage_plans_do_not_require_other_output_fields() {
        use std::collections::HashMap;
        use std::sync::Arc;

        use arrow_array::BooleanArray;
        use arrow_array::Int64Array;
        use arrow_array::RecordBatch;
        use arrow_schema::DataType;
        use arrow_schema::Field;
        use arrow_schema::Schema;
        use databend_common_expression::DataSchema;
        use databend_common_expression::TableSchema;
        use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
        use parquet::arrow::ProjectionMask;
        use parquet::arrow::parquet_to_arrow_schema_by_columns;
        use parquet::schema::parser::parse_message_type;
        use parquet::schema::types::SchemaDescriptor;

        use super::compute_output_read_plan;
        use super::transform_record_batch;
        use crate::read_schema::ParquetReadSchema;

        let field = |name: &str, ty: DataType, id: i32| {
            Field::new(name, ty, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                id.to_string(),
            )]))
        };
        let target = Schema::new(vec![
            field("k", DataType::Int64, 1),
            field("p", DataType::Boolean, 2),
            field("v", DataType::Int64, 3),
        ]);
        let table = TableSchema::try_from(&target).unwrap();
        let read_schema = ParquetReadSchema::new(target);
        let file = SchemaDescriptor::new(Arc::new(parse_message_type("message test { OPTIONAL INT64 v = 3; OPTIONAL BOOLEAN p = 2; OPTIONAL INT64 k = 1; }").unwrap()));
        // A scalar ID-alignment plan is not an inner projection. Each prefetch
        // batch has its own target even if the caller also has a wider schema.
        for (target_index, leaf) in [(0, 2), (1, 1), (2, 0)] {
            let projection = ProjectionMask::leaves(&file, [leaf]);
            let stage = table.project(&[target_index]);
            let plan = compute_output_read_plan(
                &file,
                &projection,
                &stage,
                false,
                Some(&read_schema),
                None,
            )
            .unwrap();
            assert!(plan.is_some());
            let schema =
                Arc::new(parquet_to_arrow_schema_by_columns(&file, projection, None).unwrap());
            let array: arrow_array::ArrayRef = if target_index == 1 {
                Arc::new(BooleanArray::from(vec![true]))
            } else {
                Arc::new(Int64Array::from(vec![10]))
            };
            let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
            let block = transform_record_batch(&DataSchema::from(&table), &batch, &plan).unwrap();
            assert_eq!(block.num_columns(), 1);
            assert_eq!(block.num_rows(), 1);
        }
        let empty = table.project(&[]);
        let plan = compute_output_read_plan(
            &file,
            &ProjectionMask::none(3),
            &empty,
            false,
            Some(&read_schema),
            None,
        )
        .unwrap();
        let batch = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &arrow_array::RecordBatchOptions::default().with_row_count(Some(3)),
        )
        .unwrap();
        let block = transform_record_batch(&DataSchema::from(&table), &batch, &plan).unwrap();
        assert_eq!(block.num_columns(), 0);
        assert_eq!(block.num_rows(), 3);
    }

    #[test]
    fn binary_view_fixture_keeps_fixed_size_list_compatibility() {
        use bytes::Bytes;
        use databend_common_expression::DataSchema;
        use databend_common_expression::Scalar;
        use databend_common_expression::TableSchema;
        use databend_common_expression::types::number::NumberScalar;
        use parquet::arrow::ProjectionMask;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        use super::compute_output_read_plan;
        use super::transform_record_batch;

        let data = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../../tests/data/parquet/binary_view.parquet"
        ));
        let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from_static(data)).unwrap();
        let arrow = builder.schema().clone();
        let table = TableSchema::try_from(arrow.as_ref()).unwrap();
        let schema = builder.parquet_schema();
        let plan = compute_output_read_plan(
            schema,
            &ProjectionMask::all(),
            &table,
            true,
            None,
            Some(&arrow),
        )
        .unwrap();
        // Force compiled-plan coverage separately from the flat-read fast path.
        // Nested/identity reads still need to align these Arrow representations.
        assert!(plan.is_some());
        let reader = builder.with_batch_size(1).build().unwrap();
        let expected_binary = [
            b"ab".as_slice(),
            b"hello".as_slice(),
            b"large payload over 12 bytes".as_slice(),
        ];
        let mut count = 0;
        for (index, batch) in reader.enumerate() {
            let block =
                transform_record_batch(&DataSchema::from(&table), &batch.unwrap(), &plan).unwrap();
            assert_eq!(block.num_rows(), 1);
            assert_eq!(
                block.get_by_offset(0).value().index(0).unwrap().to_owned(),
                Scalar::Binary(expected_binary[index].to_vec())
            );
            let Scalar::Array(values) = block.get_by_offset(1).value().index(0).unwrap().to_owned()
            else {
                panic!()
            };
            for child in 0..2 {
                assert_eq!(
                    values.index(child).unwrap().to_owned(),
                    Scalar::Number(NumberScalar::Int32((index * 2 + child + 1) as i32))
                );
            }
            count += 1;
        }
        assert_eq!(count, 3);
    }

    #[test]
    fn ordinary_flat_reads_do_not_build_an_alignment_plan() {
        use bytes::Bytes;
        use databend_common_expression::DataSchema;
        use databend_common_expression::TableSchema;
        use parquet::arrow::ProjectionMask;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        use super::compute_output_read_plan;
        use super::transform_record_batch;

        let data = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../../tests/data/parquet/binary_view.parquet"
        ));
        let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from_static(data)).unwrap();
        let table = TableSchema::try_from(builder.schema().as_ref()).unwrap();
        let plan = compute_output_read_plan(
            builder.parquet_schema(),
            &ProjectionMask::all(),
            &table,
            false,
            None,
            Some(builder.schema()),
        )
        .unwrap();
        assert!(plan.is_none());
        let schema = DataSchema::from(&table);
        for batch in builder.with_batch_size(1).build().unwrap() {
            let block = transform_record_batch(&schema, &batch.unwrap(), &plan).unwrap();
            assert_eq!(block.num_rows(), 1);
            assert_eq!(block.num_columns(), 2);
        }
    }

    #[test]
    fn test_parquet_field_name_matches_quoted_path_name() {
        assert!(parquet_field_name_matches_path_name("A", "\"A\""));
        assert!(parquet_field_name_matches_path_name("A", "\"a\""));
        assert!(parquet_field_name_matches_path_name("simple", "simple"));
        assert!(parquet_field_name_matches_path_name("simple", "SIMPLE"));
        assert!(!parquet_field_name_matches_path_name("A", "\"B\""));
    }
}
