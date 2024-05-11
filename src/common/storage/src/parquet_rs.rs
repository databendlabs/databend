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

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::Schema as ArrowSchema;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::converts::arrow::EXTENSION_KEY;
use databend_common_expression::FieldIndex;
use opendal::Operator;
use parquet::arrow::parquet_to_arrow_schema;
use parquet::file::footer::decode_footer;
use parquet::file::footer::decode_metadata;
use parquet::file::metadata::FileMetaData;
use parquet::file::metadata::ParquetMetaData;

const FOOTER_SIZE: u64 = 8;
/// The number of bytes read at the end of the parquet file on first read
const DEFAULT_FOOTER_READ_SIZE: u64 = 64 * 1024;

#[async_backtrace::framed]
pub async fn read_parquet_schema_async_rs(
    operator: &Operator,
    path: &str,
    file_size: Option<u64>,
) -> Result<ArrowSchema> {
    let meta = read_metadata_async(path, operator, file_size).await?;
    infer_schema_with_extension(meta.file_metadata())
}

pub fn infer_schema_with_extension(meta: &FileMetaData) -> Result<ArrowSchema> {
    let mut arrow_schema = parquet_to_arrow_schema(meta.schema_descr(), meta.key_value_metadata())?;
    // Convert data types to extension types using meta information.
    // Mainly used for types such as Variant and Bitmap,
    // as they have the same physical type as String.
    if let Some(metas) = meta.key_value_metadata() {
        let mut new_fields = Vec::with_capacity(arrow_schema.fields.len());
        for field in arrow_schema.fields.iter() {
            let mut new_field = field.clone();
            for meta in metas {
                match &meta.value {
                    Some(ty) if field.name() == &meta.key => {
                        let f = arrow_schema::Field::new(
                            field.name(),
                            field.data_type().clone(),
                            field.is_nullable(),
                        )
                        .with_metadata(HashMap::from([(
                            EXTENSION_KEY.to_string(),
                            ty.to_string(),
                        )]));
                        new_field = Arc::new(f);
                        break;
                    }
                    _ => {}
                }
            }
            new_fields.push(new_field);
        }
        arrow_schema = ArrowSchema::new_with_metadata(new_fields, arrow_schema.metadata);
    }

    Ok(arrow_schema)
}

/// Layout of Parquet file
/// +---------------------------+-----+---+
/// |      Rest of file         |  B  | A |
/// +---------------------------+-----+---+
/// where A: parquet footer, B: parquet metadata.
///
/// The reader first reads DEFAULT_FOOTER_SIZE bytes from the end of the file.
/// If it is not enough according to the length indicated in the footer, it reads more bytes.
pub async fn read_metadata_async(
    path: &str,
    operator: &Operator,
    file_size: Option<u64>,
) -> Result<ParquetMetaData> {
    let file_size = match file_size {
        None => operator.stat(path).await?.content_length(),
        Some(n) => n,
    };
    check_footer_size(file_size)?;

    // read and cache up to DEFAULT_FOOTER_READ_SIZE bytes from the end and process the footer
    let default_end_len = DEFAULT_FOOTER_READ_SIZE.min(file_size);
    let buffer = operator
        .read_with(path)
        .range((file_size - default_end_len)..file_size)
        .await?
        .to_vec();
    let buffer_len = buffer.len();
    let metadata_len = decode_footer(
        &buffer[(buffer_len - FOOTER_SIZE as usize)..]
            .try_into()
            .unwrap(),
    )? as u64;
    check_meta_size(file_size, metadata_len)?;

    let footer_len = FOOTER_SIZE + metadata_len;
    if (footer_len as usize) <= buffer_len {
        // The whole metadata is in the bytes we already read
        let offset = buffer_len - footer_len as usize;
        Ok(decode_metadata(&buffer[offset..])?)
    } else {
        // The end of file read by default is not long enough, read again including the metadata.
        // TBD: which one is better?
        // 1. Read the whole footer data again. (file_size - footer_len)..file_size
        // 2. Read the remain data only and concat with the previous read data. (file_size - footer_len)..(file_size - buffer.len()
        let mut metadata = operator
            .read_with(path)
            .range((file_size - footer_len)..(file_size - buffer_len as u64))
            .await?
            .to_vec();
        metadata.extend(buffer);
        Ok(decode_metadata(&metadata)?)
    }
}

/// check file is large enough to hold footer
fn check_footer_size(file_size: u64) -> Result<()> {
    if file_size < FOOTER_SIZE {
        Err(ErrorCode::BadBytes(
            "Invalid Parquet file. Size is smaller than footer.",
        ))
    } else {
        Ok(())
    }
}

/// check file is large enough to hold metadata
fn check_meta_size(file_size: u64, metadata_len: u64) -> Result<()> {
    if metadata_len + FOOTER_SIZE > file_size {
        Err(ErrorCode::BadBytes(format!(
            "Invalid Parquet file. Reported metadata length of {} + {} byte footer, but file is only {} bytes",
            metadata_len, FOOTER_SIZE, file_size
        )))
    } else {
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ParquetSchemaTreeNode {
    Leaf(usize),
    Inner(Vec<ParquetSchemaTreeNode>),
}

/// Convert [`parquet::schema::types::Type`] to a tree structure.
pub fn build_parquet_schema_tree(
    ty: &parquet::schema::types::Type,
    leave_id: &mut usize,
) -> ParquetSchemaTreeNode {
    match ty {
        parquet::schema::types::Type::PrimitiveType { .. } => {
            let res = ParquetSchemaTreeNode::Leaf(*leave_id);
            *leave_id += 1;
            res
        }
        parquet::schema::types::Type::GroupType { fields, .. } => {
            let mut children = Vec::with_capacity(fields.len());
            for field in fields.iter() {
                children.push(build_parquet_schema_tree(field, leave_id));
            }
            ParquetSchemaTreeNode::Inner(children)
        }
    }
}

/// Traverse the schema tree by `path` to collect the leaves' ids.
pub fn traverse_parquet_schema_tree(
    node: &ParquetSchemaTreeNode,
    path: &[FieldIndex],
    leaves: &mut Vec<usize>,
) {
    match node {
        ParquetSchemaTreeNode::Leaf(id) => {
            leaves.push(*id);
        }
        ParquetSchemaTreeNode::Inner(children) => {
            if path.is_empty() {
                // All children should be included.
                for child in children.iter() {
                    traverse_parquet_schema_tree(child, path, leaves);
                }
            } else {
                let child = path[0];
                traverse_parquet_schema_tree(&children[child], &path[1..], leaves);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use parquet::arrow::arrow_to_parquet_schema;

    use crate::parquet_rs::build_parquet_schema_tree;
    use crate::parquet_rs::ParquetSchemaTreeNode;

    #[test]
    fn test_build_parquet_schema_tree() {
        // Test schema (6 physical columns):
        // a: Int32,            (leave id: 0, path: [0])
        // b: Tuple (
        //    c: Int32,         (leave id: 1, path: [1, 0])
        //    d: Tuple (
        //        e: Int32,     (leave id: 2, path: [1, 1, 0])
        //        f: String,    (leave id: 3, path: [1, 1, 1])
        //    ),
        //    g: String,        (leave id: 4, path: [1, 2])
        // )
        // h: String,           (leave id: 5, path: [2])
        let schema = TableSchema::new(vec![
            TableField::new("a", TableDataType::Number(NumberDataType::Int32)),
            TableField::new("b", TableDataType::Tuple {
                fields_name: vec!["c".to_string(), "d".to_string(), "g".to_string()],
                fields_type: vec![
                    TableDataType::Number(NumberDataType::Int32),
                    TableDataType::Tuple {
                        fields_name: vec!["e".to_string(), "f".to_string()],
                        fields_type: vec![
                            TableDataType::Number(NumberDataType::Int32),
                            TableDataType::String,
                        ],
                    },
                    TableDataType::String,
                ],
            }),
            TableField::new("h", TableDataType::String),
        ]);
        let arrow_schema = (&schema).into();
        let schema_desc = arrow_to_parquet_schema(&arrow_schema).unwrap();
        let mut leave_id = 0;
        let tree = build_parquet_schema_tree(schema_desc.root_schema(), &mut leave_id);
        assert_eq!(leave_id, 6);
        let expected_tree = ParquetSchemaTreeNode::Inner(vec![
            ParquetSchemaTreeNode::Leaf(0),
            ParquetSchemaTreeNode::Inner(vec![
                ParquetSchemaTreeNode::Leaf(1),
                ParquetSchemaTreeNode::Inner(vec![
                    ParquetSchemaTreeNode::Leaf(2),
                    ParquetSchemaTreeNode::Leaf(3),
                ]),
                ParquetSchemaTreeNode::Leaf(4),
            ]),
            ParquetSchemaTreeNode::Leaf(5),
        ]);
        assert_eq!(tree, expected_tree);
    }
}
