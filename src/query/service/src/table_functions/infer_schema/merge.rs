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

use databend_common_expression::TableDataType;
use databend_common_expression::TableSchema;
use databend_common_expression::types::NumberDataType;

const UNSIGNED_TYPES: [NumberDataType; 4] = [
    NumberDataType::UInt8,
    NumberDataType::UInt16,
    NumberDataType::UInt32,
    NumberDataType::UInt64,
];
const SIGNED_TYPES: [NumberDataType; 4] = [
    NumberDataType::Int8,
    NumberDataType::Int16,
    NumberDataType::Int32,
    NumberDataType::Int64,
];
const FLOAT_TYPES: [NumberDataType; 2] = [NumberDataType::Float32, NumberDataType::Float64];

fn wrap_nullable(ty: TableDataType, is_nullable: bool) -> TableDataType {
    if is_nullable { ty.wrap_nullable() } else { ty }
}

pub fn merge_type(
    old: TableDataType,
    new: TableDataType,
    is_nullable: bool,
) -> Option<TableDataType> {
    if old.remove_nullable() == new.remove_nullable() {
        return Some(wrap_nullable(old, is_nullable));
    }
    if let (TableDataType::Number(old_num), TableDataType::Number(new_num)) =
        (new.remove_nullable(), old.remove_nullable())
    {
        if old_num.is_float() && new_num.is_float() {
            return promote_numeric(&old, &new, &FLOAT_TYPES)
                .map(|ty| wrap_nullable(ty, is_nullable));
        }
        return promote_numeric(&old, &new, &SIGNED_TYPES)
            .or_else(|| promote_numeric(&old, &new, &UNSIGNED_TYPES))
            .map(|ty| wrap_nullable(ty, is_nullable));
    }
    None
}

pub fn promote_numeric(
    a: &TableDataType,
    b: &TableDataType,
    types: &[NumberDataType],
) -> Option<TableDataType> {
    let idx_a = match a {
        TableDataType::Number(n) => types.iter().position(|t| t == n),
        _ => None,
    };
    let idx_b = match b {
        TableDataType::Number(n) => types.iter().position(|t| t == n),
        _ => None,
    };
    match (idx_a, idx_b) {
        (Some(i), Some(j)) => Some(TableDataType::Number(types[usize::max(i, j)])),
        _ => None,
    }
}

pub fn merge_schema(defined: TableSchema, guess: TableSchema) -> TableSchema {
    let TableSchema {
        fields: mut def_fields,
        ..
    } = defined;
    let TableSchema {
        fields: guess_fields,
        ..
    } = guess;

    for guess_field in guess_fields {
        match def_fields
            .iter_mut()
            .find(|def_field| def_field.name() == guess_field.name())
        {
            None => {
                def_fields.push(guess_field);
            }
            Some(def_field) => {
                let is_nullable =
                    def_field.data_type.is_nullable() || guess_field.data_type.is_nullable();
                def_field.data_type = merge_type(
                    def_field.data_type.clone(),
                    guess_field.data_type,
                    is_nullable,
                )
                .unwrap_or_else(|| wrap_nullable(TableDataType::String, is_nullable));
            }
        }
    }

    TableSchema::new(def_fields)
}

#[cfg(test)]
mod tests {
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use databend_common_expression::types::NumberDataType;

    use crate::table_functions::infer_schema::merge::merge_schema;
    use crate::table_functions::infer_schema::merge::merge_type;

    #[test]
    fn test_promote_unsigned() {
        assert_eq!(
            merge_type(
                TableDataType::Number(NumberDataType::UInt8),
                TableDataType::Number(NumberDataType::UInt16),
                false,
            ),
            Some(TableDataType::Number(NumberDataType::UInt16))
        );
        assert_eq!(
            merge_type(
                TableDataType::Number(NumberDataType::UInt32),
                TableDataType::Number(NumberDataType::UInt64),
                false,
            ),
            Some(TableDataType::Number(NumberDataType::UInt64))
        );
        assert_eq!(
            merge_type(
                TableDataType::Number(NumberDataType::UInt8),
                TableDataType::Number(NumberDataType::Int8),
                false,
            ),
            None
        );
    }

    #[test]
    fn test_promote_signed() {
        assert_eq!(
            merge_type(
                TableDataType::Number(NumberDataType::Int8),
                TableDataType::Number(NumberDataType::Int16),
                false,
            ),
            Some(TableDataType::Number(NumberDataType::Int16))
        );
        assert_eq!(
            merge_type(
                TableDataType::Number(NumberDataType::Int32),
                TableDataType::Number(NumberDataType::Int64),
                false,
            ),
            Some(TableDataType::Number(NumberDataType::Int64))
        );
        assert_eq!(
            merge_type(
                TableDataType::Number(NumberDataType::Int8),
                TableDataType::Number(NumberDataType::UInt8),
                false,
            ),
            None
        );
    }

    #[test]
    fn test_promote_integer() {
        assert_eq!(
            merge_type(
                TableDataType::Number(NumberDataType::Int8),
                TableDataType::Number(NumberDataType::Int16),
                false,
            ),
            Some(TableDataType::Number(NumberDataType::Int16))
        );
        assert_eq!(
            merge_type(
                TableDataType::Number(NumberDataType::UInt8),
                TableDataType::Number(NumberDataType::UInt32),
                false,
            ),
            Some(TableDataType::Number(NumberDataType::UInt32))
        );
        assert_eq!(
            merge_type(
                TableDataType::Number(NumberDataType::Int8),
                TableDataType::Number(NumberDataType::UInt8),
                false,
            ),
            None
        );
    }

    #[test]
    fn test_promote_float() {
        assert_eq!(
            merge_type(
                TableDataType::Number(NumberDataType::Float32),
                TableDataType::Number(NumberDataType::Float64),
                false,
            ),
            Some(TableDataType::Number(NumberDataType::Float64))
        );
    }

    #[test]
    fn test_promote_numeric() {
        assert_eq!(
            merge_type(
                TableDataType::Number(NumberDataType::Int8),
                TableDataType::Number(NumberDataType::Int16),
                false,
            ),
            Some(TableDataType::Number(NumberDataType::Int16))
        );
        assert_eq!(
            merge_type(
                TableDataType::Number(NumberDataType::Float32),
                TableDataType::Number(NumberDataType::Int16),
                false,
            ),
            None
        );
        assert_eq!(
            merge_type(
                TableDataType::String,
                TableDataType::Number(NumberDataType::Int32),
                false,
            ),
            None
        );
    }

    #[test]
    fn test_merge_schema() {
        let schema_1 = TableSchema::new(vec![
            TableField::new(
                "c1",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::Int8))),
            ),
            TableField::new("c2", TableDataType::Number(NumberDataType::Int8)),
            TableField::new("c3", TableDataType::Number(NumberDataType::Int32)),
            TableField::new("c4", TableDataType::Number(NumberDataType::Float32)),
            TableField::new("c5", TableDataType::Number(NumberDataType::Float32)),
        ]);
        let schema_2 = TableSchema::new(vec![
            TableField::new("c1", TableDataType::Number(NumberDataType::Int8)),
            TableField::new("c3", TableDataType::Number(NumberDataType::Float32)),
            TableField::new("c2", TableDataType::Number(NumberDataType::Int8)),
            TableField::new("c4", TableDataType::Number(NumberDataType::Float32)),
            TableField::new("c6", TableDataType::Number(NumberDataType::Float32)),
        ]);

        let schema = merge_schema(schema_1, schema_2);
        let expected_schema = TableSchema::new(vec![
            TableField::new(
                "c1",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::Int8))),
            ),
            TableField::new("c2", TableDataType::Number(NumberDataType::Int8)),
            TableField::new("c3", TableDataType::String),
            TableField::new("c4", TableDataType::Number(NumberDataType::Float32)),
            TableField::new("c5", TableDataType::Number(NumberDataType::Float32)),
            TableField::new("c6", TableDataType::Number(NumberDataType::Float32)),
        ]);
        assert_eq!(schema, expected_schema);
    }
}
