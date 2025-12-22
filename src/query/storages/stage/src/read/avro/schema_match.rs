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

use apache_avro::Schema;
use apache_avro::schema::RecordSchema;
use apache_avro::schema::UnionSchema;
use databend_common_expression::TableDataType;
use databend_common_expression::types::Decimal;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::i256;

type MatchResult<T> = Result<T, String>;

#[derive(Debug, Clone)]
pub(super) enum MatchedField {
    TypeDefault,
    FieldDefault,
    Value(MatchedSchema, usize),
}

#[derive(Debug, Clone)]
pub(super) struct MatchedUnion {
    pub variants: Vec<MatchedSchema>,
}

#[derive(Debug, Clone)]
pub(super) enum MatchedSchema {
    // all dest table/tuple fields, with the original order
    Record(Vec<MatchedField>),
    Union(MatchedUnion),
    Array(Box<MatchedSchema>),
    Map(Box<MatchedSchema>),
    Decimal { multiplier: Option<i256> },
    Primary(Schema),
}

pub(super) struct SchemaMatcher {
    pub allow_missing_field: bool,
}

impl SchemaMatcher {
    fn match_union(&self, dest: &TableDataType, u: &UnionSchema) -> MatchResult<MatchedUnion> {
        let variants: MatchResult<Vec<MatchedSchema>> = u
            .variants()
            .iter()
            .map(|variant| {
                if variant == &Schema::Null {
                    Ok(MatchedSchema::Primary(variant.clone()))
                } else {
                    self.match_field(dest, variant)
                }
            })
            .collect();
        let variants = variants?;
        Ok(MatchedUnion { variants })
    }

    fn match_field(&self, dest: &TableDataType, src: &Schema) -> MatchResult<MatchedSchema> {
        match (dest, src) {
            (TableDataType::Nullable(_), Schema::Union(u)) => {
                Ok(MatchedSchema::Union(self.match_union(dest, u)?))
            }
            (TableDataType::Nullable(n), _) => self.match_field(n, src),
            (_, Schema::Union(u)) if u.variants().contains(&Schema::Null) => {
                Ok(MatchedSchema::Union(self.match_union(dest, u)?))
            }
            (TableDataType::Map(dst), Schema::Map(src)) => match *dst.clone() {
                TableDataType::Tuple {
                    fields_name: _fields_name,
                    fields_type,
                } => Ok(MatchedSchema::Map(Box::new(
                    self.match_field(&fields_type[1], &src.types)?,
                ))),
                _ => unreachable!(),
            },
            (TableDataType::Array(dst), Schema::Array(src)) => Ok(MatchedSchema::Array(Box::new(
                self.match_field(dst, &src.items)?,
            ))),
            (
                TableDataType::Tuple {
                    fields_name,
                    fields_type,
                },
                Schema::Record(src),
            ) => Ok(MatchedSchema::Record(self.match_fields(
                fields_name,
                fields_type,
                src,
                1,
            )?)),
            (TableDataType::Decimal(d1), Schema::Decimal(d2))
                if (d1.leading_digits() as usize) >= d2.precision - d2.scale
                    && (d1.scale() as usize) >= d2.scale =>
            {
                let diff = d1.scale() - d2.scale as u8;
                let multiplier = if diff > 0 { Some(i256::e(diff)) } else { None };
                Ok(MatchedSchema::Decimal { multiplier })
            }
            (TableDataType::Number(NumberDataType::Int32), Schema::Int)
            | (TableDataType::Number(NumberDataType::UInt64), Schema::Int)
            | (TableDataType::Number(NumberDataType::Int64), Schema::Int | Schema::Long)
            | (TableDataType::Number(NumberDataType::Float32), Schema::Float)
            | (TableDataType::Number(NumberDataType::Float64), Schema::Float | Schema::Double)
            | (TableDataType::String, Schema::String | Schema::Enum(_) | Schema::Uuid)
            | (TableDataType::Binary, Schema::Bytes | Schema::Fixed(_))
            | (TableDataType::Decimal(_), Schema::BigDecimal)
            | (TableDataType::Boolean, Schema::Boolean)
            | (TableDataType::Date | TableDataType::Number(NumberDataType::Int32), Schema::Date)
            | (TableDataType::Interval, Schema::Duration)
            | (
                TableDataType::Timestamp | TableDataType::Number(NumberDataType::Int64),
                Schema::TimestampNanos
                | Schema::TimestampMillis
                | Schema::TimestampMicros
                | Schema::LocalTimestampNanos
                | Schema::LocalTimestampMillis
                | Schema::LocalTimestampMicros,
            )
            | (TableDataType::Variant, _) => Ok(MatchedSchema::Primary(src.to_owned())),
            // todo: Bitmap, Geometry, Geography
            _ => Err(format!(
                "fail to match schema: can not load avro type {src} into {dest}"
            )),
        }
    }

    pub fn match_fields(
        &self,
        fields_name: &[String],
        fields_type: &[TableDataType],
        src_schema: &RecordSchema,
        level: u32,
    ) -> MatchResult<Vec<MatchedField>> {
        let mut matched_fields = vec![MatchedField::TypeDefault; fields_name.len()];
        let mut num_matched = 0;
        for (c, name) in fields_name.iter().enumerate() {
            if let Some(pos) = src_schema.lookup.get(name) {
                matched_fields[c] = MatchedField::Value(
                    self.match_field(&fields_type[c], &src_schema.fields[*pos].schema)?,
                    *pos,
                );
                num_matched += 1;
            } else {
                if !self.allow_missing_field {
                    return Err(format!(
                        "missing field {name}. Consider add avro file format option `MISSING_FIELD_AS = FIELD_DEFAULT`"
                    ));
                }
                matched_fields[c] = if level == 0 {
                    MatchedField::FieldDefault
                } else {
                    MatchedField::TypeDefault
                }
            }
        }
        if num_matched == 0 {
            return Err("no fields match by name".to_string());
        }
        Ok(matched_fields)
    }
}
