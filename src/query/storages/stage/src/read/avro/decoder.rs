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

use std::sync::Arc;

use apache_avro::schema::RecordSchema;
use apache_avro::types::Value;
use apache_avro::Reader;
use apache_avro::Schema;
use databend_common_base::base::OrderedFloat;
use databend_common_column::bitmap::MutableBitmap;
use databend_common_column::types::months_days_micros;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::array::ArrayColumnBuilder;
use databend_common_expression::types::binary::BinaryColumnBuilder;
use databend_common_expression::types::nullable::NullableColumnBuilder;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::Number;
use databend_common_expression::types::NumberColumnBuilder;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::TableDataType;
use databend_common_meta_app::principal::AvroFileFormatParams;
use databend_common_meta_app::principal::NullAs;
use lexical_core::FromLexical;
use num_traits::NumCast;

use crate::read::avro::avro_to_jsonb::to_jsonb;
use crate::read::block_builder_state::BlockBuilderState;
use crate::read::load_context::LoadContext;
use crate::read::whole_file_reader::WholeFileData;

#[derive(Default, Debug)]
struct Error {
    reason: Option<String>,
    path: Vec<(usize, String)>,
}

impl Error {
    pub fn new_reason(reason: String) -> Self {
        Self {
            reason: Some(reason),
            path: vec![],
        }
    }

    pub fn not_match(schema: &Schema, value: &Value) -> Self {
        Self {
            reason: Some(format!(
                "schema ({schema}) and value ({value:?}) not match:"
            )),
            path: vec![],
        }
    }
}

type ReadFieldResult = std::result::Result<(), Error>;

#[derive(Clone)]
struct FieldMeta {
    fill_to_dest: usize,
    sub_record: Option<RecordMeta>,
}

#[derive(Clone)]
struct RecordMeta {
    src_fields: Vec<Option<FieldMeta>>,
    dest_fields_with_defaults: Vec<usize>,
}

impl RecordMeta {
    fn new(
        fields_name: &[String],
        fields_type: &[TableDataType],
        src_schema: &RecordSchema,
        allow_missing_field: bool,
    ) -> Result<RecordMeta> {
        let mut src_fields = vec![None; src_schema.fields.len()];
        let mut defaults = vec![];
        for (c, name) in fields_name.iter().enumerate() {
            if let Some(pos) = src_schema.lookup.get(name) {
                let sub_fill_to = match (
                    &fields_type[c].remove_nullable(),
                    &src_schema.fields[*pos].schema,
                ) {
                    (
                        TableDataType::Tuple {
                            fields_type,
                            fields_name,
                        },
                        Schema::Record(record),
                    ) => Some(Self::new(
                        fields_name,
                        fields_type,
                        record,
                        allow_missing_field,
                    )?),
                    _ => None,
                };
                src_fields[*pos] = Some(FieldMeta {
                    fill_to_dest: c,
                    sub_record: sub_fill_to,
                });
            } else {
                if !allow_missing_field {
                    return Err(ErrorCode::BadArguments(format!("missing field {name}. Consider add avro file format option `MISSING_FIELD_AS = FIELD_DEFAULT`")));
                }
                defaults.push(c)
            }
        }
        if defaults.len() == src_schema.fields.len() {
            return Err(ErrorCode::BadArguments("no fields match by name"));
        }
        Ok(RecordMeta {
            src_fields,
            dest_fields_with_defaults: defaults,
        })
    }
}
pub(super) struct AvroDecoder {
    pub ctx: Arc<LoadContext>,
    pub is_rounding_mode: bool,
    pub params: AvroFileFormatParams,
}

impl AvroDecoder {
    pub fn new(ctx: Arc<LoadContext>, params: AvroFileFormatParams) -> Self {
        let is_rounding_mode = ctx.file_format_options_ext.is_rounding_mode;
        Self {
            is_rounding_mode,
            ctx,
            params,
        }
    }

    pub fn add(&self, state: &mut BlockBuilderState, data: WholeFileData) -> Result<()> {
        self.read_file(&data.data, state)
    }

    fn read_file(&self, file_data: &[u8], state: &mut BlockBuilderState) -> Result<()> {
        let reader = Reader::new(file_data).unwrap();
        let src_schema = reader.writer_schema().clone();
        let src_schema = if let Schema::Record(record) = src_schema {
            record
        } else {
            return Err(ErrorCode::BadArguments(
                "only support avro with record as top level type",
            ));
        };
        let names = self
            .ctx
            .schema
            .fields
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>();
        let types = self
            .ctx
            .schema
            .fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect::<Vec<_>>();
        let record_meta = RecordMeta::new(
            &names,
            &types,
            &src_schema,
            self.params.missing_field_as == NullAs::FieldDefault,
        )?;

        for (row, value) in reader.enumerate() {
            let value = value.unwrap();
            if let Value::Record(r) = value {
                self.read_record(r, &record_meta, &mut state.column_builders, &src_schema)
                    .map_err(|e| {
                        let path: String = e
                            .path
                            .iter()
                            .map(|(_, v)| v.clone())
                            .intersperse(".".to_string())
                            .collect();
                        format!(
                            "fail to load row {row}, column {path}: {} ",
                            e.reason.unwrap_or_default(),
                        )
                    })?;
                state.add_row(row)
            } else {
                return Err(ErrorCode::BadBytes("a avro row must be a record"));
            }
        }
        Ok(())
    }

    fn read_record(
        &self,
        record: Vec<(String, Value)>,
        record_meta: &RecordMeta,
        column_builders: &mut [ColumnBuilder],
        record_schema: &RecordSchema,
    ) -> ReadFieldResult {
        for (i, (name, v)) in record.into_iter().enumerate() {
            if let Some(Some(dest)) = record_meta.src_fields.get(i) {
                let t = column_builders[dest.fill_to_dest].data_type();
                self.read_field(
                    &mut column_builders[dest.fill_to_dest],
                    v,
                    &Some(dest.clone()),
                    &record_schema.fields[i].schema,
                )
                .map_err(|mut e| {
                    e.path.push((i, name));
                    Error {
                        reason: Some(e.reason.unwrap_or_else(|| {
                            format!(
                                "can not load {:?} to {}",
                                &record_schema.fields[i].schema, t
                            )
                        })),
                        path: e.path,
                    }
                })?;
            }
        }
        for c in &record_meta.dest_fields_with_defaults {
            column_builders[*c].push_default();
        }
        Ok(())
    }

    fn read_field(
        &self,
        column: &mut ColumnBuilder,
        value: Value,
        field_meta: &Option<FieldMeta>,
        schema: &Schema,
    ) -> ReadFieldResult {
        let (schema, value) = if let Value::Union(i, v) = value {
            let s = if let Schema::Union(u) = schema {
                u.variants()[i as usize].clone()
            } else {
                return Err(Error::not_match(&schema.clone(), &Value::Union(i, v)));
            };
            (&s.clone(), *v)
        } else {
            (schema, value)
        };

        match column {
            ColumnBuilder::Null { len } => self.read_null(len, value),
            ColumnBuilder::Boolean(c) => self.read_bool(c, value),
            ColumnBuilder::String(c) => self.read_string(c, value),
            ColumnBuilder::Binary(c) => self.read_binary(c, value),
            ColumnBuilder::Number(c) => with_number_mapped_type!(|NUM_TYPE| match c {
                NumberColumnBuilder::NUM_TYPE(c) => {
                    match value {
                        Value::Int(v) => self.read_number(c, v),
                        Value::Long(v) => self.read_number(c, v),
                        Value::Float(mut v) => {
                            if self.is_rounding_mode && !NUM_TYPE::FLOATING {
                                v = v.round()
                            }
                            self.read_number(c, OrderedFloat::<f32>(v))
                        }
                        Value::Double(mut v) => {
                            if self.is_rounding_mode && !NUM_TYPE::FLOATING {
                                v = v.round()
                            }
                            self.read_number(c, OrderedFloat::<f64>(v))
                        }
                        _ => Err(Error::default()),
                    }
                }
            }),
            ColumnBuilder::Variant(c) => self.read_variant(c, value),
            ColumnBuilder::Date(c) => self.read_date(c, value),
            ColumnBuilder::Timestamp(c) => self.read_timestamp(c, value),
            ColumnBuilder::Interval(c) => self.read_interval(c, value),
            ColumnBuilder::Nullable(c) => self.read_nullable(c, value, field_meta, schema),
            ColumnBuilder::Array(c) => self.read_array(c, value, field_meta, schema),
            ColumnBuilder::Map(c) => self.read_map(c, value, field_meta, schema),
            ColumnBuilder::Tuple(fields) => self.read_tuple(fields, value, field_meta, schema),
            // ColumnBuilder::Decimal(c) => with_decimal_type!(|DECIMAL_TYPE| match c {
            //     DecimalColumnBuilder::DECIMAL_TYPE(c, size) => self.read_decimal(c, *size, value),
            // }),
            _ => Err(Error::new_reason(format!(
                "loading avro to table with column of type {} not supported yet",
                column.data_type()
            ))),
        }
    }

    fn read_bool(&self, column: &mut MutableBitmap, value: Value) -> ReadFieldResult {
        match value {
            Value::Boolean(v) => column.push(v),
            _ => return Err(Error::default()),
        }
        Ok(())
    }

    fn read_null(&self, len: &mut usize, _value: Value) -> ReadFieldResult {
        *len += 1;
        Ok(())
    }

    fn read_number<T, F>(&self, column: &mut Vec<T>, value: F) -> ReadFieldResult
    where
        T: Number + From<T::Native>,
        T::Native: FromLexical + NumCast,
        F: Number + From<F::Native>,
        F::Native: FromLexical + NumCast,
    {
        if let Some(v) = num_traits::cast::cast(value) {
            column.push(v);
            Ok(())
        } else {
            Err(Error::default())
        }
    }

    fn read_string(&self, column: &mut StringColumnBuilder, value: Value) -> ReadFieldResult {
        match value {
            Value::String(s) => {
                column.put_str(s.as_str());
            }
            Value::Enum(_, s) => {
                column.put_str(s.as_str());
            }
            _ => return Err(Error::default()),
        }
        column.commit_row();
        Ok(())
    }

    fn read_binary(&self, column: &mut BinaryColumnBuilder, value: Value) -> ReadFieldResult {
        match value {
            Value::Bytes(v) => {
                column.put_slice(&v);
            }
            Value::Fixed(_, v) => {
                column.put_slice(&v);
            }
            _ => return Err(Error::default()),
        }
        column.commit_row();
        Ok(())
    }

    fn read_date(&self, column: &mut Vec<i32>, value: Value) -> ReadFieldResult {
        match value {
            Value::Date(v) => {
                column.push(v);
                Ok(())
            }
            _ => Err(Error::default()),
        }
    }

    fn read_timestamp(&self, column: &mut Vec<i64>, value: Value) -> ReadFieldResult {
        let v = match value {
            Value::TimestampMicros(v) => v * 1000,
            Value::TimestampMillis(v) => v * 1000000,
            Value::TimestampNanos(v) => v,
            _ => return Err(Error::default()),
        };
        column.push(v);
        Ok(())
    }

    // fn read_decimal<D: Decimal>(
    //     &self,
    //     column: &mut Vec<D>,
    //     size: DecimalSize,
    //     value: Value,
    // ) ->ReadFieldResult{
    //     let v = match value {
    //         Value::Decimal(v) => {v}
    //         Value::BigDecimal(v) => {v}
    //         _ => return Err(None),
    //     };
    //     column.push(v);
    //     Ok(())
    // }

    fn read_interval(&self, column: &mut Vec<months_days_micros>, value: Value) -> ReadFieldResult {
        match value {
            Value::Duration(d) => {
                let months: u32 = d.months().into();
                let days: u32 = d.days().into();
                let millis: u32 = d.millis().into();
                column.push(months_days_micros::new(
                    months as i32,
                    days as i32,
                    (millis * 1000) as i64,
                ));
                Ok(())
            }
            _ => Err(Error::default()),
        }
    }

    fn read_variant(&self, column: &mut BinaryColumnBuilder, value: Value) -> ReadFieldResult {
        let v = to_jsonb(&value).map_err(Error::new_reason)?;
        v.write_to_vec(&mut column.data);
        column.commit_row();
        Ok(())
    }

    fn read_nullable(
        &self,
        column: &mut NullableColumnBuilder<AnyType>,
        value: Value,
        field_meta: &Option<FieldMeta>,
        schema: &Schema,
    ) -> ReadFieldResult {
        match value {
            Value::Null => {
                column.push_null();
            }
            other => {
                self.read_field(&mut column.builder, other, field_meta, schema)?;
                column.validity.push(true);
            }
        }
        Ok(())
    }

    fn read_array(
        &self,
        column: &mut ArrayColumnBuilder<AnyType>,
        value: Value,
        field_meta: &Option<FieldMeta>,
        schema: &Schema,
    ) -> ReadFieldResult {
        match value {
            Value::Array(vals) => {
                let schema = if let Schema::Array(a) = schema {
                    a.items.as_ref()
                } else {
                    return Err(Error::not_match(schema, &Value::Array(vals)));
                };
                for val in vals {
                    self.read_field(&mut column.builder, val, field_meta, schema)?;
                }
                column.commit_row();
                Ok(())
            }
            _ => Err(Error::default()),
        }
    }

    fn read_map(
        &self,
        column: &mut ArrayColumnBuilder<AnyType>,
        value: Value,
        field_meta: &Option<FieldMeta>,
        schema: &Schema,
    ) -> ReadFieldResult {
        const KEY: usize = 0;
        const VALUE: usize = 1;
        let map_builder = column.builder.as_tuple_mut().unwrap();
        match value {
            Value::Map(obj) => {
                let schema = if let Schema::Map(m) = schema {
                    m.types.as_ref()
                } else {
                    return Err(Error::not_match(schema, &Value::Map(obj)));
                };
                for (key, val) in obj.into_iter() {
                    let key = Value::String(key.to_string());
                    let string_builder = map_builder[KEY].as_string_mut().unwrap();
                    self.read_string(string_builder, key)?;
                    self.read_field(&mut map_builder[VALUE], val, field_meta, schema)?;
                }
                column.commit_row();
                Ok(())
            }
            _ => Err(Error::default()),
        }
    }

    fn read_tuple(
        &self,
        column_builders: &mut [ColumnBuilder],
        value: Value,
        field_meta: &Option<FieldMeta>,
        schema: &Schema,
    ) -> ReadFieldResult {
        let field_meta = field_meta.as_ref().ok_or(Error::default())?;
        let record_meta = field_meta.sub_record.as_ref().ok_or(Error::default())?;
        match value {
            Value::Record(r) => {
                let schema = if let Schema::Record(s) = schema {
                    s
                } else {
                    return Err(Error::not_match(schema, &Value::Record(r)));
                };
                self.read_record(r, record_meta, column_builders, schema)?;
                Ok(())
            }
            _ => Err(Error::default()),
        }
    }
}
