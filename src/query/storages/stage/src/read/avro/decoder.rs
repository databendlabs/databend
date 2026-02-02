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

use apache_avro::Reader;
use apache_avro::Schema;
use apache_avro::schema::UnionSchema;
use apache_avro::types::Value;
use databend_common_base::base::OrderedFloat;
use databend_common_column::bitmap::MutableBitmap;
use databend_common_column::types::months_days_micros;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::Decimal;
use databend_common_expression::types::DecimalColumnBuilder;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::Number;
use databend_common_expression::types::NumberColumnBuilder;
use databend_common_expression::types::array::ArrayColumnBuilder;
use databend_common_expression::types::binary::BinaryColumnBuilder;
use databend_common_expression::types::nullable::NullableColumnBuilder;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_expression::with_decimal_type;
use databend_common_expression::with_number_mapped_type;
use databend_common_meta_app::principal::AvroFileFormatParams;
use databend_common_meta_app::principal::NullAs;
use databend_common_storage::FileParseError;
use lexical_core::FromLexical;
use num_bigint::BigInt;
use num_traits::NumCast;
use uuid;

use crate::read::avro::avro_to_jsonb::to_jsonb;
use crate::read::avro::schema_match::MatchedField;
use crate::read::avro::schema_match::MatchedSchema;
use crate::read::avro::schema_match::SchemaMatcher;
use crate::read::block_builder_state::BlockBuilderState;
use crate::read::default_expr_evaluator::DefaultExprEvaluator;
use crate::read::error_handler::ErrorHandler;
use crate::read::load_context::LoadContext;

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

    pub fn value_schema_not_match(schema: &MatchedSchema, value: &Value) -> Self {
        Self {
            reason: Some(format!(
                "schema ({schema:?}) and value ({value:?}) not match:"
            )),
            path: vec![],
        }
    }
}

type ReadFieldResult = std::result::Result<(), Error>;

pub(super) struct AvroDecoder {
    pub is_rounding_mode: bool,
    pub params: AvroFileFormatParams,
    // todo(youngsofun): impl error_mode::continue
    #[allow(dead_code)]
    pub error_handler: Arc<ErrorHandler>,
    pub schema: TableSchemaRef,
    pub default_expr_evaluator: Option<Arc<DefaultExprEvaluator>>,
    is_select: bool,
}

fn date_time_to_int(schema: &mut Schema) {
    match schema {
        Schema::Array(a) => date_time_to_int(&mut a.items),
        Schema::Map(m) => date_time_to_int(&mut m.types),
        Schema::Union(u) => {
            *u = {
                let mut variants = u.variants().to_owned();
                variants.iter_mut().for_each(date_time_to_int);
                UnionSchema::new(variants).expect("should never fail")
            }
        }
        Schema::Record(r) => r
            .fields
            .iter_mut()
            .for_each(|f| date_time_to_int(&mut f.schema)),
        Schema::Date => *schema = Schema::Int,
        Schema::TimestampMillis
        | Schema::TimestampMicros
        | Schema::TimestampNanos
        | Schema::LocalTimestampMillis
        | Schema::LocalTimestampMicros
        | Schema::LocalTimestampNanos => *schema = Schema::Long,
        _ => {}
    }
}

impl AvroDecoder {
    pub fn new(ctx: Arc<LoadContext>, params: AvroFileFormatParams) -> Self {
        let is_rounding_mode = ctx.settings.is_rounding_mode;
        let is_select = ctx.is_select;
        Self {
            is_rounding_mode,
            params,
            schema: ctx.schema.clone(),
            error_handler: ctx.error_handler.clone(),
            default_expr_evaluator: ctx.default_expr_evaluator.clone(),
            is_select,
        }
    }

    fn read_file_for_select(
        &self,
        reader: Reader<&[u8]>,
        state: &mut BlockBuilderState,
        schema: Schema,
    ) -> Result<()> {
        let schema = MatchedSchema::Primary(schema);
        for (row, value) in reader.enumerate() {
            let column_builder = if let ColumnBuilder::Variant(b) = &mut state.column_builders[0] {
                b
            } else {
                return Err(ErrorCode::Internal(
                    "invalid column builder when querying avro",
                ));
            };
            if let Err(e) = self.read_variant(column_builder, value.unwrap(), &schema) {
                self.error_handler.on_error(
                    FileParseError::InvalidRow {
                        format: "AVRO".to_string(),
                        message: e.reason.unwrap_or_default().to_string(),
                    }
                    .with_row(row),
                    None,
                    &mut state.file_status,
                    &state.file_path,
                )?;
            }
            state.add_row(row)
        }
        Ok(())
    }
    pub fn read_file(&self, file_data: &[u8], state: &mut BlockBuilderState) -> Result<()> {
        let reader = match Reader::new(file_data) {
            Ok(r) => r,
            Err(e) => {
                let e = match e {
                    apache_avro::Error::HeaderMagic | apache_avro::Error::ReadHeader(_) => {
                        FileParseError::WrongFileType {
                            format: "AVRO".to_string(),
                            message: e.to_string(),
                        }
                    }
                    _ => FileParseError::BadFile {
                        format: "AVRO".to_string(),
                        message: e.to_string(),
                    },
                };
                return self.error_handler.on_error(
                    e.with_row(0),
                    None,
                    &mut state.file_status,
                    &state.file_path,
                );
            }
        };

        let mut schema = reader.writer_schema().clone();
        if !self.params.use_logic_type {
            date_time_to_int(&mut schema);
        }
        if self.is_select {
            return self.read_file_for_select(reader, state, schema);
        }
        let src_schema = if let Schema::Record(record) = schema {
            record
        } else {
            let e = FileParseError::WrongRootType {
                message: "copy from AVRO only support record as root type".to_string(),
            };
            return self.error_handler.on_error(
                e.with_row(0),
                None,
                &mut state.file_status,
                &state.file_path,
            );
        };
        let names = self
            .schema
            .fields
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>();
        let types = self
            .schema
            .fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect::<Vec<_>>();
        let matcher = SchemaMatcher {
            allow_missing_field: self.params.missing_field_as == NullAs::FieldDefault,
        };

        let matched_fields = matcher.match_fields(&names, &types, &src_schema, 0)?;

        for (row, value) in reader.enumerate() {
            let value = value.unwrap();
            if let Value::Record(r) = value {
                self.read_record(r, &matched_fields, &mut state.column_builders)
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
        matched_fields: &[MatchedField],
        column_builders: &mut [ColumnBuilder],
    ) -> ReadFieldResult {
        assert_eq!(column_builders.len(), matched_fields.len());
        for (i, (column_builder, mf)) in column_builders.iter_mut().zip(matched_fields).enumerate()
        {
            match mf {
                MatchedField::FieldDefault => {
                    if let Some(eval) = &self.default_expr_evaluator {
                        eval.push_default_value(column_builder, i)
                            .map_err(|e| Error::new_reason(e.to_string()))?;
                    } else {
                        column_builder.push_default();
                    }
                }
                MatchedField::TypeDefault => {
                    column_builder.push_default();
                }
                MatchedField::Value(matched_schema, pos) => {
                    let pos = *pos;
                    assert!(pos < record.len());
                    let name = record[pos].0.clone();
                    self.read_field(column_builder, record[pos].1.clone(), matched_schema)
                        .map_err(|mut e| {
                            e.path.push((i, name));
                            Error {
                                reason: Some(e.reason.unwrap_or_else(|| {
                                    format!(
                                        "can not load value of type {:?} to {}",
                                        &matched_schema,
                                        column_builder.data_type()
                                    )
                                })),
                                path: e.path,
                            }
                        })?;
                }
            }
        }
        Ok(())
    }

    fn read_field(
        &self,
        builder: &mut ColumnBuilder,
        value: Value,
        matched_schema: &MatchedSchema,
    ) -> ReadFieldResult {
        let (matched_schema, value) = if let Value::Union(i, v) = value {
            let s = if let MatchedSchema::Union(u) = matched_schema {
                u.variants[i as usize].clone()
            } else {
                return Err(Error::value_schema_not_match(
                    &matched_schema.clone(),
                    &Value::Union(i, v),
                ));
            };
            (&s.clone(), *v)
        } else {
            (matched_schema, value)
        };

        match builder {
            ColumnBuilder::Nullable(c) => self.read_nullable(c, value, matched_schema),
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
                        Value::TimestampMicros(v)
                        | Value::LocalTimestampMicros(v)
                        | Value::TimestampMillis(v)
                        | Value::LocalTimestampMillis(v)
                        | Value::TimestampNanos(v)
                        | Value::LocalTimestampNanos(v) => self.read_number(c, v),
                        Value::Date(v) => self.read_number(c, v),
                        _ => Err(Error::default()),
                    }
                }
            }),
            ColumnBuilder::Date(c) => self.read_date(c, value),
            ColumnBuilder::Timestamp(c) => self.read_timestamp(c, value),
            ColumnBuilder::Interval(c) => self.read_interval(c, value),
            ColumnBuilder::Variant(c) => self.read_variant(c, value, matched_schema),
            ColumnBuilder::Decimal(c) => with_decimal_type!(|DECIMAL_TYPE| match c {
                DecimalColumnBuilder::DECIMAL_TYPE(c, size) =>
                    self.read_decimal(c, *size, value, matched_schema),
            }),
            ColumnBuilder::Array(c) => self.read_array(c, value, matched_schema),
            ColumnBuilder::Map(c) => self.read_map(c, value, matched_schema),
            ColumnBuilder::Tuple(fields) => self.read_tuple(fields, value, matched_schema),

            // todo: Bitmap, Geometry, Geography
            _ => Err(Error::new_reason(format!(
                "loading avro to table with column of type {} not supported yet",
                builder.data_type()
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
            Value::Uuid(u) => {
                let mut encode_buffer = uuid::Uuid::encode_buffer();
                column.put_str(u.hyphenated().encode_lower(&mut encode_buffer));
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
            Value::TimestampMillis(v) | Value::LocalTimestampMillis(v) => v * 1000,
            Value::TimestampMicros(v) | Value::LocalTimestampMicros(v) => v,
            Value::TimestampNanos(v) | Value::LocalTimestampNanos(v) => v / 1000,
            _ => return Err(Error::default()),
        };
        column.push(v);
        Ok(())
    }

    fn read_decimal<D: Decimal>(
        &self,
        column: &mut Vec<D>,
        size: DecimalSize,
        value: Value,
        schema: &MatchedSchema,
    ) -> ReadFieldResult {
        match (value, schema) {
            (Value::Decimal(d1), MatchedSchema::Decimal { multiplier }) => {
                let big_int = <BigInt>::from(d1);
                if let Some(d1) = <D>::from_bigint(big_int) {
                    let d = if let Some(m) = multiplier {
                        d1.checked_mul(D::from_i256_uncheck(*m)).unwrap()
                    } else {
                        d1
                    };
                    column.push(d);
                } else {
                    return Err(Error::default());
                }
            }
            (Value::BigDecimal(v), MatchedSchema::Primary(Schema::BigDecimal)) => {
                let v_precision = v.digits() as i64;
                let v_leading_digits = v_precision - v.fractional_digit_count();
                let (big_int, v_scale) = v.into_bigint_and_exponent();
                if v_leading_digits <= size.leading_digits() as i64
                    && v_scale <= size.scale() as i64
                {
                    let Some(mut d1) = <D>::from_bigint(big_int) else {
                        return Err(Error::default());
                    };
                    let scale_diff = (size.scale() as i64) - v_scale;
                    if scale_diff > 0 {
                        d1 = d1
                            .checked_mul(D::e(scale_diff as u8))
                            .expect("rescale should not overflow");
                    }
                    column.push(d1);
                } else {
                    return Err(Error::default());
                }
            }
            _ => return Err(Error::default()),
        };
        Ok(())
    }

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

    fn read_variant(
        &self,
        column: &mut BinaryColumnBuilder,
        value: Value,
        matched_schema: &MatchedSchema,
    ) -> ReadFieldResult {
        if let MatchedSchema::Primary(schema) = matched_schema {
            let v = to_jsonb(&value, schema).map_err(Error::new_reason)?;
            v.write_to_vec(&mut column.data);
            column.commit_row();
            Ok(())
        } else {
            Err(Error::default())
        }
    }

    fn read_nullable(
        &self,
        column: &mut NullableColumnBuilder<AnyType>,
        value: Value,
        matched_schema: &MatchedSchema,
    ) -> ReadFieldResult {
        match value {
            Value::Null => {
                column.push_null();
            }
            other => {
                self.read_field(&mut column.builder, other, matched_schema)?;
                column.validity.push(true);
            }
        }
        Ok(())
    }

    fn read_array(
        &self,
        column: &mut ArrayColumnBuilder<AnyType>,
        value: Value,
        matched_schema: &MatchedSchema,
    ) -> ReadFieldResult {
        match value {
            Value::Array(vals) => {
                let item_matched_schema = if let MatchedSchema::Array(a) = matched_schema {
                    a
                } else {
                    return Err(Error::value_schema_not_match(
                        matched_schema,
                        &Value::Array(vals),
                    ));
                };
                for val in vals {
                    self.read_field(&mut column.builder, val, item_matched_schema)?;
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
        matched_schema: &MatchedSchema,
    ) -> ReadFieldResult {
        const KEY: usize = 0;
        const VALUE: usize = 1;
        let map_builder = column.builder.as_tuple_mut().unwrap();
        match value {
            Value::Map(obj) => {
                let value_matched_schema = if let MatchedSchema::Map(m) = matched_schema {
                    m
                } else {
                    return Err(Error::value_schema_not_match(
                        matched_schema,
                        &Value::Map(obj),
                    ));
                };
                for (key, val) in obj.into_iter() {
                    let key = Value::String(key.to_string());
                    let string_builder = map_builder[KEY].as_string_mut().unwrap();
                    self.read_string(string_builder, key)?;
                    self.read_field(&mut map_builder[VALUE], val, value_matched_schema)?;
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
        matched_schema: &MatchedSchema,
    ) -> ReadFieldResult {
        match value {
            Value::Record(r) => {
                let matched_fields = if let MatchedSchema::Record(s) = matched_schema {
                    s
                } else {
                    return Err(Error::value_schema_not_match(
                        matched_schema,
                        &Value::Record(r),
                    ));
                };
                self.read_record(r, matched_fields, column_builders)?;
                Ok(())
            }
            _ => Err(Error::default()),
        }
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;
    use std::sync::Arc;

    use apache_avro::BigDecimal;
    use apache_avro::Decimal;
    use apache_avro::Schema;
    use apache_avro::Writer;
    use apache_avro::types::Value;
    use databend_common_expression::ColumnBuilder;
    use databend_common_expression::ScalarRef;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use databend_common_expression::TableSchemaRef;
    use databend_common_expression::types::DecimalDataType;
    use databend_common_expression::types::DecimalScalar;
    use databend_common_expression::types::DecimalSize;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::NumberScalar;
    use num_bigint::BigInt;
    use serde_json::json;

    use crate::read::avro::decoder::AvroDecoder;
    use crate::read::block_builder_state::BlockBuilderState;
    use crate::read::error_handler::ErrorHandler;

    fn make_file(fields: &[serde_json::value::Value], values: Vec<Value>) -> Vec<u8> {
        let fields = fields
            .iter()
            .enumerate()
            .map(|(i, v)| {
                json!({
                    "name": format!("c{i}"),
                    "type": v
                })
            })
            .collect::<Vec<_>>();

        let schema = json!(
        {
            "type": "record",
            "name": "test_record",
            "fields": fields,
        });

        let mut buffer = Vec::new();
        let schema = Schema::parse(&schema).unwrap();
        let mut writer = Writer::new(&schema, &mut buffer);
        let values = values
            .iter()
            .enumerate()
            .map(|(i, v)| (format!("c{i}"), v.clone()))
            .collect::<Vec<_>>();
        let row = Value::Record(values);
        writer.append(row).unwrap();
        writer.flush().unwrap();
        buffer
    }

    fn make_decoder(table_schema: TableSchemaRef) -> AvroDecoder {
        AvroDecoder {
            is_rounding_mode: false,
            params: Default::default(),
            error_handler: Arc::new(ErrorHandler {
                on_error_mode: Default::default(),
                on_error_count: Default::default(),
            }),
            schema: table_schema,
            default_expr_evaluator: None,
            is_select: false,
        }
    }

    fn make_table_schema(fields: Vec<TableDataType>) -> Arc<TableSchema> {
        let fields = fields
            .into_iter()
            .enumerate()
            .map(|(i, t)| TableField::new(&format!("c{i}"), t))
            .collect();
        Arc::new(TableSchema::new(fields))
    }

    fn test_single_field(
        table_data_type: TableDataType,
        avro_schema: serde_json::value::Value,
        value: Value,
        expected: ScalarRef,
    ) -> databend_common_exception::Result<()> {
        test_helper(vec![table_data_type], vec![avro_schema], vec![value], vec![
            expected,
        ])
    }

    fn test_helper(
        table_fields: Vec<TableDataType>,
        avro_fields: Vec<serde_json::value::Value>,
        values: Vec<Value>,
        expected: Vec<ScalarRef>,
    ) -> databend_common_exception::Result<()> {
        let f = make_file(&avro_fields, values);
        let table_schema = make_table_schema(table_fields);
        let decoder = make_decoder(table_schema.clone());
        let column_builders: Vec<_> = table_schema
            .fields()
            .iter()
            .map(|f| ColumnBuilder::with_capacity_hint(&f.data_type().into(), 1024, false))
            .collect();
        let mut state = BlockBuilderState {
            column_builders,
            ..Default::default()
        };
        decoder.read_file(&f, &mut state)?;
        let columns = state.take_columns(true).unwrap();
        let row = columns
            .iter()
            .map(|c| c.index(0).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(expected, row);
        Ok(())
    }

    #[test]
    fn test_decimal() -> Result<(), String> {
        let avro_schema = json!(
        {
          "type": "bytes",
          "logicalType": "decimal",
          "precision": 5,
          "scale": 2
        });
        let make_value = |s: &str| {
            let big_int = BigInt::from_str(s).unwrap();
            Value::Decimal(Decimal::from(big_int.to_signed_bytes_be()))
        };
        let value = make_value("12345");
        let decimal_size = DecimalSize::new_unchecked(7, 4);
        let table_field = TableDataType::Decimal(DecimalDataType::Decimal256(decimal_size));
        let expected = ScalarRef::Decimal(DecimalScalar::Decimal64(1234500, decimal_size));
        test_single_field(
            table_field,
            avro_schema.clone(),
            value.clone(),
            expected.clone(),
        )
        .unwrap();

        // smaller leading digits (p - s)
        let decimal_size = DecimalSize::new_unchecked(6, 4);
        let table_field = TableDataType::Decimal(DecimalDataType::Decimal256(decimal_size));
        assert!(test_single_field(table_field, avro_schema, value, expected).is_err());

        Ok(())
    }

    #[test]
    fn test_big_decimal() -> Result<(), String> {
        let avro_schema = json!(
        {
          "type": "bytes",
          "logicalType": "big-decimal",
        });
        let make_value = |s: &str| {
            let big_int = BigInt::from_str(s).unwrap();
            Value::BigDecimal(BigDecimal::new(big_int, 2))
        };
        let decimal_size = DecimalSize::new_unchecked(7, 4);
        let table_field = TableDataType::Decimal(DecimalDataType::Decimal256(decimal_size));

        let value = make_value("12345");
        let expected = ScalarRef::Decimal(DecimalScalar::Decimal64(1234500, decimal_size));
        test_single_field(
            table_field.clone(),
            avro_schema.clone(),
            value,
            expected.clone(),
        )
        .unwrap();

        let value = make_value("123456");
        assert!(test_single_field(table_field, avro_schema, value, expected).is_err());

        Ok(())
    }

    #[test]
    fn test_union() -> Result<(), String> {
        let int64 = TableDataType::Number(NumberDataType::Int64);
        let int64_nullable = TableDataType::Nullable(Box::new(int64.clone()));
        assert!(
            test_single_field(
                int64.clone(),
                json!(["null", "int"]),
                Value::Union(0, Box::new(Value::Null)),
                ScalarRef::Null
            )
            .is_err()
        );

        assert!(
            test_single_field(
                int64.clone(),
                json!(["long", "int"]),
                Value::Union(1, Box::new(Value::Int(100))),
                ScalarRef::Number(NumberScalar::Int64(100))
            )
            .is_err()
        );

        assert!(
            test_single_field(
                int64_nullable.clone(),
                json!(["null", "int"]),
                Value::Union(0, Box::new(Value::Null)),
                ScalarRef::Null
            )
            .is_ok()
        );

        assert!(
            test_single_field(
                int64_nullable.clone(),
                json!(["null", "int"]),
                Value::Union(1, Box::new(Value::Int(100))),
                ScalarRef::Number(NumberScalar::Int64(100))
            )
            .is_ok()
        );
        Ok(())
    }
}
