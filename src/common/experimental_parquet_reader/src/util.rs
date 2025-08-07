use databend_common_expression::types::NumberDataType;
use databend_common_expression::TableDataType;
use parquet2::schema::types::PhysicalType;
use parquet2::schema::types::PrimitiveType;
use parquet2::schema::Repetition;

pub fn from_table_field_type(field_name: String, field_type: &TableDataType) -> PrimitiveType {
    let (inner_type, is_nullable) = match field_type {
        TableDataType::Nullable(inner) => (inner.as_ref(), true),
        other => (other, false),
    };

    let mut parquet_primitive_type = match inner_type {
        TableDataType::String => PrimitiveType::from_physical(field_name, PhysicalType::ByteArray),
        TableDataType::Number(number_type) => match number_type {
            NumberDataType::Int8 => PrimitiveType::from_physical(field_name, PhysicalType::Int32),
            NumberDataType::Int16 => PrimitiveType::from_physical(field_name, PhysicalType::Int32),
            NumberDataType::Int32 => PrimitiveType::from_physical(field_name, PhysicalType::Int32),
            NumberDataType::Int64 => PrimitiveType::from_physical(field_name, PhysicalType::Int64),
            NumberDataType::UInt8 => PrimitiveType::from_physical(field_name, PhysicalType::Int32),
            NumberDataType::UInt16 => PrimitiveType::from_physical(field_name, PhysicalType::Int32),
            NumberDataType::UInt32 => PrimitiveType::from_physical(field_name, PhysicalType::Int64),
            NumberDataType::UInt64 => PrimitiveType::from_physical(field_name, PhysicalType::Int64),
            NumberDataType::Float32 => {
                PrimitiveType::from_physical(field_name, PhysicalType::Float)
            }
            NumberDataType::Float64 => {
                PrimitiveType::from_physical(field_name, PhysicalType::Double)
            }
        },
        TableDataType::Decimal(decimal_type) => {
            let precision = decimal_type.precision();
            let _scale = decimal_type.scale();
            if precision <= 9 {
                PrimitiveType::from_physical(field_name, PhysicalType::Int32)
            } else if precision <= 18 {
                PrimitiveType::from_physical(field_name, PhysicalType::Int64)
            } else {
                let len = decimal_length_from_precision(precision as usize);
                PrimitiveType::from_physical(field_name, PhysicalType::FixedLenByteArray(len))
            }
        }
        TableDataType::Date => PrimitiveType::from_physical(field_name, PhysicalType::Int32),
        TableDataType::Nullable(_) => unreachable!("Nullable should have been unwrapped"),
        t => unimplemented!("Unsupported type: {:?} ", t),
    };

    if !is_nullable {
        parquet_primitive_type.field_info.repetition = Repetition::Required;
    }

    parquet_primitive_type
}

fn decimal_length_from_precision(precision: usize) -> usize {
    (((10.0_f64.powi(precision as i32) + 1.0).log2() + 1.0) / 8.0).ceil() as usize
}

pub fn calculate_parquet_max_levels(data_type: &TableDataType) -> (i16, i16) {
    match data_type {
        TableDataType::Boolean => (0, 0),
        TableDataType::Binary => (0, 0),
        TableDataType::String => (0, 0),
        TableDataType::Number(_) => (0, 0),
        TableDataType::Decimal(_) => (0, 0),
        TableDataType::Timestamp => (0, 0),
        TableDataType::Date => (0, 0),
        TableDataType::Interval => (0, 0),
        TableDataType::Bitmap => (0, 0),
        TableDataType::Variant => (0, 0),
        TableDataType::Geometry => (0, 0),
        TableDataType::Geography => (0, 0),
        TableDataType::Vector(_) => (0, 0),

        TableDataType::Nullable(inner) => {
            let (inner_def, inner_rep) = calculate_parquet_max_levels(inner);
            (inner_def + 1, inner_rep)
        }

        TableDataType::Null
        | TableDataType::EmptyArray
        | TableDataType::EmptyMap
        | TableDataType::Array(_)
        | TableDataType::Map(_)
        | TableDataType::Tuple { .. } => unimplemented!(),
    }
}
