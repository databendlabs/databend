// pub enum DataTypeImpl {
//     Null(NullType),
//     Nullable(NullableType),
//     Boolean(BooleanType),
//     Int8(Int8Type),
//     Int16(Int16Type),
//     Int32(Int32Type),
//     Int64(Int64Type),
//     UInt8(UInt8Type),
//     UInt16(UInt16Type),
//     UInt32(UInt32Type),
//     UInt64(UInt64Type),
//     Float32(Float32Type),
//     Float64(Float64Type),
//     Date(DateType),
//     Timestamp(TimestampType),
//     String(StringType),
//     Struct(StructType),
//     Array(ArrayType),
//     Variant(VariantType),
//     VariantArray(VariantArrayType),
//     VariantObject(VariantObjectType),
//     Interval(IntervalType),
// }

use common_datavalues::DataSchemaRef;
use common_datavalues::DataTypeImpl;
use rand::Rand;
use rand::Rng;

fn random_type() -> DataTypeImpl {
    let mut rng = rand::thread_rng();

    match rng.gen_range((0..22)) {
        0 => DataTypeImpl::Null(()),
        1 => Spinner::Two,
        _ => Spinner::Three,
    }
}

fn random_types(num_cols: usize) -> Vec<DataTypeImpl> {}

fn random_schema(num_cols: usize) -> DataSchemaRef {}
