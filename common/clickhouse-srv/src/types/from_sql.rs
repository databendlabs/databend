use std::net::Ipv4Addr;
use std::net::Ipv6Addr;

use chrono::prelude::*;
use chrono_tz::Tz;

use crate::errors::Error;
use crate::errors::FromSqlError;
use crate::errors::Result;
use crate::types::column::datetime64::to_datetime;
use crate::types::column::Either;
use crate::types::value::decode_ipv4;
use crate::types::value::decode_ipv6;
use crate::types::Decimal;
use crate::types::Enum16;
use crate::types::Enum8;
use crate::types::SqlType;
use crate::types::ValueRef;

pub type FromSqlResult<T> = Result<T>;

pub trait FromSql<'a>: Sized {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self>;
}

macro_rules! from_sql_impl {
    ( $( $t:ident: $k:ident ),* ) => {
        $(
            impl<'a> FromSql<'a> for $t {
                fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
                    match value {
                        ValueRef::$k(v) => Ok(v),
                        _ => {
                            let from = SqlType::from(value.clone()).to_string();
                            Err(Error::FromSql(FromSqlError::InvalidType { src: from, dst: stringify!($t).into() }))
                        }
                    }
                }
            }
        )*
    };
}

impl<'a> FromSql<'a> for Decimal {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Decimal(v) => Ok(v),
            _ => {
                let from = SqlType::from(value.clone()).to_string();
                Err(Error::FromSql(FromSqlError::InvalidType {
                    src: from,
                    dst: "Decimal".into(),
                }))
            }
        }
    }
}

impl<'a> FromSql<'a> for Enum8 {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Enum8(_enum_values, v) => Ok(v),
            _ => {
                let from = SqlType::from(value.clone()).to_string();

                Err(Error::FromSql(FromSqlError::InvalidType {
                    src: from,
                    dst: "Enum8".into(),
                }))
            }
        }
    }
}

impl<'a> FromSql<'a> for Enum16 {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Enum16(_enum_values, v) => Ok(v),
            _ => {
                let from = SqlType::from(value.clone()).to_string();

                Err(Error::FromSql(FromSqlError::InvalidType {
                    src: from,
                    dst: "Enum16".into(),
                }))
            }
        }
    }
}

impl<'a> FromSql<'a> for &'a str {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<&'a str> {
        value.as_str()
    }
}

impl<'a> FromSql<'a> for &'a [u8] {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<&'a [u8]> {
        value.as_bytes()
    }
}

impl<'a> FromSql<'a> for String {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        value.as_str().map(str::to_string)
    }
}

impl<'a> FromSql<'a> for Ipv4Addr {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Ipv4(ip) => Ok(decode_ipv4(&ip)),
            _ => {
                let from = SqlType::from(value.clone()).to_string();
                Err(Error::FromSql(FromSqlError::InvalidType {
                    src: from,
                    dst: "Ipv4".into(),
                }))
            }
        }
    }
}

impl<'a> FromSql<'a> for Ipv6Addr {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Ipv6(ip) => Ok(decode_ipv6(&ip)),
            _ => {
                let from = SqlType::from(value.clone()).to_string();
                Err(Error::FromSql(FromSqlError::InvalidType {
                    src: from,
                    dst: "Ipv6".into(),
                }))
            }
        }
    }
}

impl<'a> FromSql<'a> for uuid::Uuid {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Uuid(row) => {
                let mut buffer = row;
                buffer[..8].reverse();
                buffer[8..].reverse();
                Ok(uuid::Uuid::from_bytes(buffer))
            }
            _ => {
                let from = SqlType::from(value.clone()).to_string();
                Err(Error::FromSql(FromSqlError::InvalidType {
                    src: from,
                    dst: "Uuid".into(),
                }))
            }
        }
    }
}

macro_rules! from_sql_vec_impl {
    ( $( $t:ty: $k:pat => $f:expr ),* ) => {
        $(
            impl<'a> FromSql<'a> for Vec<$t> {
                fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
                    match value {
                        ValueRef::Array($k, vs) => {
                            let f: fn(ValueRef<'a>) -> FromSqlResult<$t> = $f;
                            let mut result = Vec::with_capacity(vs.len());
                            for v in vs.iter() {
                                let value: $t = f(v.clone())?;
                                result.push(value);
                            }
                            Ok(result)
                        }
                        _ => {
                            let from = SqlType::from(value.clone()).to_string();
                            Err(Error::FromSql(FromSqlError::InvalidType {
                                src: from,
                                dst: format!("Vec<{}>", stringify!($t)).into(),
                            }))
                        }
                    }
                }
            }
        )*
    };
}

from_sql_vec_impl! {
    &'a str: SqlType::String => |v| v.as_str(),
    String: SqlType::String => |v| v.as_string(),
    &'a [u8]: SqlType::String => |v| v.as_bytes(),
    Vec<u8>: SqlType::String => |v| v.as_bytes().map(<[u8]>::to_vec),
    Date<Tz>: SqlType::Date => |z| Ok(z.into()),
    DateTime<Tz>: SqlType::DateTime(_) => |z| Ok(z.into())
}

impl<'a> FromSql<'a> for Vec<u8> {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Array(SqlType::UInt8, vs) => {
                let mut result = Vec::with_capacity(vs.len());
                for v in vs.iter() {
                    result.push(v.clone().into());
                }
                Ok(result)
            }
            _ => value.as_bytes().map(|bs| bs.to_vec()),
        }
    }
}

macro_rules! from_sql_vec_impl {
    ( $( $t:ident: $k:ident ),* ) => {
        $(
            impl<'a> FromSql<'a> for Vec<$t> {
                fn from_sql(value: ValueRef<'a>) -> Result<Self> {
                    match value {
                        ValueRef::Array(SqlType::$k, vs) => {
                            let mut result = Vec::with_capacity(vs.len());
                            for v in vs.iter() {
                                let val: $t = v.clone().into();
                                result.push(val);
                            }
                            Ok(result)
                        }
                        _ => {
                            let from = SqlType::from(value.clone()).to_string();
                            Err(Error::FromSql(FromSqlError::InvalidType { src: from, dst: stringify!($t).into() }))
                        }
                    }
                }
            }
        )*
    };
}

from_sql_vec_impl! {
    i8: Int8,
    i16: Int16,
    i32: Int32,
    i64: Int64,

    u16: UInt16,
    u32: UInt32,
    u64: UInt64,

    f32: Float32,
    f64: Float64
}

impl<'a, T> FromSql<'a> for Option<T>
where T: FromSql<'a>
{
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Nullable(e) => match e {
                Either::Left(_) => Ok(None),
                Either::Right(u) => {
                    let value_ref = u.as_ref().clone();
                    Ok(Some(T::from_sql(value_ref)?))
                }
            },
            _ => {
                let from = SqlType::from(value.clone()).to_string();
                Err(Error::FromSql(FromSqlError::InvalidType {
                    src: from,
                    dst: stringify!($t).into(),
                }))
            }
        }
    }
}

impl<'a> FromSql<'a> for Date<Tz> {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Date(v, tz) => {
                let time = tz.timestamp(i64::from(v) * 24 * 3600, 0);
                Ok(time.date())
            }
            _ => {
                let from = SqlType::from(value).to_string();
                Err(Error::FromSql(FromSqlError::InvalidType {
                    src: from,
                    dst: "Date<Tz>".into(),
                }))
            }
        }
    }
}

impl<'a> FromSql<'a> for DateTime<Tz> {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        match value {
            ValueRef::DateTime(v, tz) => {
                let time = tz.timestamp(i64::from(v), 0);
                Ok(time)
            }
            ValueRef::DateTime64(v, params) => {
                let (precision, tz) = *params;
                Ok(to_datetime(v, precision, tz))
            }
            _ => {
                let from = SqlType::from(value).to_string();
                Err(Error::FromSql(FromSqlError::InvalidType {
                    src: from,
                    dst: "DateTime<Tz>".into(),
                }))
            }
        }
    }
}

from_sql_impl! {
    u8: UInt8,
    u16: UInt16,
    u32: UInt32,
    u64: UInt64,

    i8: Int8,
    i16: Int16,
    i32: Int32,
    i64: Int64,

    f32: Float32,
    f64: Float64
}

#[cfg(test)]
mod test {
    use chrono::prelude::*;
    use chrono_tz::Tz;

    use crate::types::column::Either;
    use crate::types::from_sql::FromSql;
    use crate::types::DateTimeType;
    use crate::types::SqlType;
    use crate::types::ValueRef;

    #[test]
    fn test_u8() {
        let v = ValueRef::from(42_u8);
        let actual = u8::from_sql(v).unwrap();
        assert_eq!(actual, 42_u8);
    }

    #[test]
    fn test_bad_convert() {
        let v = ValueRef::from(42_u16);
        match u32::from_sql(v) {
            Ok(_) => panic!("should fail"),
            Err(e) => assert_eq!(
                "From SQL error: `SqlType::UInt16 cannot be cast to u32.`".to_string(),
                format!("{}", e)
            ),
        }
    }

    #[test]
    fn null_to_datetime() {
        let null_value = ValueRef::Nullable(Either::Left(
            SqlType::DateTime(DateTimeType::DateTime32).into(),
        ));
        let date = Option::<DateTime<Tz>>::from_sql(null_value);
        assert_eq!(date.unwrap(), None);
    }
}
