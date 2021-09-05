use std::convert;
use std::fmt;
use std::sync::Arc;

use chrono::prelude::*;
use chrono::Date;
use chrono_tz::Tz;

use crate::binary::Encoder;
use crate::binary::ReadEx;
use crate::errors::Result;
use crate::types::column::array::ArrayColumnData;
use crate::types::column::column_data::BoxColumnData;
use crate::types::column::column_data::ColumnData;
use crate::types::column::list::List;
use crate::types::column::nullable::NullableColumnData;
use crate::types::column::numeric::save_data;
use crate::types::column::ArcColumnWrapper;
use crate::types::column::ColumnFrom;
use crate::types::column::ColumnWrapper;
use crate::types::column::Either;
use crate::types::DateConverter;
use crate::types::Marshal;
use crate::types::SqlType;
use crate::types::StatBuffer;
use crate::types::Unmarshal;
use crate::types::Value;
use crate::types::ValueRef;

pub struct DateColumnData<T>
where T: StatBuffer
        + Unmarshal<T>
        + Marshal
        + Copy
        + convert::Into<Value>
        + convert::From<Value>
        + fmt::Display
        + Sync
        + Default
        + 'static
{
    data: List<T>,
    tz: Tz,
}

impl<T> DateColumnData<T>
where T: StatBuffer
        + Unmarshal<T>
        + Marshal
        + Copy
        + convert::Into<Value>
        + convert::From<Value>
        + fmt::Display
        + Sync
        + Default
        + 'static
{
    pub(crate) fn with_capacity(capacity: usize, timezone: Tz) -> DateColumnData<T> {
        DateColumnData {
            data: List::with_capacity(capacity),
            tz: timezone,
        }
    }

    pub(crate) fn load<R: ReadEx>(
        reader: &mut R,
        size: usize,
        tz: Tz,
    ) -> Result<DateColumnData<T>> {
        let mut data = List::with_capacity(size);
        unsafe {
            data.set_len(size);
        }
        reader.read_bytes(data.as_mut())?;
        Ok(DateColumnData { data, tz })
    }
}

impl ColumnFrom for Vec<Date<Tz>> {
    fn column_from<W: ColumnWrapper>(source: Self) -> W::Wrapper {
        let mut data = List::<u16>::with_capacity(source.len());
        for s in source {
            data.push(u16::get_days(s));
        }

        let column: DateColumnData<u16> = DateColumnData { data, tz: Tz::Zulu };
        W::wrap(column)
    }
}

impl ColumnFrom for Vec<Vec<Date<Tz>>> {
    fn column_from<W: ColumnWrapper>(source: Self) -> W::Wrapper {
        let fake: Vec<Date<Tz>> = Vec::with_capacity(source.len());
        let inner = Vec::column_from::<ArcColumnWrapper>(fake);
        let sql_type = inner.sql_type();

        let mut data = ArrayColumnData {
            inner,
            offsets: List::with_capacity(source.len()),
        };

        for vs in source {
            let mut inner = Vec::with_capacity(vs.len());
            for v in vs {
                let days = u16::get_days(v);
                let value: Value = Value::Date(days, v.timezone());
                inner.push(value);
            }
            data.push(Value::Array(sql_type.clone().into(), Arc::new(inner)));
        }

        W::wrap(data)
    }
}

impl ColumnFrom for Vec<Vec<DateTime<Tz>>> {
    fn column_from<W: ColumnWrapper>(source: Self) -> W::Wrapper {
        let fake: Vec<DateTime<Tz>> = Vec::with_capacity(source.len());
        let inner = Vec::column_from::<ArcColumnWrapper>(fake);
        let sql_type = inner.sql_type();

        let mut data = ArrayColumnData {
            inner,
            offsets: List::with_capacity(source.len()),
        };

        for vs in source {
            let mut inner = Vec::with_capacity(vs.len());
            for v in vs {
                let value: Value = Value::DateTime(v.timestamp() as u32, v.timezone());
                inner.push(value);
            }
            data.push(Value::Array(sql_type.clone().into(), Arc::new(inner)));
        }

        W::wrap(data)
    }
}

impl ColumnFrom for Vec<Option<Date<Tz>>> {
    fn column_from<W: ColumnWrapper>(source: Self) -> <W as ColumnWrapper>::Wrapper {
        let fake: Vec<Date<Tz>> = Vec::with_capacity(source.len());
        let inner = Vec::column_from::<ArcColumnWrapper>(fake);

        let mut data = NullableColumnData {
            inner,
            nulls: Vec::with_capacity(source.len()),
        };

        for value in source {
            match value {
                None => data.push(Value::Nullable(Either::Left(SqlType::Date.into()))),
                Some(d) => {
                    let days = u16::get_days(d);
                    let value = Value::Date(days, d.timezone());
                    data.push(Value::Nullable(Either::Right(Box::new(value))))
                }
            }
        }

        W::wrap(data)
    }
}

impl<T> ColumnData for DateColumnData<T>
where T: StatBuffer
        + Unmarshal<T>
        + Marshal
        + Copy
        + convert::Into<Value>
        + convert::From<Value>
        + fmt::Display
        + Sync
        + Send
        + DateConverter
        + Default
        + 'static
{
    fn sql_type(&self) -> SqlType {
        T::date_type()
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        save_data::<T>(self.data.as_ref(), encoder, start, end);
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn push(&mut self, value: Value) {
        self.data.push(T::get_stamp(value));
    }

    fn at(&self, index: usize) -> ValueRef {
        self.data.at(index).to_date(self.tz)
    }

    fn clone_instance(&self) -> BoxColumnData {
        Box::new(Self {
            data: self.data.clone(),
            tz: self.tz,
        })
    }

    unsafe fn get_internal(&self, pointers: &[*mut *const u8], level: u8) -> Result<()> {
        assert_eq!(level, 0);
        *pointers[0] = self.data.as_ptr() as *const u8;
        *pointers[1] = &self.tz as *const Tz as *const u8;
        *(pointers[2] as *mut usize) = self.len();
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use chrono::TimeZone;
    use chrono_tz::Tz;

    use super::*;
    use crate::types::column::ArcColumnWrapper;

    #[test]
    fn test_create_date() {
        let tz = Tz::Zulu;
        let column = Vec::column_from::<ArcColumnWrapper>(vec![tz.ymd(2016, 10, 22)]);
        assert_eq!("2016-10-22UTC", format!("{:#}", column.at(0)));
        assert_eq!(SqlType::Date, column.sql_type());
    }

    #[test]
    fn test_create_date_time() {
        let tz = Tz::Zulu;
        let column =
            Vec::column_from::<ArcColumnWrapper>(vec![tz.ymd(2016, 10, 22).and_hms(12, 0, 0)]);
        assert_eq!(format!("{}", column.at(0)), "2016-10-22 12:00:00");
    }
}
