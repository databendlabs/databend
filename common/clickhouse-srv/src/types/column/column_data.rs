use std::convert;
use std::sync::Arc;

use crate::binary::Encoder;
use crate::errors::Error;
use crate::errors::FromSqlError;
use crate::errors::Result;
use crate::types::SqlType;
use crate::types::Value;
use crate::types::ValueRef;

pub(crate) type ArcColumnData = Arc<dyn ColumnData + Send + Sync>;

pub(crate) type BoxColumnData = Box<dyn ColumnData + Send + Sync>;

pub trait ColumnData {
    fn sql_type(&self) -> SqlType;
    fn save(&self, encoder: &mut Encoder, start: usize, end: usize);
    fn len(&self) -> usize;
    fn push(&mut self, value: Value);
    fn at(&self, index: usize) -> ValueRef;

    fn clone_instance(&self) -> BoxColumnData;

    unsafe fn get_internal(&self, _pointers: &[*mut *const u8], _level: u8) -> Result<()> {
        Err(Error::FromSql(FromSqlError::UnsupportedOperation))
    }

    fn cast_to(&self, _this: &ArcColumnData, _target: &SqlType) -> Option<ArcColumnData> {
        None
    }
}

pub(crate) trait ColumnDataExt {
    fn append<T: convert::Into<Value>>(&mut self, value: T);
}

impl<C: ColumnData> ColumnDataExt for C {
    fn append<T: convert::Into<Value>>(&mut self, value: T) {
        self.push(value.into());
    }
}
