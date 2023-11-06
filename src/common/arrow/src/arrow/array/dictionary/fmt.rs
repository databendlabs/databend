use std::fmt::Debug;
use std::fmt::Formatter;
use std::fmt::Result;
use std::fmt::Write;

use super::super::fmt::get_display;
use super::super::fmt::write_vec;
use super::DictionaryArray;
use super::DictionaryKey;
use crate::arrow::array::Array;

pub fn write_value<K: DictionaryKey, W: Write>(
    array: &DictionaryArray<K>,
    index: usize,
    null: &'static str,
    f: &mut W,
) -> Result {
    let keys = array.keys();
    let values = array.values();

    if keys.is_valid(index) {
        let key = array.key_value(index);
        get_display(values.as_ref(), null)(f, key)
    } else {
        write!(f, "{null}")
    }
}

impl<K: DictionaryKey> Debug for DictionaryArray<K> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        let writer = |f: &mut Formatter, index| write_value(self, index, "None", f);

        write!(f, "DictionaryArray")?;
        write_vec(f, writer, self.validity(), self.len(), "None", false)
    }
}
