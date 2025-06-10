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

use std::any::Any;
use std::collections::HashSet;
use std::fmt::Debug;
use std::ops::Range;

use arrow_array::ArrayRef;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::schema::DataSchema;
use crate::types::AnyType;
use crate::types::ArgType;
use crate::types::DataType;
use crate::Column;
use crate::ColumnBuilder;
use crate::ColumnSet;
use crate::DataField;
use crate::DataSchemaRef;
use crate::Domain;
use crate::Scalar;
use crate::ScalarRef;
use crate::TableSchemaRef;
use crate::Value;

pub type SendableDataBlockStream =
    std::pin::Pin<Box<dyn futures::stream::Stream<Item = Result<DataBlock>> + Send>>;
pub type BlockMetaInfoPtr = Box<dyn BlockMetaInfo>;

/// DataBlock is a lightweight container for a group of columns.
#[derive(Clone)]
pub struct DataBlock {
    entries: Vec<BlockEntry>,
    num_rows: usize,
    meta: Option<BlockMetaInfoPtr>,
}

#[derive(Clone, PartialEq)]
pub struct BlockEntry(InnerEntry);

#[derive(Clone, PartialEq)]
enum InnerEntry {
    Const(Scalar, DataType, Option<usize>),
    Column(Column),
}

impl Debug for BlockEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockEntry")
            .field("data_type", &self.data_type())
            .field("value", &self.value())
            .finish()
    }
}

impl BlockEntry {
    pub fn new(data_type: DataType, value: Value<AnyType>) -> Self {
        #[cfg(debug_assertions)]
        {
            match &value {
                Value::Column(c) => {
                    c.check_valid().unwrap();
                }
                Value::Scalar(_) => {
                    check_type(&data_type, &value);
                }
            }
        }

        match value {
            Value::Column(c) => Self(InnerEntry::Column(c)),
            Value::Scalar(s) => Self(InnerEntry::Const(s, data_type, None)),
        }
    }

    pub fn from_arg_value<T: ArgType>(value: Value<T>) -> Self {
        Self::new(T::data_type(), value.upcast())
    }

    pub fn from_arg_scalar<T: ArgType>(scalar: T::Scalar) -> Self {
        Self::new(T::data_type(), Value::Scalar(T::upcast_scalar(scalar)))
    }

    pub fn remove_nullable(self) -> Self {
        match self.0 {
            InnerEntry::Column(Column::Nullable(col)) => col.column_ref().clone().into(),
            _ => self,
        }
    }

    pub fn memory_size(&self) -> usize {
        match &self.0 {
            InnerEntry::Const(scalar, _, _) => scalar.as_ref().memory_size(),
            InnerEntry::Column(column) => column.memory_size(),
        }
    }

    pub fn to_column(&self, num_rows: usize) -> Column {
        match &self.0 {
            InnerEntry::Const(scalar, data_type, n) => {
                debug_assert_eq!(num_rows, n.unwrap_or(num_rows));
                Value::<AnyType>::Scalar(scalar.clone()).convert_to_full_column(data_type, num_rows)
            }
            InnerEntry::Column(column) => {
                debug_assert_eq!(num_rows, column.len());
                column.clone()
            }
        }
    }

    pub fn data_type(&self) -> DataType {
        match &self.0 {
            InnerEntry::Const(_, data_type, _) => data_type.clone(),
            InnerEntry::Column(column) => column.data_type(),
        }
    }

    pub fn as_column(&self) -> Option<&Column> {
        match &self.0 {
            InnerEntry::Const(_, _, _) => None,
            InnerEntry::Column(column) => Some(column),
        }
    }

    pub fn as_scalar(&self) -> Option<&Scalar> {
        match &self.0 {
            InnerEntry::Const(scalar, _, _) => Some(scalar),
            InnerEntry::Column(_) => None,
        }
    }

    pub fn into_column(self) -> std::result::Result<Column, Self> {
        match self.0 {
            InnerEntry::Column(column) => Ok(column),
            _ => Err(self),
        }
    }

    pub fn value(&self) -> Value<AnyType> {
        match &self.0 {
            InnerEntry::Const(scalar, _, _) => Value::Scalar(scalar.clone()),
            InnerEntry::Column(column) => Value::Column(column.clone()),
        }
    }

    pub fn index(&self, index: usize) -> Option<ScalarRef<'_>> {
        match &self.0 {
            InnerEntry::Const(scalar, _, Some(n)) if index < *n => Some(scalar.as_ref()),
            InnerEntry::Column(column) => column.index(index),
            _ => unreachable!(),
        }
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    pub unsafe fn index_unchecked(&self, index: usize) -> ScalarRef {
        match &self.0 {
            InnerEntry::Const(scalar, _, n) => {
                #[cfg(debug_assertions)]
                match *n {
                    Some(n) if index < n => (),
                    _ => panic!(
                        "index out of bounds: the len is {:?} but the index is {}",
                        n, index
                    ),
                }

                scalar.as_ref()
            }
            InnerEntry::Column(column) => column.index_unchecked(index),
        }
    }
}

impl From<Column> for BlockEntry {
    fn from(col: Column) -> Self {
        #[cfg(debug_assertions)]
        col.check_valid().unwrap();

        Self(InnerEntry::Column(col))
    }
}

#[typetag::serde(tag = "type")]
pub trait BlockMetaInfo: Debug + Send + Sync + Any + 'static {
    #[allow(clippy::borrowed_box)]
    fn equals(&self, _info: &Box<dyn BlockMetaInfo>) -> bool {
        panic!(
            "The reason for not implementing equals is usually because the higher-level logic doesn't allow/need the meta to be compared."
        )
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        panic!(
            "The reason for not implementing clone_self is usually because the higher-level logic doesn't allow/need the associated block to be cloned."
        )
    }
}

pub trait BlockMetaInfoDowncast: Sized + BlockMetaInfo {
    fn boxed(self) -> BlockMetaInfoPtr {
        Box::new(self)
    }

    fn downcast_from(boxed: BlockMetaInfoPtr) -> Option<Self> {
        let boxed: Box<dyn Any> = boxed;
        boxed.downcast().ok().map(|x| *x)
    }

    fn downcast_ref_from(boxed: &BlockMetaInfoPtr) -> Option<&Self> {
        let boxed = boxed.as_ref() as &dyn Any;
        boxed.downcast_ref()
    }
}

impl<T: BlockMetaInfo> BlockMetaInfoDowncast for T {}

impl DataBlock {
    #[inline]
    pub fn new(columns: Vec<BlockEntry>, num_rows: usize) -> Self {
        DataBlock::new_with_meta(columns, num_rows, None)
    }

    #[inline]
    pub fn new_with_meta(
        mut entries: Vec<BlockEntry>,
        num_rows: usize,
        meta: Option<BlockMetaInfoPtr>,
    ) -> Self {
        for entry in entries.iter_mut() {
            match &mut entry.0 {
                InnerEntry::Const(_, _, n) if n.is_none() => *n = Some(num_rows),
                _ => (),
            }
        }
        Self::check_columns_valid(&entries, num_rows).unwrap();

        Self {
            entries,
            num_rows,
            meta,
        }
    }

    fn check_columns_valid(columns: &[BlockEntry], num_rows: usize) -> Result<()> {
        for entry in columns.iter() {
            match &entry.0 {
                InnerEntry::Const(_, _, n) => {
                    if *n != Some(num_rows) {
                        return Err(ErrorCode::Internal(format!(
                            "DataBlock corrupted, const column length mismatch, const rows: {n:?}, num_rows: {num_rows}",
                        )));
                    }
                }
                InnerEntry::Column(c) => {
                    #[cfg(debug_assertions)]
                    c.check_valid()?;
                    if c.len() != num_rows {
                        return Err(ErrorCode::Internal(format!(
                        "DataBlock corrupted, column length mismatch, col rows: {}, num_rows: {num_rows}, datatype: {}",
                        c.len(),
                        c.data_type()
                    )));
                    }
                }
            }
        }
        Ok(())
    }

    pub fn check_valid(&self) -> Result<()> {
        Self::check_columns_valid(&self.entries, self.num_rows)
    }

    #[inline]
    pub fn new_from_columns(columns: Vec<Column>) -> Self {
        assert!(!columns.is_empty());
        let num_rows = columns[0].len();
        debug_assert!(columns.iter().all(|c| c.len() == num_rows));

        let columns = columns.into_iter().map(|col| col.into()).collect();
        DataBlock::new(columns, num_rows)
    }

    #[inline]
    pub fn empty() -> Self {
        DataBlock::new(vec![], 0)
    }

    #[inline]
    pub fn empty_with_rows(rows: usize) -> Self {
        DataBlock::new(vec![], rows)
    }

    #[inline]
    pub fn empty_with_schema(schema: DataSchemaRef) -> Self {
        let columns = schema
            .fields()
            .iter()
            .map(|f| {
                let builder = ColumnBuilder::with_capacity(f.data_type(), 0);
                builder.build().into()
            })
            .collect();
        DataBlock::new(columns, 0)
    }

    #[inline]
    pub fn empty_with_meta(meta: BlockMetaInfoPtr) -> Self {
        DataBlock::new_with_meta(vec![], 0, Some(meta))
    }

    #[inline]
    pub fn take_meta(&mut self) -> Option<BlockMetaInfoPtr> {
        self.meta.take()
    }

    #[inline]
    pub fn columns(&self) -> &[BlockEntry] {
        &self.entries
    }

    #[inline]
    pub fn columns_mut(&mut self) -> &mut [BlockEntry] {
        &mut self.entries
    }

    #[inline]
    pub fn take_columns(self) -> Vec<BlockEntry> {
        self.entries
    }

    #[inline]
    pub fn get_by_offset(&self, offset: usize) -> &BlockEntry {
        &self.entries[offset]
    }

    #[inline]
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    #[inline]
    pub fn num_columns(&self) -> usize {
        self.entries.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.num_rows() == 0
    }

    // Full empty means no row, no column, no meta
    #[inline]
    pub fn is_full_empty(&self) -> bool {
        self.is_empty() && self.meta.is_none() && self.entries.is_empty()
    }

    #[inline]
    pub fn domains(&self) -> Vec<Domain> {
        self.entries
            .iter()
            .map(|entry| match &entry.0 {
                InnerEntry::Const(scalar, data_type, _) => scalar.as_ref().domain(data_type),
                InnerEntry::Column(column) => column.domain(),
            })
            .collect()
    }

    #[inline]
    pub fn memory_size(&self) -> usize {
        self.columns().iter().map(|entry| entry.memory_size()).sum()
    }

    pub fn data_type(&self, offset: usize) -> DataType {
        self.entries[offset].data_type()
    }

    pub fn consume_convert_to_full(self) -> Self {
        if self
            .columns()
            .iter()
            .all(|entry| matches!(entry.0, InnerEntry::Column(_)))
        {
            return self;
        }

        self.convert_to_full()
    }

    pub fn convert_to_full(&self) -> Self {
        let entries = self
            .entries
            .iter()
            .map(|entry| match &entry.0 {
                InnerEntry::Const(s, data_type, _) => {
                    let builder = ColumnBuilder::repeat(&s.as_ref(), self.num_rows, data_type);
                    builder.build().into()
                }
                InnerEntry::Column(c) => c.clone().into(),
            })
            .collect();
        Self {
            entries,
            num_rows: self.num_rows,
            meta: self.meta.clone(),
        }
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        assert!(
            range.end <= self.num_rows(),
            "range {:?} out of len {}",
            range,
            self.num_rows()
        );
        let entries = self
            .entries
            .iter()
            .map(|entry| match &entry.0 {
                InnerEntry::Const(scalar, data_type, Some(_)) => BlockEntry(InnerEntry::Const(
                    scalar.clone(),
                    data_type.clone(),
                    Some(range.end - range.start),
                )),
                InnerEntry::Column(c) => c.slice(range.clone()).into(),
                _ => unreachable!(),
            })
            .collect();
        Self {
            entries,
            num_rows: if range.is_empty() {
                0
            } else {
                range.end - range.start
            },
            meta: self.meta.clone(),
        }
    }

    pub fn split_by_rows(&self, max_rows_per_block: usize) -> (Vec<Self>, Option<Self>) {
        let mut res = Vec::with_capacity(self.num_rows / max_rows_per_block);
        let mut offset = 0;
        let mut remain_rows = self.num_rows;
        while remain_rows >= max_rows_per_block {
            let cut = self.slice(offset..(offset + max_rows_per_block));
            res.push(cut);
            offset += max_rows_per_block;
            remain_rows -= max_rows_per_block;
        }
        let remain = if remain_rows > 0 {
            Some(self.slice(offset..(offset + remain_rows)))
        } else {
            None
        };
        (res, remain)
    }

    pub fn split_by_rows_no_tail(&self, max_rows_per_block: usize) -> Vec<Self> {
        let mut res = Vec::with_capacity(self.num_rows.div_ceil(max_rows_per_block));
        let mut offset = 0;
        let mut remain_rows = self.num_rows;
        while remain_rows >= max_rows_per_block {
            let cut = self.slice(offset..(offset + max_rows_per_block));
            res.push(cut);
            offset += max_rows_per_block;
            remain_rows -= max_rows_per_block;
        }
        if remain_rows > 0 {
            res.push(self.slice(offset..(offset + remain_rows)));
        }
        res
    }

    pub fn split_by_rows_if_needed_no_tail(&self, rows_per_block: usize) -> Vec<Self> {
        // Since rows_per_block represents the expected number of rows per block,
        // and the minimum number of rows per block is 0.8 * rows_per_block,
        // the maximum is taken as 1.8 * rows_per_block.
        let max_rows_per_block = (rows_per_block * 9).div_ceil(5);
        let mut res = vec![];
        let mut offset = 0;
        let mut remain_rows = self.num_rows;
        while remain_rows >= max_rows_per_block {
            let cut = self.slice(offset..(offset + rows_per_block));
            res.push(cut);
            offset += rows_per_block;
            remain_rows -= rows_per_block;
        }
        res.push(self.slice(offset..(offset + remain_rows)));
        res
    }

    #[inline]
    pub fn merge_block(&mut self, block: DataBlock) {
        self.entries.reserve(block.num_columns());
        for other in block.entries.into_iter() {
            #[cfg(debug_assertions)]
            match &other.0 {
                InnerEntry::Const(_, _, Some(n)) => assert_eq!(*n, self.num_rows),
                InnerEntry::Column(column) => assert_eq!(column.len(), self.num_rows),
                _ => unreachable!(),
            }
            self.entries.push(other);
        }
    }

    #[inline]
    pub fn add_column(&mut self, entry: BlockEntry) {
        self.entries.push(entry);
        #[cfg(debug_assertions)]
        self.check_valid().unwrap();
    }

    #[inline]
    pub fn pop_columns(&mut self, num: usize) {
        debug_assert!(num <= self.entries.len());
        self.entries.truncate(self.entries.len() - num);
    }

    /// Resort the columns according to the schema.
    #[inline]
    pub fn resort(self, src_schema: &DataSchema, dest_schema: &DataSchema) -> Result<Self> {
        let columns = dest_schema
            .fields()
            .iter()
            .map(|dest_field| {
                let src_offset = src_schema.index_of(dest_field.name()).map_err(|_| {
                    let valid_fields: Vec<String> = src_schema
                        .fields()
                        .iter()
                        .map(|f| f.name().to_string())
                        .collect();
                    ErrorCode::BadArguments(format!(
                        "Unable to get field named \"{}\". Valid fields: {:?}",
                        dest_field.name(),
                        valid_fields
                    ))
                })?;
                Ok(self.get_by_offset(src_offset).clone())
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            entries: columns,
            num_rows: self.num_rows,
            meta: self.meta,
        })
    }

    #[inline]
    pub fn add_meta(self, meta: Option<BlockMetaInfoPtr>) -> Result<Self> {
        if self.meta.is_some() {
            return Err(ErrorCode::Internal(
                "Internal error, block meta data is set twice.",
            ));
        }

        Ok(Self {
            entries: self.entries,
            num_rows: self.num_rows,
            meta,
        })
    }

    #[inline]
    pub fn replace_meta(&mut self, meta: BlockMetaInfoPtr) {
        self.meta.replace(meta);
    }

    #[inline]
    pub fn get_meta(&self) -> Option<&BlockMetaInfoPtr> {
        self.meta.as_ref()
    }

    #[inline]
    pub fn get_owned_meta(self) -> Option<BlockMetaInfoPtr> {
        self.meta
    }

    // If default_vals[i].is_some(), then DataBlock.column[i] = num_rows * default_vals[i].
    // Else, DataBlock.column[i] = self.column.
    // For example, Schema.field is [a,b,c] and default_vals is [Some("a"), None, Some("c")],
    // then the return block column will be ["a"*num_rows, chunk.column[0], "c"*num_rows].
    pub fn create_with_opt_default_value(
        arrays: Vec<ArrayRef>,
        schema: &DataSchema,
        default_vals: &[Option<Scalar>],
        num_rows: usize,
    ) -> Result<DataBlock> {
        let mut chunk_idx: usize = 0;
        let schema_fields = schema.fields();

        let mut columns = Vec::with_capacity(default_vals.len());
        for (i, default_val) in default_vals.iter().enumerate() {
            let field = &schema_fields[i];
            let data_type = field.data_type();

            let column = match default_val {
                Some(default_val) => {
                    BlockEntry::new(data_type.clone(), Value::Scalar(default_val.to_owned()))
                }
                None => {
                    let value = Value::from_arrow_rs(arrays[chunk_idx].clone(), data_type)?;
                    chunk_idx += 1;
                    BlockEntry::new(data_type.clone(), value)
                }
            };

            columns.push(column);
        }

        Ok(DataBlock::new(columns, num_rows))
    }

    pub fn create_with_default_value(
        schema: &DataSchema,
        default_vals: &[Scalar],
        num_rows: usize,
    ) -> Result<DataBlock> {
        let schema_fields = schema.fields();

        let mut columns = Vec::with_capacity(default_vals.len());
        for (i, default_val) in default_vals.iter().enumerate() {
            let field = &schema_fields[i];
            let data_type = field.data_type();

            let column = BlockEntry::new(data_type.clone(), Value::Scalar(default_val.to_owned()));
            columns.push(column);
        }

        Ok(DataBlock::new(columns, num_rows))
    }

    // If block_column_ids not contain schema.field[i].column_id,
    // then DataBlock.column[i] = num_rows * default_vals[i].
    // Else, DataBlock.column[i] = data_block.column.
    pub fn create_with_default_value_and_block(
        schema: &TableSchemaRef,
        data_block: &DataBlock,
        block_column_ids: &HashSet<u32>,
        default_vals: &[Scalar],
    ) -> Result<DataBlock> {
        let num_rows = data_block.num_rows();
        let mut data_block_columns_idx: usize = 0;
        let data_block_columns = data_block.columns();

        let schema_fields = schema.fields();
        let mut columns = Vec::with_capacity(default_vals.len());
        for (i, field) in schema_fields.iter().enumerate() {
            let column_id = field.column_id();
            let column = if !block_column_ids.contains(&column_id) {
                let default_val = &default_vals[i];
                let table_data_type = field.data_type();
                BlockEntry::new(
                    table_data_type.into(),
                    Value::Scalar(default_val.to_owned()),
                )
            } else {
                let chunk_column = &data_block_columns[data_block_columns_idx];
                data_block_columns_idx += 1;
                chunk_column.clone()
            };

            columns.push(column);
        }

        Ok(DataBlock::new(columns, num_rows))
    }

    #[inline]
    pub fn project(mut self, projections: &ColumnSet) -> Self {
        let mut entries = Vec::with_capacity(projections.len());
        for (index, column) in self.entries.into_iter().enumerate() {
            if !projections.contains(&index) {
                continue;
            }
            entries.push(column);
        }
        self.entries = entries;
        self
    }

    #[inline]
    pub fn project_with_agg_index(self, projections: &ColumnSet, num_evals: usize) -> Self {
        let mut columns = Vec::with_capacity(projections.len());
        let eval_offset = self.entries.len() - num_evals;
        for (index, column) in self.entries.into_iter().enumerate() {
            if !projections.contains(&index) && index < eval_offset {
                continue;
            }
            columns.push(column);
        }
        DataBlock::new_with_meta(columns, self.num_rows, self.meta)
    }

    #[inline]
    pub fn get_last_column(&self) -> &Column {
        debug_assert!(!self.entries.is_empty());
        self.entries.last().unwrap().as_column().unwrap()
    }

    pub fn infer_schema(&self) -> DataSchema {
        let fields = self
            .columns()
            .iter()
            .enumerate()
            .map(|(index, e)| DataField::new(&format!("col_{index}"), e.data_type()))
            .collect();
        DataSchema::new(fields)
    }

    // This is inefficient, don't use it in hot path
    pub fn value_at(&self, col: usize, row: usize) -> Option<ScalarRef<'_>> {
        self.entries.get(col)?.index(row)
    }

    /// Calculates the memory size of a `DataBlock` for writing purposes.
    /// This function is used to estimate the memory footprint of a `DataBlock` when writing it to storage.
    pub fn estimate_block_size(&self) -> usize {
        let num_rows = self.num_rows();
        self.columns()
            .iter()
            .map(|entry| match &entry.0 {
                InnerEntry::Column(Column::Nullable(col))
                    if col.validity_ref().true_count() == 0 =>
                {
                    // For `Nullable` columns with no valid values,
                    // only the size of the validity bitmap is counted.
                    col.validity.as_slice().0.len()
                }
                InnerEntry::Const(s, data_type, _) => {
                    s.as_ref().estimated_scalar_repeat_size(num_rows, data_type)
                }
                _ => entry.memory_size(),
            })
            .sum()
    }
}

impl Eq for Box<dyn BlockMetaInfo> {}

impl PartialEq for Box<dyn BlockMetaInfo> {
    fn eq(&self, other: &Self) -> bool {
        let this_any = self.as_ref() as &dyn Any;
        let other_any = other.as_ref() as &dyn Any;

        match this_any.type_id() == other_any.type_id() {
            true => self.equals(other),
            false => false,
        }
    }
}

impl Clone for Box<dyn BlockMetaInfo> {
    fn clone(&self) -> Self {
        self.clone_self()
    }
}

fn check_type(data_type: &DataType, value: &Value<AnyType>) {
    match value {
        Value::Scalar(Scalar::Null) => {
            assert!(data_type.is_nullable_or_null());
        }
        Value::Scalar(Scalar::Tuple(fields)) => {
            // Check if data_type is Tuple type.
            let data_type = data_type.remove_nullable();
            assert!(matches!(data_type, DataType::Tuple(_)));
            if let DataType::Tuple(dts) = data_type {
                for (s, dt) in fields.iter().zip(dts.iter()) {
                    check_type(dt, &Value::Scalar(s.clone()));
                }
            }
        }
        Value::Scalar(s) => assert_eq!(s.as_ref().infer_data_type(), data_type.remove_nullable()),
        Value::Column(c) => assert_eq!(&c.data_type(), data_type),
    }
}

#[macro_export]
macro_rules! local_block_meta_serde {
    ($T:ty) => {
        impl serde::Serialize for $T {
            fn serialize<S>(&self, _: S) -> std::result::Result<S::Ok, S::Error>
            where S: serde::Serializer {
                unreachable!(
                    "{} must not be exchanged between multiple nodes.",
                    stringify!($T)
                )
            }
        }

        impl<'de> serde::Deserialize<'de> for $T {
            fn deserialize<D>(_: D) -> std::result::Result<Self, D::Error>
            where D: serde::Deserializer<'de> {
                unreachable!(
                    "{} must not be exchanged between multiple nodes.",
                    stringify!($T)
                )
            }
        }
    };
}
