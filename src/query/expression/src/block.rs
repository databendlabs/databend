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
use enum_as_inner::EnumAsInner;

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
use crate::schema::DataSchema;
use crate::types::AccessType;
use crate::types::AnyType;
use crate::types::ArgType;
use crate::types::BooleanType;
use crate::types::DataType;
use crate::types::ValueType;

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

#[derive(Clone, Debug, PartialEq, EnumAsInner)]
pub enum BlockEntry {
    Const(Scalar, DataType, usize),
    Column(Column),
}

impl BlockEntry {
    pub fn new<F>(value: Value<AnyType>, on_scalar: F) -> Self
    where F: FnOnce() -> (DataType, usize) {
        match value {
            Value::Column(c) => c.into(),
            Value::Scalar(s) => {
                let (data_type, num_rows) = on_scalar();
                BlockEntry::new_const_column(data_type, s, num_rows)
            }
        }
    }

    pub fn new_const_column(data_type: DataType, scalar: Scalar, num_rows: usize) -> Self {
        debug_assert!(
            scalar.as_ref().is_value_of_type(&data_type),
            "type not match: {scalar:?}, {data_type:?}"
        );
        BlockEntry::Const(scalar, data_type, num_rows)
    }

    pub fn new_const_column_arg<T: ArgType>(scalar: T::Scalar, num_rows: usize) -> Self {
        Self::new_const_column(T::data_type(), T::upcast_scalar(scalar), num_rows)
    }

    pub fn remove_nullable(self) -> Self {
        match self {
            BlockEntry::Column(Column::Nullable(col)) => {
                let (col, _) = col.destructure();
                col.into()
            }
            BlockEntry::Column(_) => self,
            BlockEntry::Const(scalar, DataType::Nullable(inner), num_rows) => {
                if scalar.is_null() {
                    BlockEntry::Const(Scalar::default_value(&inner), *inner, num_rows)
                } else {
                    BlockEntry::Const(scalar, *inner, num_rows)
                }
            }
            _ => self,
        }
    }

    pub fn split_nullable(self) -> (Self, ColumnView<BooleanType>) {
        let n = self.len();
        match self {
            BlockEntry::Column(Column::Nullable(col)) => {
                let (column, validity) = col.destructure();
                let validity = if validity.null_count() == 0 {
                    ColumnView::Const(true, n)
                } else if validity.true_count() == 0 {
                    ColumnView::Const(false, n)
                } else {
                    ColumnView::Column(validity)
                };
                (column.into(), validity)
            }
            BlockEntry::Column(_) => (self, ColumnView::Const(true, n)),
            BlockEntry::Const(scalar, DataType::Nullable(inner), _) => {
                if scalar.is_null() {
                    (
                        BlockEntry::Const(Scalar::default_value(&inner), *inner, n),
                        ColumnView::Const(false, n),
                    )
                } else {
                    (
                        BlockEntry::Const(scalar, *inner, n),
                        ColumnView::Const(true, n),
                    )
                }
            }
            _ => (self, ColumnView::Const(true, n)),
        }
    }

    pub fn memory_size(&self) -> usize {
        match self {
            BlockEntry::Const(scalar, _, _) => scalar.as_ref().memory_size(),
            BlockEntry::Column(column) => column.memory_size(false),
        }
    }

    pub fn memory_size_with_options(&self, gc: bool) -> usize {
        match self {
            BlockEntry::Const(scalar, _, _) => scalar.as_ref().memory_size(),
            BlockEntry::Column(column) => column.memory_size(gc),
        }
    }

    pub fn to_column(&self) -> Column {
        match self {
            BlockEntry::Const(scalar, data_type, num_rows) => {
                Value::<AnyType>::Scalar(scalar.clone())
                    .convert_to_full_column(data_type, *num_rows)
            }
            BlockEntry::Column(column) => column.clone(),
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            BlockEntry::Const(_, data_type, _) => data_type.clone(),
            BlockEntry::Column(column) => column.data_type(),
        }
    }

    pub fn as_scalar(&self) -> Option<&Scalar> {
        match self {
            BlockEntry::Const(scalar, _, _) => Some(scalar),
            BlockEntry::Column(_) => None,
        }
    }

    pub fn value(&self) -> Value<AnyType> {
        match self {
            BlockEntry::Const(scalar, _, _) => Value::Scalar(scalar.clone()),
            BlockEntry::Column(column) => Value::Column(column.clone()),
        }
    }

    pub fn index(&self, index: usize) -> Option<ScalarRef<'_>> {
        match self {
            BlockEntry::Const(scalar, _, n) if index < *n => Some(scalar.as_ref()),
            BlockEntry::Column(column) => column.index(index),
            _ => None,
        }
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    pub unsafe fn index_unchecked(&self, index: usize) -> ScalarRef<'_> {
        unsafe {
            match self {
                BlockEntry::Const(scalar, _, _n) => {
                    #[cfg(debug_assertions)]
                    if index >= *_n {
                        panic!(
                            "index out of bounds: the len is {:?} but the index is {}",
                            _n, index
                        )
                    }

                    scalar.as_ref()
                }
                BlockEntry::Column(column) => column.index_unchecked(index),
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            BlockEntry::Const(_, _, n) => *n,
            BlockEntry::Column(column) => column.len(),
        }
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        assert!(
            range.end <= self.len(),
            "range {range:?} out of len {}",
            self.len()
        );
        match self {
            BlockEntry::Const(scalar, data_type, _) => {
                BlockEntry::Const(scalar.clone(), data_type.clone(), range.end - range.start)
            }
            BlockEntry::Column(c) => c.slice(range).into(),
        }
    }

    pub fn downcast<T: AccessType>(&self) -> Result<ColumnView<T>> {
        match self {
            BlockEntry::Const(scalar, _, num_rows) => Ok(ColumnView::Const(
                T::to_owned_scalar(T::try_downcast_scalar(&scalar.as_ref())?),
                *num_rows,
            )),
            BlockEntry::Column(column) => Ok(ColumnView::Column(T::try_downcast_column(column)?)),
        }
    }
}

impl From<Column> for BlockEntry {
    fn from(col: Column) -> Self {
        #[cfg(debug_assertions)]
        col.check_valid().unwrap();
        BlockEntry::Column(col)
    }
}

impl TryFrom<Value<AnyType>> for BlockEntry {
    type Error = Scalar;

    fn try_from(value: Value<AnyType>) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Scalar(scalar) => Err(scalar),
            Value::Column(column) => Ok(column.into()),
        }
    }
}

#[derive(Debug, EnumAsInner, Clone)]
pub enum ColumnView<T: AccessType> {
    Const(T::Scalar, usize),
    Column(T::Column),
}

impl<T: AccessType> ColumnView<T> {
    pub fn len(&self) -> usize {
        match self {
            ColumnView::Const(_, num_rows) => *num_rows,
            ColumnView::Column(column) => T::column_len(column),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn iter(&self) -> ColumnViewIter<'_, T> {
        match self {
            ColumnView::Const(scalar, num_rows) => {
                ColumnViewIter::Const(T::to_scalar_ref(scalar), *num_rows)
            }
            ColumnView::Column(column) => ColumnViewIter::Column(T::iter_column(column)),
        }
    }

    pub fn index(&self, i: usize) -> Option<T::ScalarRef<'_>> {
        match self {
            ColumnView::Const(scalar, n) => {
                if i < *n {
                    Some(T::to_scalar_ref(scalar))
                } else {
                    None
                }
            }
            ColumnView::Column(column) => T::index_column(column, i),
        }
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    pub unsafe fn index_unchecked(&self, i: usize) -> T::ScalarRef<'_> {
        unsafe {
            debug_assert!(i < self.len());
            match self {
                ColumnView::Const(scalar, _) => T::to_scalar_ref(scalar),
                ColumnView::Column(column) => T::index_column_unchecked(column, i),
            }
        }
    }
}

impl<T: ValueType> ColumnView<T> {
    pub fn upcast(self, data_type: DataType) -> BlockEntry {
        match self {
            ColumnView::Const(scalar, num_rows) => {
                let scalar = T::upcast_scalar_with_type(scalar, &data_type);
                BlockEntry::new_const_column(data_type, scalar, num_rows)
            }
            ColumnView::Column(column) => T::upcast_column_with_type(column, &data_type).into(),
        }
    }
}

pub enum ColumnViewIter<'a, T: AccessType> {
    Column(T::ColumnIterator<'a>),
    Const(T::ScalarRef<'a>, usize),
}

impl<'a, T: AccessType> Iterator for ColumnViewIter<'a, T> {
    type Item = T::ScalarRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ColumnViewIter::Column(iter) => iter.next(),
            ColumnViewIter::Const(scalar, n) => {
                if *n == 0 {
                    None
                } else {
                    *n -= 1;
                    Some(scalar.clone())
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            ColumnViewIter::Column(iter) => iter.size_hint(),
            ColumnViewIter::Const(_, n) => (*n, Some(*n)),
        }
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

    fn override_block_schema(&self) -> Option<DataSchemaRef> {
        None
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

    fn downcast_from_err(boxed: BlockMetaInfoPtr) -> std::result::Result<Self, BlockMetaInfoPtr> {
        if (boxed.as_ref() as &dyn Any).is::<Self>() {
            Ok(*(boxed as Box<dyn Any>).downcast().unwrap())
        } else {
            Err(boxed)
        }
    }

    fn downcast_mut(boxed: &mut BlockMetaInfoPtr) -> Option<&mut Self> {
        let boxed = boxed.as_mut() as &mut dyn Any;
        boxed.downcast_mut()
    }
}

impl<T: BlockMetaInfo> BlockMetaInfoDowncast for T {}

#[typetag::serde(name = "empty")]
impl BlockMetaInfo for () {
    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        <() as BlockMetaInfoDowncast>::downcast_ref_from(info).is_some()
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(())
    }
}

impl DataBlock {
    #[inline]
    pub fn new(entries: Vec<BlockEntry>, num_rows: usize) -> Self {
        DataBlock::new_with_meta(entries, num_rows, None)
    }

    pub fn from_iter<T>(iter: T, num_rows: usize) -> Self
    where T: IntoIterator<Item = BlockEntry> {
        DataBlock::new_with_meta(iter.into_iter().collect(), num_rows, None)
    }

    #[inline]
    pub fn new_with_meta(
        entries: Vec<BlockEntry>,
        num_rows: usize,
        meta: Option<BlockMetaInfoPtr>,
    ) -> Self {
        Self::check_columns_valid(&entries, num_rows).unwrap();

        Self {
            entries,
            num_rows,
            meta,
        }
    }

    fn check_columns_valid(entries: &[BlockEntry], num_rows: usize) -> Result<()> {
        for entry in entries.iter() {
            match entry {
                BlockEntry::Const(scalar, data_type, n) => {
                    if *n != num_rows {
                        return Err(ErrorCode::Internal(format!(
                            "DataBlock corrupted, const column length mismatch, const rows: {n:?}, num_rows: {num_rows}",
                        )));
                    }
                    debug_assert!(scalar.as_ref().is_value_of_type(data_type));
                }
                BlockEntry::Column(c) => {
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

        let entries = columns.into_iter().map(|col| col.into()).collect();
        DataBlock::new(entries, num_rows)
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
            .map(|entry| match entry {
                BlockEntry::Const(scalar, data_type, _) => scalar.as_ref().domain(data_type),
                BlockEntry::Column(column) => column.domain(),
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
        if self.columns().iter().all(BlockEntry::is_column) {
            return self;
        }

        self.convert_to_full()
    }

    pub fn convert_to_full(&self) -> Self {
        let entries = self
            .entries
            .iter()
            .map(|entry| match entry {
                BlockEntry::Const(s, data_type, _) => {
                    let builder = ColumnBuilder::repeat(&s.as_ref(), self.num_rows, data_type);
                    builder.build().into()
                }
                BlockEntry::Column(c) => c.clone().into(),
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
            range.end <= self.num_rows,
            "range {:?} out of len {}",
            range,
            self.num_rows
        );
        let entries = self
            .entries
            .iter()
            .map(|entry| entry.slice(range.clone()))
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
            match &other {
                BlockEntry::Const(_, _, n) => assert_eq!(*n, self.num_rows),
                BlockEntry::Column(column) => assert_eq!(column.len(), self.num_rows),
            }
            self.entries.push(other);
        }
    }

    #[inline]
    pub fn add_entry(&mut self, entry: BlockEntry) {
        self.entries.push(entry);
        #[cfg(debug_assertions)]
        self.check_valid().unwrap();
    }

    #[inline]
    pub fn add_column(&mut self, column: Column) {
        self.add_entry(column.into());
    }

    #[inline]
    pub fn remove_column(&mut self, index: usize) {
        self.entries.remove(index);
    }

    #[inline]
    pub fn add_const_column(&mut self, scalar: Scalar, data_type: DataType) {
        self.entries.push(BlockEntry::new_const_column(
            data_type,
            scalar,
            self.num_rows,
        ));
        #[cfg(debug_assertions)]
        self.check_valid().unwrap();
    }

    #[inline]
    pub fn add_value(&mut self, value: Value<AnyType>, data_type: DataType) {
        match value {
            Value::Scalar(scalar) => self.add_const_column(scalar, data_type),
            Value::Column(column) => {
                debug_assert_eq!(data_type, column.data_type());
                self.add_column(column)
            }
        }
    }

    #[inline]
    pub fn pop_columns(&mut self, num: usize) {
        debug_assert!(num <= self.entries.len());
        self.entries.truncate(self.entries.len() - num);
    }

    /// Resort the columns according to the schema.
    #[inline]
    pub fn resort(self, src_schema: &DataSchema, dest_schema: &DataSchema) -> Result<Self> {
        let num_rows = self.num_rows;
        let columns = dest_schema
            .fields()
            .iter()
            .map(|dest_field| {
                match src_schema.index_of(dest_field.name()) {
                    Ok(src_offset) => {
                        // Column exists in source, use it
                        Ok(self.get_by_offset(src_offset).clone())
                    }
                    Err(_) => {
                        // Column not found in source - this is an error
                        let valid_fields: Vec<String> = src_schema
                            .fields()
                            .iter()
                            .map(|f| f.name().to_string())
                            .collect();
                        Err(ErrorCode::BadArguments(format!(
                            "Unable to get field named \"{}\". Valid fields: {:?}",
                            dest_field.name(),
                            valid_fields
                        )))
                    }
                }
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
    pub fn take_meta(&mut self) -> Option<BlockMetaInfoPtr> {
        self.meta.take()
    }

    #[inline]
    pub fn mut_meta(&mut self) -> Option<&mut BlockMetaInfoPtr> {
        self.meta.as_mut()
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

        let mut entries = Vec::with_capacity(default_vals.len());
        for (i, default_val) in default_vals.iter().enumerate() {
            let field = &schema_fields[i];
            let data_type = field.data_type();

            let entry = match default_val {
                Some(default_val) => BlockEntry::new_const_column(
                    data_type.clone(),
                    default_val.to_owned(),
                    num_rows,
                ),
                None => {
                    let value = Value::from_arrow_rs(arrays[chunk_idx].clone(), data_type)?;
                    chunk_idx += 1;
                    BlockEntry::new(value, || (data_type.clone(), num_rows))
                }
            };

            entries.push(entry);
        }

        Ok(DataBlock::new(entries, num_rows))
    }

    pub fn create_with_default_value(
        schema: &DataSchema,
        default_vals: &[Scalar],
        num_rows: usize,
    ) -> Result<DataBlock> {
        let schema_fields = schema.fields();

        let mut entries = Vec::with_capacity(default_vals.len());
        for (i, default_val) in default_vals.iter().enumerate() {
            let field = &schema_fields[i];
            let data_type = field.data_type();

            entries.push(BlockEntry::new_const_column(
                data_type.clone(),
                default_val.to_owned(),
                num_rows,
            ));
        }

        Ok(DataBlock::new(entries, num_rows))
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
        let mut entries = Vec::with_capacity(default_vals.len());
        for (i, field) in schema_fields.iter().enumerate() {
            let column_id = field.column_id();
            let entry = if !block_column_ids.contains(&column_id) {
                let default_val = &default_vals[i];
                let table_data_type = field.data_type();
                BlockEntry::new_const_column(
                    table_data_type.into(),
                    default_val.to_owned(),
                    num_rows,
                )
            } else {
                let chunk_column = &data_block_columns[data_block_columns_idx];
                data_block_columns_idx += 1;
                chunk_column.clone()
            };

            entries.push(entry);
        }

        Ok(DataBlock::new(entries, num_rows))
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
            .map(|entry| match entry {
                BlockEntry::Column(Column::Nullable(col)) if col.validity().true_count() == 0 => {
                    // For `Nullable` columns with no valid values,
                    // only the size of the validity bitmap is counted.
                    col.validity.as_slice().0.len()
                }
                BlockEntry::Const(s, data_type, _) => {
                    s.as_ref().estimated_scalar_repeat_size(num_rows, data_type)
                }
                _ => entry.memory_size(),
            })
            .sum()
    }

    pub fn maybe_gc(self) -> DataBlock {
        let mut columns = Vec::with_capacity(self.entries.len());

        for entry in self.entries {
            columns.push(match entry {
                BlockEntry::Column(column) => BlockEntry::Column(column.maybe_gc()),
                BlockEntry::Const(s, d, n) => BlockEntry::Const(s, d, n),
            });
        }

        DataBlock::new(columns, self.num_rows)
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
