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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::hash::Hasher;
use std::ops::ControlFlow;
use std::ops::Deref;
use std::sync::Arc;

use bytes::Bytes;
use databend_common_ast::Span;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::converts::datavalues::scalar_to_datavalue;
use databend_common_expression::eval_function;
use databend_common_expression::expr::*;
use databend_common_expression::generate_like_pattern;
use databend_common_expression::types::boolean::BooleanDomain;
use databend_common_expression::types::nullable::NullableDomain;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::Buffer;
use databend_common_expression::types::DataType;
use databend_common_expression::types::MapType;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::Number;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::ValueType;
use databend_common_expression::visit_expr;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ColumnId;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataBlock;
use databend_common_expression::Domain;
use databend_common_expression::Expr;
use databend_common_expression::ExprVisitor;
use databend_common_expression::FieldIndex;
use databend_common_expression::FunctionContext;
use databend_common_expression::LikePattern;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::Value;
use databend_common_functions::scalars::CityHasher64;
use databend_common_functions::scalars::DFHash;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_storages_common_table_meta::meta::SingleColumnMeta;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::meta::Versioned;
use jsonb::RawJsonb;
use parquet::format::FileMetaData;
use serde::Deserialize;
use serde::Serialize;

use super::eliminate_cast::is_injective_cast;
use crate::eliminate_cast::cast_const;
use crate::filters::BlockBloomFilterIndexVersion;
use crate::filters::BlockFilter;
use crate::filters::BloomBuilder;
use crate::filters::BloomFilter;
use crate::filters::Filter;
use crate::filters::FilterBuilder;
use crate::filters::FilterImpl;
use crate::filters::FilterImplBuilder;
use crate::filters::V2BloomBlock;
use crate::filters::Xor8Builder;
use crate::filters::Xor8Filter;
use crate::statistics_to_domain;
use crate::Index;

#[derive(Clone, Serialize, Deserialize)]
pub struct BloomIndexMeta {
    pub columns: Vec<(String, SingleColumnMeta)>,
}

impl TryFrom<&BloomIndexMeta> for Vec<u8> {
    type Error = ErrorCode;
    fn try_from(value: &BloomIndexMeta) -> std::result::Result<Self, Self::Error> {
        bincode::serde::encode_to_vec(value, bincode::config::standard()).map_err(|e| {
            ErrorCode::StorageOther(format!("failed to encode bloom index meta {:?}", e))
        })
    }
}

impl TryFrom<Bytes> for BloomIndexMeta {
    type Error = ErrorCode;

    fn try_from(value: Bytes) -> std::result::Result<Self, Self::Error> {
        bincode::serde::decode_from_slice(value.as_ref(), bincode::config::standard())
            .map(|(v, len)| {
                // TODO return error?
                assert_eq!(len, value.len());
                v
            })
            .map_err(|e| {
                ErrorCode::StorageOther(format!("failed to decode bloom index meta {:?}", e))
            })
    }
}

impl TryFrom<FileMetaData> for BloomIndexMeta {
    type Error = ErrorCode;

    fn try_from(mut meta: FileMetaData) -> std::result::Result<Self, Self::Error> {
        let rg = meta.row_groups.remove(0);
        let mut col_metas = Vec::with_capacity(rg.columns.len());
        for x in &rg.columns {
            match &x.meta_data {
                Some(chunk_meta) => {
                    let col_start =
                        if let Some(dict_page_offset) = chunk_meta.dictionary_page_offset {
                            dict_page_offset
                        } else {
                            chunk_meta.data_page_offset
                        };
                    let col_len = chunk_meta.total_compressed_size;
                    assert!(
                        col_start >= 0 && col_len >= 0,
                        "column start and length should not be negative"
                    );
                    let num_values = chunk_meta.num_values as u64;
                    let res = SingleColumnMeta {
                        offset: col_start as u64,
                        len: col_len as u64,
                        num_values,
                    };
                    let column_name = chunk_meta.path_in_schema[0].to_owned();
                    col_metas.push((column_name, res));
                }
                None => {
                    panic!(
                        "expecting chunk meta data while converting ThriftFileMetaData to BloomIndexMeta"
                    )
                }
            }
        }
        col_metas.shrink_to_fit();
        Ok(Self { columns: col_metas })
    }
}

/// BlockFilter represents multiple per-column filters(bloom filter or xor filter etc) for data block.
///
/// By default we create a filter per column for a parquet data file. For columns whose data_type
/// are not applicable for a filter, we skip the creation.
/// That is to say, it is legal to have a BlockFilter with zero columns.
///
/// For example, for the source data block as follows:
/// ```
///         +---name--+--age--+
///         | "Alice" |  20   |
///         | "Bob"   |  30   |
///         +---------+-------+
/// ```
/// We will create table of filters as follows:
/// ```
///         +---Bloom(name)--+--Bloom(age)--+
///         |  123456789abcd |  ac2345bcd   |
///         +----------------+--------------+
/// ```
pub struct BloomIndex {
    pub func_ctx: FunctionContext,

    /// The schema of the filter block.
    ///
    /// It is a sub set of the source table's schema.
    pub filter_schema: TableSchemaRef,

    pub version: u64,

    ///  filters.
    pub filters: Vec<Arc<FilterImpl>>,

    /// Approximate distinct count of columns generated by xor hash function.
    pub column_distinct_count: HashMap<ColumnId, usize>,
}

/// FilterExprEvalResult represents the evaluation result of an expression by a filter.
///
/// For example, expression of 'age = 12' should return false is the filter are sure
/// of the nonexistent of value '12' in column 'age'. Otherwise should return 'Uncertain'.
///
/// If the column is not applicable for a filter, like TypeID::struct, Uncertain is used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterEvalResult {
    MustFalse,
    Uncertain,
}

pub struct BloomIndexResult {
    pub bloom_fields: Vec<TableField>,
    pub bloom_scalars: Vec<(usize, Scalar, DataType)>,
    pub ngram_fields: Vec<TableField>,
    pub ngram_scalars: Vec<(usize, Scalar)>,
}

impl BloomIndex {
    /// Load a filter directly from the source table's schema and the corresponding filter parquet file.
    #[fastrace::trace]
    pub fn from_filter_block(
        func_ctx: FunctionContext,
        filter_schema: TableSchemaRef,
        filters: Vec<Arc<FilterImpl>>,
        version: u64,
    ) -> Result<Self> {
        Ok(Self {
            version,
            func_ctx,
            filter_schema,
            filters,
            column_distinct_count: HashMap::new(),
        })
    }

    pub fn serialize_to_data_block(&self) -> Result<DataBlock> {
        let fields = self.filter_schema.fields();
        let mut filter_columns = Vec::with_capacity(fields.len());
        for filter in &self.filters {
            let serialized_bytes = filter.to_bytes()?;
            let filter_value = Value::Scalar(Scalar::Binary(serialized_bytes));
            filter_columns.push(BlockEntry::new(DataType::Binary, filter_value));
        }
        Ok(DataBlock::new(filter_columns, 1))
    }

    /// Apply the predicate expression, return the result.
    /// If we are sure of skipping the scan, return false, e.g. the expression must be false.
    /// This happens when the data doesn't show up in the filter.
    ///
    /// Otherwise return `Uncertain`.
    #[fastrace::trace]
    pub fn apply(
        &self,
        expr: Expr<String>,
        scalar_map: &HashMap<Scalar, u64>,
        ngram_args: &[NgramArgs],
        column_stats: &StatisticsOfColumns,
        data_schema: TableSchemaRef,
    ) -> Result<FilterEvalResult> {
        let (expr, domains) =
            self.rewrite_expr(expr, scalar_map, ngram_args, column_stats, data_schema)?;
        match ConstantFolder::fold_with_domain(&expr, &domains, &self.func_ctx, &BUILTIN_FUNCTIONS)
            .0
        {
            Expr::Constant(Constant {
                scalar: Scalar::Boolean(false),
                ..
            }) => Ok(FilterEvalResult::MustFalse),
            _ => Ok(FilterEvalResult::Uncertain),
        }
    }

    pub fn rewrite_expr(
        &self,
        expr: Expr<String>,
        scalar_map: &HashMap<Scalar, u64>,
        ngram_args: &[NgramArgs],
        column_stats: &StatisticsOfColumns,
        data_schema: TableSchemaRef,
    ) -> Result<(Expr<String>, HashMap<String, Domain>)> {
        let mut domains = expr
            .column_refs()
            .into_iter()
            .map(|(id, ty)| {
                match ty.remove_nullable() {
                    DataType::Binary
                    | DataType::String
                    | DataType::Number(_)
                    | DataType::Decimal(_)
                    | DataType::Timestamp
                    | DataType::Date => (),
                    _ => {
                        let domain = Domain::full(&ty);
                        return (id, domain);
                    }
                };
                let domain = data_schema
                    .column_id_of(&id)
                    .ok()
                    .and_then(|col_id| {
                        column_stats
                            .get(&col_id)
                            .map(|stat| statistics_to_domain(vec![stat], &ty))
                    })
                    .unwrap_or_else(|| Domain::full(&ty));
                (id, domain)
            })
            .collect::<HashMap<_, _>>();

        let visitor = RewriteVisitor {
            new_col_id: 1,
            index: self,
            data_schema,
            scalar_map,
            ngram_args,
            column_stats,
            domains: &mut domains,
        };
        let expr = visit_expr(&expr, &mut Visitor(visitor))?.unwrap_or(expr);
        Ok((expr, domains))
    }

    /// calculate digest for column
    pub fn calculate_column_digest(
        func_ctx: &FunctionContext,
        column: &Column,
        data_type: &DataType,
        target_type: &DataType,
    ) -> Result<Column> {
        let (value, _) = eval_function(
            None,
            "siphash",
            [(Value::Column(column.clone()), data_type.clone())],
            func_ctx,
            column.len(),
            &BUILTIN_FUNCTIONS,
        )?;
        let column = value.convert_to_full_column(target_type, column.len());
        Ok(column)
    }

    /// calculate digest for column that may have null values
    ///
    /// returns (column, validity) where column is the digest of the column
    /// and validity is the optional bitmap of the null values
    pub fn calculate_nullable_column_digest(
        func_ctx: &FunctionContext,
        column: &Column,
        data_type: &DataType,
    ) -> Result<(Buffer<u64>, Option<Bitmap>)> {
        Ok(if data_type.is_nullable() {
            let col = Self::calculate_column_digest(
                func_ctx,
                column,
                data_type,
                &DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt64))),
            )?;
            let nullable_column = NullableType::<UInt64Type>::try_downcast_column(&col).unwrap();
            (nullable_column.column, Some(nullable_column.validity))
        } else {
            let col = Self::calculate_column_digest(
                func_ctx,
                column,
                data_type,
                &DataType::Number(NumberDataType::UInt64),
            )?;
            let column = UInt64Type::try_downcast_column(&col).unwrap();
            (column, None)
        })
    }

    pub fn calculate_ngram_nullable_column<'a, F, T>(
        arg: Value<AnyType>,
        gram_size: usize,
        fn_call: F,
    ) -> impl Iterator<Item = Vec<T>> + 'a
    where
        F: Fn(&str) -> T + 'a,
    {
        (0..arg.len()).filter_map(move |i| {
            arg.index(i).and_then(|scalar| {
                scalar.as_string().and_then(|text| {
                    let text = text.to_lowercase();
                    if text.is_empty() {
                        return None;
                    }

                    let indices: Vec<_> = text.char_indices().map(|(i, _)| i).collect();
                    let char_count = indices.len();

                    if gram_size > char_count {
                        return Some(vec![fn_call(&text)]);
                    }

                    let times = char_count - gram_size + 1;
                    let mut words = Vec::with_capacity(times);
                    for j in 0..times {
                        let start = indices[j];
                        let end = if j + gram_size < char_count {
                            indices[j + gram_size]
                        } else {
                            text.len()
                        };
                        words.push(fn_call(&text[start..end]));
                    }
                    Some(words)
                })
            })
        })
    }

    pub fn ngram_hash(s: &str) -> u64 {
        let mut hasher = CityHasher64::with_seed(1575457558);
        DFHash::hash(s, &mut hasher);
        hasher.finish()
    }

    /// calculate digest for constant scalar
    pub fn calculate_scalar_digest(
        func_ctx: &FunctionContext,
        scalar: &Scalar,
        data_type: &DataType,
    ) -> Result<u64> {
        let (value, _) = eval_function(
            None,
            "siphash",
            [(Value::Scalar(scalar.clone()), data_type.clone())],
            func_ctx,
            1,
            &BUILTIN_FUNCTIONS,
        )?;
        let number_scalar = value.into_scalar().unwrap().into_number().unwrap();
        let digest = u64::try_downcast_scalar(&number_scalar).unwrap();
        Ok(digest)
    }

    /// Find all columns that can be use for index in the expression.
    #[expect(clippy::type_complexity)]
    pub fn filter_index_field(
        expr: &Expr<String>,
        bloom_fields: Vec<TableField>,
        ngram_fields: Vec<TableField>,
    ) -> Result<BloomIndexResult> {
        let mut visitor = Visitor(ShortListVisitor {
            bloom_fields,
            ngram_fields,
            bloom_founds: Vec::new(),
            ngram_founds: Vec::new(),
            bloom_scalars: Vec::new(),
            ngram_scalars: Vec::new(),
        });
        visit_expr(expr, &mut visitor)?;
        let Visitor(ShortListVisitor {
            bloom_founds,
            ngram_founds,
            bloom_scalars,
            ngram_scalars,
            ..
        }) = visitor;
        Ok(BloomIndexResult {
            bloom_fields: bloom_founds,
            bloom_scalars,
            ngram_fields: ngram_founds,
            ngram_scalars,
        })
    }

    /// For every applicable column, we will create a filter.
    /// The filter will be stored with field name 'Bloom(column_name)'
    pub fn build_filter_bloom_name(version: u64, field: &TableField) -> Result<String> {
        let index_version = BlockBloomFilterIndexVersion::try_from(version)?;
        match index_version {
            BlockBloomFilterIndexVersion::V0(_) => Err(ErrorCode::DeprecatedIndexFormat(
                "bloom filter index version(v0) is deprecated",
            )),
            BlockBloomFilterIndexVersion::V2(_) | BlockBloomFilterIndexVersion::V3(_) => {
                Ok(format!("Bloom({})", field.name()))
            }
            BlockBloomFilterIndexVersion::V4(_) => Ok(format!("Bloom({})", field.column_id())),
        }
    }

    pub fn build_filter_ngram_name(field: &TableField) -> String {
        format!("Ngram({})", field.column_id())
    }

    fn find(
        &self,
        filter_column: &str,
        target: &Scalar,
        ty: &DataType,
        scalar_map: &HashMap<Scalar, u64>,
        ngram_args: &[NgramArgs],
        is_ngram: bool,
    ) -> Result<FilterEvalResult> {
        if !self.filter_schema.has_field(filter_column)
            || !Xor8Filter::supported_type(ty)
            || target.is_null()
        {
            // The column doesn't have a filter.
            return Ok(FilterEvalResult::Uncertain);
        }

        let idx = self.filter_schema.index_of(filter_column)?;
        let filter = &self.filters[idx];

        let contains = if self.version == V2BloomBlock::VERSION {
            let data_value = scalar_to_datavalue(target);
            filter.contains(&data_value)
        } else if is_ngram && !ngram_args.is_empty() {
            // NgramFilter is always placed after BloomFilter
            let offset = self.filters.len() - ngram_args.len();
            let arg = &ngram_args[idx - offset];
            let Some(words) = BloomIndex::calculate_ngram_nullable_column(
                Value::Scalar(target.clone()),
                arg.gram_size,
                |text| text.to_string(),
            )
            .next() else {
                return Ok(FilterEvalResult::Uncertain);
            };
            !words.into_iter().any(|word| {
                scalar_map
                    .get(&Scalar::String(word))
                    .is_some_and(|digest| !filter.contains_digest(*digest))
            })
        } else {
            scalar_map
                .get(target)
                .is_none_or(|digest| filter.contains_digest(*digest))
        };

        if contains {
            Ok(FilterEvalResult::Uncertain)
        } else {
            Ok(FilterEvalResult::MustFalse)
        }
    }

    pub fn supported_type(data_type: &TableDataType) -> bool {
        let data_type = DataType::from(data_type);
        Xor8Filter::supported_type(&data_type)
    }

    /// Checks if the average length of a string column exceeds 256 bytes.
    /// If it does, the bloom index for the column will not be established.
    pub fn check_large_string(column: &Column) -> bool {
        if let Column::String(v) = &column {
            let bytes_per_row = v.total_bytes_len() / v.len().max(1);
            if bytes_per_row > 256 {
                return true;
            }
        }
        false
    }
}

pub struct BloomIndexBuilder {
    func_ctx: FunctionContext,
    columns: Vec<ColumnFilterBuilder>,
}

struct ColumnFilterBuilder {
    index: FieldIndex,
    field: TableField,
    is_ngram: bool,
    gram_size: usize,
    builder: FilterImplBuilder,
}

#[derive(Clone)]
pub struct NgramArgs {
    index: FieldIndex,
    field: TableField,
    gram_size: usize,
    bitmap_size: usize,
}

impl NgramArgs {
    pub fn new(index: FieldIndex, field: TableField, gram_size: usize, bitmap_size: usize) -> Self {
        Self {
            index,
            field,
            gram_size,
            bitmap_size,
        }
    }

    pub fn field(&self) -> &TableField {
        &self.field
    }

    pub fn gram_size(&self) -> usize {
        self.gram_size
    }
}

impl BloomIndexBuilder {
    pub fn create(
        func_ctx: FunctionContext,
        bloom_columns_map: BTreeMap<FieldIndex, TableField>,
        ngram_args: &[NgramArgs],
    ) -> Result<Self> {
        let mut bloom_columns = Vec::with_capacity(bloom_columns_map.len() + ngram_args.len());
        for (&index, field) in bloom_columns_map.iter() {
            bloom_columns.push(ColumnFilterBuilder {
                index,
                field: field.clone(),
                is_ngram: false,
                gram_size: 0,
                builder: FilterImplBuilder::Xor(Xor8Builder::create()),
            });
        }
        for arg in ngram_args.iter() {
            bloom_columns.push(ColumnFilterBuilder {
                index: arg.index,
                field: arg.field.clone(),
                is_ngram: true,
                gram_size: arg.gram_size,
                builder: FilterImplBuilder::Ngram(BloomBuilder::create(arg.bitmap_size)?),
            });
        }

        Ok(Self {
            func_ctx,
            columns: bloom_columns,
        })
    }
}

impl BloomIndexBuilder {
    pub fn add_block(&mut self, block: &DataBlock) -> Result<()> {
        if block.is_empty() {
            return Err(ErrorCode::BadArguments("block is empty"));
        }
        if block.num_columns() == 0 {
            return Ok(());
        }

        let mut keys_to_remove = Vec::with_capacity(self.columns.len());

        let (bloom_iter, ngram_iter): (Vec<_>, Vec<_>) = self
            .columns
            .iter_mut()
            .enumerate()
            .partition(|(_, column)| !column.is_ngram);

        for (index, index_column) in bloom_iter {
            let field_type = &block.get_by_offset(index_column.index).data_type;
            if !Xor8Filter::supported_type(field_type) {
                keys_to_remove.push(index);
                continue;
            }

            let column = match &block.get_by_offset(index_column.index).value {
                Value::Scalar(s) => {
                    let builder = ColumnBuilder::repeat(&s.as_ref(), 1, field_type);
                    builder.build()
                }
                Value::Column(c) => c.clone(),
            };

            let (column, data_type) = match field_type.remove_nullable() {
                DataType::Map(box inner_ty) => {
                    // Add bloom filter for the value of map type
                    let map_column = if field_type.is_nullable() {
                        let nullable_column =
                            NullableType::<MapType<AnyType, AnyType>>::try_downcast_column(&column)
                                .unwrap();
                        nullable_column.column
                    } else {
                        MapType::<AnyType, AnyType>::try_downcast_column(&column).unwrap()
                    };
                    let column = map_column.underlying_column().values;

                    let DataType::Tuple(kv_tys) = inner_ty else {
                        unreachable!();
                    };
                    let val_type = kv_tys[1].clone();
                    // Extract JSON value of string type to create bloom index,
                    // other types of JSON value will be ignored.
                    if val_type.remove_nullable() == DataType::Variant {
                        let mut builder = ColumnBuilder::with_capacity(
                            &DataType::Nullable(Box::new(DataType::String)),
                            column.len(),
                        );
                        for val in column.iter() {
                            if let ScalarRef::Variant(v) = val {
                                let raw_jsonb = RawJsonb::new(v);
                                if let Ok(Some(str_val)) = raw_jsonb.as_str() {
                                    builder.push(ScalarRef::String(&str_val));
                                    continue;
                                }
                            }
                            builder.push_default();
                        }
                        let str_column = builder.build();
                        if BloomIndex::check_large_string(&str_column) {
                            keys_to_remove.push(index);
                            continue;
                        }
                        let str_type = DataType::Nullable(Box::new(DataType::String));
                        (str_column, str_type)
                    } else {
                        if BloomIndex::check_large_string(&column) {
                            keys_to_remove.push(index);
                            continue;
                        }
                        (column, val_type)
                    }
                }
                _ => {
                    if BloomIndex::check_large_string(&column) {
                        keys_to_remove.push(index);
                        continue;
                    }
                    (column, field_type.clone())
                }
            };

            let (column, validity) =
                BloomIndex::calculate_nullable_column_digest(&self.func_ctx, &column, &data_type)?;
            // create filter per column
            if validity.as_ref().map(|v| v.null_count()).unwrap_or(0) > 0 {
                let validity = validity.unwrap();
                let it = column.deref().iter().zip(validity.iter()).map(
                    |(v, b)| {
                        if !b {
                            &0
                        } else {
                            v
                        }
                    },
                );
                index_column.builder.add_digests(it);
            } else {
                index_column.builder.add_digests(column.deref());
            }
        }
        for (index, index_column) in ngram_iter {
            let field_type = &block.get_by_offset(index_column.index).data_type;
            if !BloomFilter::supported_type(field_type) {
                keys_to_remove.push(index);
                continue;
            }

            let column = match &block.get_by_offset(index_column.index).value {
                Value::Scalar(s) => {
                    let builder = ColumnBuilder::repeat(&s.as_ref(), 1, field_type);
                    builder.build()
                }
                Value::Column(c) => c.clone(),
            };

            for digests in BloomIndex::calculate_ngram_nullable_column(
                Value::Column(column),
                index_column.gram_size,
                BloomIndex::ngram_hash,
            ) {
                if digests.is_empty() {
                    continue;
                }
                index_column.builder.add_digests(digests.iter())
            }
        }
        for k in keys_to_remove {
            self.columns.remove(k);
        }
        Ok(())
    }

    pub fn finalize(&mut self) -> Result<Option<BloomIndex>> {
        let mut column_distinct_count = HashMap::with_capacity(self.columns.len());
        let mut filters = Vec::with_capacity(self.columns.len());
        let mut filter_fields = Vec::with_capacity(self.columns.len());
        for column in self.columns.iter_mut() {
            let filter = column.builder.build()?;
            let filter_name = if column.is_ngram {
                BloomIndex::build_filter_ngram_name(&column.field)
            } else {
                if let Some(len) = filter.len() {
                    if !matches!(
                        column.field.data_type().remove_nullable(),
                        TableDataType::Map(_) | TableDataType::Variant
                    ) {
                        column_distinct_count.insert(column.field.column_id, len);
                        // Not need to generate bloom index,
                        // it will never be used since range index is checked first.
                        if len < 2 {
                            continue;
                        }
                    }
                }
                BloomIndex::build_filter_bloom_name(BlockFilter::VERSION, &column.field)?
            };
            filter_fields.push(TableField::new(&filter_name, TableDataType::Binary));
            filters.push(Arc::new(filter));
        }

        if filter_fields.is_empty() {
            return Ok(None);
        }
        let filter_schema = Arc::new(TableSchema::new(filter_fields));
        Ok(Some(BloomIndex {
            func_ctx: self.func_ctx.clone(),
            version: BlockFilter::VERSION,
            filter_schema,
            filters,
            column_distinct_count,
        }))
    }
}

struct Visitor<T: EqVisitor>(T);

impl<T> ExprVisitor<String> for Visitor<T>
where T: EqVisitor
{
    type Error = ErrorCode;

    fn enter_function_call(&mut self, expr: &FunctionCall<String>) -> Result<Option<Expr<String>>> {
        let FunctionCall {
            span,
            id,
            args,
            return_type,
            ..
        } = expr;

        if id.name() != "eq" && id.name() != "like" {
            return Self::visit_function_call(expr, self);
        }
        let mut result = ControlFlow::Continue(None);

        if id.name() == "like" {
            match args.as_slice() {
                // patterns like `Column = <constant>`, `<constant> = Column`
                [Expr::ColumnRef(ColumnRef {
                    id,
                    data_type: column_type,
                    ..
                }), Expr::Constant(Constant { scalar, .. })]
                | [Expr::Constant(Constant { scalar, .. }), Expr::ColumnRef(ColumnRef {
                    id,
                    data_type: column_type,
                    ..
                })] => {
                    if let Some(pattern) = scalar.as_string() {
                        if let LikePattern::SurroundByPercent(v) =
                            generate_like_pattern(pattern.as_bytes(), 1)
                        {
                            let string = String::from_utf8_lossy(v.needle()).to_string();

                            result = self.0.enter_target(
                                *span,
                                id,
                                &Scalar::String(string),
                                column_type,
                                return_type,
                                true,
                            )?;
                        }
                    }
                }
                _ => (),
            }
        } else {
            result = match args.as_slice() {
                // patterns like `Column = <constant>`, `<constant> = Column`
                [Expr::ColumnRef(ColumnRef {
                    id,
                    data_type: column_type,
                    ..
                }), Expr::Constant(Constant {
                    scalar,
                    data_type: scalar_type,
                    ..
                })]
                | [Expr::Constant(Constant {
                    scalar,
                    data_type: scalar_type,
                    ..
                }), Expr::ColumnRef(ColumnRef {
                    id,
                    data_type: column_type,
                    ..
                })] => {
                    // decimal don't respect datatype equal
                    // debug_assert_eq!(scalar_type, column_type);
                    // If the visitor returns a new expression, then replace with the current expression.
                    if scalar_type == column_type {
                        self.0
                            .enter_target(*span, id, scalar, column_type, return_type, false)?
                    } else {
                        ControlFlow::Continue(None)
                    }
                }
                // patterns like `MapColumn[<key>] = <constant>`, `<constant> = MapColumn[<key>]`
                [Expr::FunctionCall(FunctionCall { id, args, .. }), Expr::Constant(Constant {
                    scalar,
                    data_type: scalar_type,
                    ..
                })]
                | [Expr::Constant(Constant {
                    scalar,
                    data_type: scalar_type,
                    ..
                }), Expr::FunctionCall(FunctionCall { id, args, .. })]
                    if id.name() == "get" =>
                {
                    self.0
                        .enter_map_column(*span, args, scalar, scalar_type, return_type, false)?
                }
                // patterns like `CAST(MapColumn[<key>] as X) = <constant>`, `<constant> = CAST(MapColumn[<key>] as X)`
                [Expr::Cast(Cast {
                    expr:
                        box Expr::FunctionCall(FunctionCall {
                            id,
                            args,
                            return_type,
                            ..
                        }),
                    dest_type,
                    ..
                }), Expr::Constant(Constant {
                    scalar,
                    data_type: scalar_type,
                    ..
                })]
                | [Expr::Constant(Constant {
                    scalar,
                    data_type: scalar_type,
                    ..
                }), Expr::Cast(Cast {
                    expr:
                        box Expr::FunctionCall(FunctionCall {
                            id,
                            args,
                            return_type,
                            ..
                        }),
                    dest_type,
                    ..
                })] if id.name() == "get" => {
                    // Only support cast variant value in map to string value
                    if return_type.remove_nullable() != DataType::Variant
                        || dest_type.remove_nullable() != DataType::String
                    {
                        ControlFlow::Break(None)
                    } else {
                        self.0.enter_map_column(
                            *span,
                            args,
                            scalar,
                            scalar_type,
                            return_type,
                            false,
                        )?
                    }
                }
                [cast @ Expr::Cast(_), Expr::Constant(constant)]
                | [Expr::Constant(constant), cast @ Expr::Cast(_)] => {
                    self.0.enter_cast(cast, constant, false)?
                }

                [func @ Expr::FunctionCall(FunctionCall {
                    id,
                    args,
                    return_type: dest_type,
                    ..
                }), Expr::Constant(constant)]
                | [Expr::Constant(constant), func @ Expr::FunctionCall(FunctionCall {
                    id,
                    args,
                    return_type: dest_type,
                    ..
                })] if id.name().starts_with("to_")
                    && args.len() == 1
                    && func.contains_column_ref() =>
                {
                    self.0.enter_cast(
                        &Expr::Cast(Cast {
                            span: *span,
                            is_try: false,
                            expr: Box::new(args[0].clone()),
                            dest_type: dest_type.clone(),
                        }),
                        constant,
                        false,
                    )?
                }
                _ => ControlFlow::Continue(None),
            };
        }
        match result {
            ControlFlow::Continue(Some(expr)) => visit_expr(&expr, self),
            ControlFlow::Continue(None) => Self::visit_function_call(expr, self),
            ControlFlow::Break(expr) => Ok(expr),
        }
    }

    fn enter_lambda_function_call(
        &mut self,
        _: &LambdaFunctionCall<String>,
    ) -> Result<Option<Expr<String>>> {
        Ok(None)
    }
}

type ResultRewrite = Result<ControlFlow<Option<Expr<String>>, Option<Expr<String>>>>;

trait EqVisitor {
    fn enter_target(
        &mut self,
        span: Span,
        col_name: &str,
        scalar: &Scalar,
        ty: &DataType,
        return_type: &DataType,
        is_like: bool,
    ) -> ResultRewrite;

    fn enter_map_column(
        &mut self,
        span: Span,
        args: &[Expr<String>],
        scalar: &Scalar,
        scalar_type: &DataType,
        return_type: &DataType,
        is_like: bool,
    ) -> ResultRewrite {
        match &args[0] {
            Expr::ColumnRef(ColumnRef { id, data_type, .. })
            | Expr::Cast(Cast {
                expr: box Expr::ColumnRef(ColumnRef { id, data_type, .. }),
                ..
            }) => {
                if let DataType::Map(box inner_ty) = data_type.remove_nullable() {
                    let val_type = match inner_ty {
                        DataType::Tuple(kv_tys) => kv_tys[1].clone(),
                        _ => unreachable!(),
                    };
                    // Only JSON value of string type have bloom index.
                    if val_type.remove_nullable() == DataType::Variant {
                        // If the scalar value is variant string, we can try extract the string
                        // value to take advantage of bloom filtering.
                        if scalar_type.remove_nullable() == DataType::Variant {
                            if let Some(val) = scalar.as_variant() {
                                let raw_jsonb = RawJsonb::new(val);
                                if let Ok(Some(str_val)) = raw_jsonb.as_str() {
                                    let new_scalar = Scalar::String(str_val.to_string());
                                    let new_scalar_type = DataType::String;
                                    return self.enter_target(
                                        span,
                                        id,
                                        &new_scalar,
                                        &new_scalar_type,
                                        return_type,
                                        is_like,
                                    );
                                }
                            }
                        }
                        if scalar_type.remove_nullable() != DataType::String {
                            return Ok(ControlFlow::Continue(None));
                        }
                    } else if val_type.remove_nullable() != scalar_type.remove_nullable() {
                        return Ok(ControlFlow::Continue(None));
                    }
                    return self.enter_target(span, id, scalar, scalar_type, return_type, is_like);
                }
            }
            _ => {}
        }
        Ok(ControlFlow::Continue(None))
    }

    fn enter_cast(&mut self, _cast: &Expr<String>, _constant: &Constant, _: bool) -> ResultRewrite {
        Ok(ControlFlow::Continue(None))
    }
}

struct RewriteVisitor<'a> {
    new_col_id: usize,
    index: &'a BloomIndex,
    data_schema: TableSchemaRef,
    scalar_map: &'a HashMap<Scalar, u64>,
    ngram_args: &'a [NgramArgs],
    column_stats: &'a StatisticsOfColumns,
    domains: &'a mut HashMap<String, Domain>,
}

impl EqVisitor for RewriteVisitor<'_> {
    fn enter_target(
        &mut self,
        span: Span,
        col_name: &str,
        scalar: &Scalar,
        ty: &DataType,
        return_type: &DataType,
        is_like: bool,
    ) -> ResultRewrite {
        let table_field = self.data_schema.field_with_name(col_name)?;
        let filter_column = if is_like {
            BloomIndex::build_filter_ngram_name(table_field)
        } else {
            BloomIndex::build_filter_bloom_name(self.index.version, table_field)?
        };

        // If the column doesn't contain the constant,
        // we rewrite the expression to a new column with `false` domain.
        if self.index.find(
            &filter_column,
            scalar,
            ty,
            self.scalar_map,
            self.ngram_args,
            is_like,
        )? != FilterEvalResult::MustFalse
        {
            return Ok(ControlFlow::Continue(None));
        }
        let new_col_name = format!("__bloom_column_{}_{}", col_name, self.new_col_id);
        self.new_col_id += 1;

        let bool_domain = Domain::Boolean(BooleanDomain {
            has_false: true,
            has_true: false,
        });
        let new_domain = if return_type.is_nullable() {
            // generate `has_null` based on the `null_count` in column statistics.
            let has_null = match self.data_schema.column_id_of(col_name) {
                Ok(col_id) => match self.column_stats.get(&col_id) {
                    Some(stat) => stat.null_count > 0,
                    None => true,
                },
                Err(_) => true,
            };
            Domain::Nullable(NullableDomain {
                has_null,
                value: Some(Box::new(bool_domain)),
            })
        } else {
            bool_domain
        };
        self.domains.insert(new_col_name.clone(), new_domain);

        Ok(ControlFlow::Break(Some(
            ColumnRef {
                span,
                id: new_col_name.clone(),
                data_type: return_type.clone(),
                display_name: new_col_name,
            }
            .into(),
        )))
    }

    fn enter_cast(
        &mut self,
        cast: &Expr<String>,
        constant: &Constant,
        is_like: bool,
    ) -> ResultRewrite {
        let Expr::Cast(Cast {
            span,
            is_try: false,
            expr:
                box Expr::ColumnRef(ColumnRef {
                    id,
                    data_type: src_type,
                    ..
                }),
            dest_type,
            ..
        }) = cast
        else {
            return Ok(ControlFlow::Continue(None));
        };

        // For any injective function y = f(x), the eq(column, inverse_f(const)) is a necessary condition for eq(f(column), const).
        // For example, the result of col = '+1'::int contains col::string = '+1'.
        if !Xor8Filter::supported_type(src_type) || !is_injective_cast(src_type, dest_type) {
            return Ok(ControlFlow::Break(None));
        }

        // check domain for possible overflow
        if ConstantFolder::<String>::fold_with_domain(
            cast,
            self.domains,
            &FunctionContext::default(),
            &BUILTIN_FUNCTIONS,
        )
        .1
        .is_none()
        {
            return Ok(ControlFlow::Break(None));
        }

        let Some(s) = cast_const(
            &FunctionContext::default(),
            src_type.to_owned(),
            constant.clone(),
        ) else {
            return Ok(ControlFlow::Break(None));
        };
        if s.is_null() {
            return Ok(ControlFlow::Break(None));
        }

        self.enter_target(
            *span,
            id,
            &s,
            src_type,
            &if dest_type.is_nullable() {
                DataType::Boolean.wrap_nullable()
            } else {
                DataType::Boolean
            },
            is_like,
        )
    }
}

struct ShortListVisitor {
    bloom_fields: Vec<TableField>,
    ngram_fields: Vec<TableField>,
    bloom_founds: Vec<TableField>,
    ngram_founds: Vec<TableField>,
    bloom_scalars: Vec<(usize, Scalar, DataType)>,
    ngram_scalars: Vec<(usize, Scalar)>,
}

impl ShortListVisitor {
    fn found_field<'a>(fields: &'a [TableField], name: &str) -> Option<(usize, &'a TableField)> {
        fields
            .iter()
            .enumerate()
            .find_map(|(i, field)| (field.name == name).then_some((i, field)))
    }
}

impl EqVisitor for ShortListVisitor {
    fn enter_target(
        &mut self,
        _: Span,
        col_name: &str,
        scalar: &Scalar,
        ty: &DataType,
        _: &DataType,
        is_like: bool,
    ) -> ResultRewrite {
        if is_like {
            if let Some((i, v)) = Self::found_field(&self.ngram_fields, col_name) {
                if !scalar.is_null() && BloomFilter::supported_type(ty) {
                    self.ngram_founds.push(v.clone());
                    self.ngram_scalars.push((i, scalar.clone()));
                }
            }
        } else {
            if let Some((i, v)) = Self::found_field(&self.bloom_fields, col_name) {
                if !scalar.is_null() && Xor8Filter::supported_type(ty) {
                    self.bloom_founds.push(v.clone());
                    self.bloom_scalars.push((i, scalar.clone(), ty.clone()));
                }
            }
        }
        Ok(ControlFlow::Break(None))
    }

    fn enter_cast(
        &mut self,
        cast: &Expr<String>,
        constant: &Constant,
        is_like: bool,
    ) -> ResultRewrite {
        let Expr::Cast(Cast {
            is_try: false,
            expr:
                box Expr::ColumnRef(ColumnRef {
                    id,
                    data_type: src_type,
                    ..
                }),
            dest_type,
            ..
        }) = cast
        else {
            return Ok(ControlFlow::Continue(None));
        };

        if is_like {
            let Some((i, field)) = Self::found_field(&self.ngram_fields, id) else {
                return Ok(ControlFlow::Break(None));
            };
            if !BloomFilter::supported_type(src_type) || !is_injective_cast(src_type, dest_type) {
                return Ok(ControlFlow::Break(None));
            }

            let Some(s) = cast_const(
                &FunctionContext::default(),
                src_type.to_owned(),
                constant.clone(),
            ) else {
                return Ok(ControlFlow::Break(None));
            };

            if !s.is_null() {
                self.ngram_founds.push(field.to_owned());
                self.ngram_scalars.push((i, s));
            }
        } else {
            let Some((i, field)) = Self::found_field(&self.bloom_fields, id) else {
                return Ok(ControlFlow::Break(None));
            };
            if !Xor8Filter::supported_type(src_type) || !is_injective_cast(src_type, dest_type) {
                return Ok(ControlFlow::Break(None));
            }

            let Some(s) = cast_const(
                &FunctionContext::default(),
                src_type.to_owned(),
                constant.clone(),
            ) else {
                return Ok(ControlFlow::Break(None));
            };

            if !s.is_null() {
                self.bloom_founds.push(field.to_owned());
                self.bloom_scalars.push((i, s, src_type.to_owned()));
            }
        }

        Ok(ControlFlow::Break(None))
    }
}
