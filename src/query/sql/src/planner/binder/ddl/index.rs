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
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::sync::LazyLock;

use databend_common_ast::ast::CreateOption;
use databend_common_ast::ast::CreateTableIndexStmt;
use databend_common_ast::ast::DropTableIndexStmt;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::RefreshTableIndexStmt;
use databend_common_ast::ast::TableIndexType as AstTableIndexType;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchemaRef;
use databend_common_meta_app::schema::TableIndexType;
use itertools::Itertools;

use crate::BindContext;
use crate::binder::Binder;
use crate::plans::CreateTableIndexPlan;
use crate::plans::DropTableIndexPlan;
use crate::plans::Plan;
use crate::plans::RefreshTableIndexPlan;

const MAXIMUM_BLOOM_SIZE: u64 = 10 * 1024 * 1024;
const MINIMUM_BLOOM_SIZE: u64 = 512;

// valid values for inverted index option tokenizer
static INDEX_TOKENIZER_VALUES: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    let mut r = HashSet::new();
    r.insert("english");
    r.insert("chinese");
    r.insert("japanese");
    r
});

// valid values for inverted index option filter
static INDEX_FILTER_VALUES: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    let mut r = HashSet::new();
    r.insert("english_stop");
    r.insert("english_stemmer");
    r.insert("chinese_stop");
    r.insert("japanese_stop");
    r.insert("japanese_stemmer");
    r
});

// valid values for inverted index record option
static INDEX_RECORD_VALUES: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    let mut r = HashSet::new();
    r.insert("basic");
    r.insert("freq");
    r.insert("position");
    r
});

fn is_valid_tokenizer_values<S: AsRef<str>>(opt_val: S) -> bool {
    INDEX_TOKENIZER_VALUES.contains(opt_val.as_ref())
}

fn is_valid_filter_values<S: AsRef<str>>(opt_val: S) -> bool {
    INDEX_FILTER_VALUES.contains(opt_val.as_ref())
}

fn is_valid_index_record_values<S: AsRef<str>>(opt_val: S) -> bool {
    INDEX_RECORD_VALUES.contains(opt_val.as_ref())
}

// valid values for vector index distance
static INDEX_DISTANCE_VALUES: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    let mut r = HashSet::new();
    r.insert("cosine");
    r.insert("l1");
    r.insert("l2");
    r
});

fn is_valid_index_distance_values<S: AsRef<str>>(opt_val: S) -> bool {
    INDEX_DISTANCE_VALUES.contains(opt_val.as_ref())
}

/// Whether a column type can be indexed by a bloom filter. Mirrors
/// `Xor8Filter::supported_type` (storages-common-index), duplicated here to avoid pulling the
/// index crate into the planner. Keep in sync with that source.
fn is_bloom_supported_type(data_type: &TableDataType) -> bool {
    let inner = data_type.remove_nullable();
    if let TableDataType::Map(inner_ty) = &inner {
        if let TableDataType::Tuple { fields_type, .. } = inner_ty.remove_nullable() {
            return matches!(
                fields_type[1].remove_nullable(),
                TableDataType::Number(_)
                    | TableDataType::String
                    | TableDataType::Variant
                    | TableDataType::Timestamp
                    | TableDataType::Date
            );
        }
        return false;
    }
    matches!(
        inner,
        TableDataType::Number(_)
            | TableDataType::String
            | TableDataType::Timestamp
            | TableDataType::Date
    )
}

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_create_table_index(
        &mut self,
        _bind_context: &mut BindContext,
        stmt: &CreateTableIndexStmt,
    ) -> Result<Plan> {
        let CreateTableIndexStmt {
            create_option,
            index_name,
            index_type,
            catalog,
            database,
            table,
            columns,
            sync_creation,
            index_options,
        } = stmt;

        if matches!(index_type, AstTableIndexType::Bloom) && !sync_creation {
            return Err(ErrorCode::UnsupportedIndex(
                "ASYNC BLOOM INDEX is not supported".to_string(),
            ));
        }

        let (catalog, database, table) =
            self.normalize_object_identifier_triple(catalog, database, table);

        let table = self.ctx.get_table(&catalog, &database, &table).await?;

        if table.is_read_only() {
            return Err(ErrorCode::UnsupportedIndex(format!(
                "Table {} is read-only, creating index not allowed",
                table.name()
            )));
        }

        if !table.support_index() {
            return Err(ErrorCode::UnsupportedIndex(format!(
                "Table engine {} does not support create index",
                table.engine()
            )));
        }
        if table.is_temp() {
            return Err(ErrorCode::UnsupportedIndex(format!(
                "Table {} is temporary table, creating index not allowed",
                table.name()
            )));
        }
        let table_schema = table.schema();
        let table_id = table.get_id();
        let index_name = self.normalize_object_identifier(index_name);

        let (column_ids, index_options, meta_index_type) = match index_type {
            AstTableIndexType::Inverted => {
                let column_ids =
                    self.validate_inverted_index_columns(table_schema.clone(), columns)?;
                let index_options = self.validate_inverted_index_options(index_options)?;
                (column_ids, index_options, TableIndexType::Inverted)
            }
            AstTableIndexType::Ngram => {
                let column_ids =
                    self.validate_ngram_index_columns(table_schema.clone(), columns)?;
                let index_options = self.validate_ngram_index_options(index_options)?;
                (column_ids, index_options, TableIndexType::Ngram)
            }
            AstTableIndexType::Vector => {
                let column_ids =
                    self.validate_vector_index_columns(table_schema.clone(), columns)?;
                let index_options = self.validate_vector_index_options(index_options)?;
                (column_ids, index_options, TableIndexType::Vector)
            }
            AstTableIndexType::Spatial => {
                let column_ids =
                    self.validate_spatial_index_columns(table_schema.clone(), columns)?;
                let index_options = self.validate_spatial_index_options(index_options)?;
                (column_ids, index_options, TableIndexType::Spatial)
            }
            AstTableIndexType::Bloom => {
                let column_ids =
                    self.validate_bloom_index_columns(table_schema.clone(), columns)?;
                let index_options = self.validate_bloom_index_options(index_options)?;
                (column_ids, index_options, TableIndexType::Bloom)
            }
        };

        let table_info = table.get_table_info();
        let column_ids_set = column_ids.iter().copied().collect::<HashSet<_>>();
        for table_index in table_info.meta.indexes.values() {
            if index_name == table_index.name {
                if matches!(
                    create_option,
                    CreateOption::CreateIfNotExists | CreateOption::CreateOrReplace
                ) {
                    continue;
                }
                return Err(ErrorCode::UnsupportedIndex(format!(
                    "Index `{}` already exist",
                    index_name
                )));
            }
            if meta_index_type != table_index.index_type {
                continue;
            }
            let old_column_ids_set = table_index
                .column_ids
                .iter()
                .copied()
                .collect::<HashSet<_>>();
            let intersection_column_ids = old_column_ids_set
                .intersection(&column_ids_set)
                .collect::<HashSet<_>>();
            if !intersection_column_ids.is_empty() {
                let field_names = intersection_column_ids
                    .iter()
                    .map(|id| table_schema.field_of_column_id(**id).unwrap().name.clone())
                    .collect::<Vec<_>>()
                    .join(", ");

                return Err(ErrorCode::UnsupportedIndex(format!(
                    "{} index for columns ({}) already exist",
                    index_type, field_names
                )));
            }
        }

        let plan = CreateTableIndexPlan {
            index_type: *index_type,
            create_option: create_option.clone().into(),
            catalog,
            database,
            table: table.name().to_string(),
            index_name,
            column_ids,
            table_id,
            sync_creation: *sync_creation,
            index_options,
        };
        Ok(Plan::CreateTableIndex(Box::new(plan)))
    }

    pub(in crate::planner::binder) fn validate_ngram_index_columns(
        &self,
        table_schema: TableSchemaRef,
        columns: &[Identifier],
    ) -> Result<Vec<ColumnId>> {
        let mut column_set = BTreeSet::new();
        for column in columns {
            match table_schema.field_with_name(&column.name) {
                Ok(field) => {
                    if field.data_type.remove_nullable() != TableDataType::String {
                        return Err(ErrorCode::UnsupportedIndex(format!(
                            "Ngram index currently only support String type, but the type of column {} is {}",
                            column, field.data_type
                        )));
                    }
                    if column_set.contains(&field.column_id) {
                        return Err(ErrorCode::UnsupportedIndex(format!(
                            "Ngram index column must be unique, but column {} is duplicate",
                            column.name
                        )));
                    }
                    column_set.insert(field.column_id);
                }
                Err(_) => {
                    return Err(ErrorCode::UnsupportedIndex(format!(
                        "Table does not have column {}",
                        column
                    )));
                }
            }
        }
        Ok(Vec::from_iter(column_set))
    }

    pub(in crate::planner::binder) fn validate_ngram_index_options(
        &self,
        index_options: &BTreeMap<String, String>,
    ) -> Result<BTreeMap<String, String>> {
        let mut options = BTreeMap::new();
        for (opt, val) in index_options.iter() {
            let key = opt.to_lowercase();
            let value = val.to_lowercase();
            match key.as_str() {
                "gram_size" => {
                    match value.parse::<usize>() {
                        Ok(num) => {
                            if num == 0 {
                                return Err(ErrorCode::IndexOptionInvalid(
                                    "`gram_size` cannot be 0",
                                ));
                            }
                        }
                        Err(_) => {
                            return Err(ErrorCode::IndexOptionInvalid(format!(
                                "value `{value}` is not a legal number",
                            )));
                        }
                    }
                    options.insert("gram_size".to_string(), value);
                }
                "bloom_size" => {
                    match value.parse::<u64>() {
                        Ok(num) => {
                            if num == 0 {
                                return Err(ErrorCode::IndexOptionInvalid(
                                    "`bloom_size` cannot be 0",
                                ));
                            }
                            if num < MINIMUM_BLOOM_SIZE {
                                return Err(ErrorCode::IndexOptionInvalid(format!(
                                    "bloom_size: `{num}` is too small (bloom_size is minimum: {MINIMUM_BLOOM_SIZE})",
                                )));
                            }
                            if num > MAXIMUM_BLOOM_SIZE {
                                return Err(ErrorCode::IndexOptionInvalid(format!(
                                    "bloom_size: `{num}` is too large (bloom_size is maximum: {MAXIMUM_BLOOM_SIZE})",
                                )));
                            }
                        }
                        Err(_) => {
                            return Err(ErrorCode::IndexOptionInvalid(format!(
                                "value `{value}` is not a legal number",
                            )));
                        }
                    }
                    options.insert("bloom_size".to_string(), value);
                }
                "false_positive_rate" => {
                    match value.parse::<f64>() {
                        Ok(num) if num.is_finite() && num > 0.0 && num < 1.0 => {}
                        Ok(_) => {
                            return Err(ErrorCode::IndexOptionInvalid(
                                "`false_positive_rate` must be finite and between 0 and 1",
                            ));
                        }
                        Err(_) => {
                            return Err(ErrorCode::IndexOptionInvalid(format!(
                                "value `{value}` is not a legal number",
                            )));
                        }
                    }
                    options.insert("false_positive_rate".to_string(), value);
                }
                "hash_algorithm" => {
                    if !matches!(value.as_str(), "city64_v0" | "rolling_v1") {
                        return Err(ErrorCode::IndexOptionInvalid(format!(
                            "invalid NGRAM hash algorithm `{value}`, must be one of: city64_v0, rolling_v1"
                        )));
                    }
                    options.insert("hash_algorithm".to_string(), value);
                }
                _ => {
                    return Err(ErrorCode::IndexOptionInvalid(format!(
                        "index option `{key}` is invalid key for create ngram index statement",
                    )));
                }
            }
        }
        Ok(options)
    }

    pub(in crate::planner::binder) fn validate_inverted_index_columns(
        &self,
        table_schema: TableSchemaRef,
        columns: &[Identifier],
    ) -> Result<Vec<ColumnId>> {
        let mut column_set = BTreeSet::new();
        for column in columns {
            match table_schema.field_with_name(&column.name) {
                Ok(field) => {
                    if field.data_type.remove_nullable() != TableDataType::String
                        && field.data_type.remove_nullable() != TableDataType::Variant
                    {
                        return Err(ErrorCode::UnsupportedIndex(format!(
                            "Inverted index currently only support String and Variant type, but the type of column {} is {}",
                            column, field.data_type
                        )));
                    }
                    if column_set.contains(&field.column_id) {
                        return Err(ErrorCode::UnsupportedIndex(format!(
                            "Inverted index column must be unique, but column {} is duplicate",
                            column.name
                        )));
                    }
                    column_set.insert(field.column_id);
                }
                Err(_) => {
                    return Err(ErrorCode::UnsupportedIndex(format!(
                        "Table does not have column {}",
                        column
                    )));
                }
            }
        }
        Ok(Vec::from_iter(column_set))
    }

    pub(in crate::planner::binder) fn validate_inverted_index_options(
        &self,
        index_options: &BTreeMap<String, String>,
    ) -> Result<BTreeMap<String, String>> {
        let mut options = BTreeMap::new();
        for (opt, val) in index_options.iter() {
            let key = opt.to_lowercase();
            let value = val.to_lowercase();
            match key.as_str() {
                "tokenizer" => {
                    if !is_valid_tokenizer_values(&value) {
                        return Err(ErrorCode::IndexOptionInvalid(format!(
                            "value `{value}` is invalid index tokenizer",
                        )));
                    }
                    options.insert("tokenizer".to_string(), value.to_string());
                }
                "filters" => {
                    let raw_filters: Vec<&str> = value.split(',').collect();
                    let mut filters = Vec::with_capacity(raw_filters.len());
                    for raw_filter in raw_filters {
                        let filter = raw_filter.trim();
                        if !is_valid_filter_values(filter) {
                            return Err(ErrorCode::IndexOptionInvalid(format!(
                                "value `{filter}` is invalid index filters",
                            )));
                        }
                        filters.push(filter);
                    }
                    options.insert("filters".to_string(), filters.join(",").to_string());
                }
                "index_record" => {
                    if !is_valid_index_record_values(&value) {
                        return Err(ErrorCode::IndexOptionInvalid(format!(
                            "value `{value}` is invalid index record option",
                        )));
                    }
                    // convert to a JSON string, for `IndexRecordOption` deserialize
                    let index_record_val = format!("\"{}\"", value);
                    options.insert("index_record".to_string(), index_record_val);
                }
                _ => {
                    return Err(ErrorCode::IndexOptionInvalid(format!(
                        "index option `{key}` is invalid key for create inverted index statement",
                    )));
                }
            }
        }
        Ok(options)
    }

    pub(in crate::planner::binder) fn validate_vector_index_columns(
        &self,
        table_schema: TableSchemaRef,
        columns: &[Identifier],
    ) -> Result<Vec<ColumnId>> {
        let mut column_set = BTreeSet::new();
        for column in columns {
            match table_schema.field_with_name(&column.name) {
                Ok(field) => {
                    if !matches!(field.data_type.remove_nullable(), TableDataType::Vector(_)) {
                        return Err(ErrorCode::UnsupportedIndex(format!(
                            "Vector index only support Vector type, but the type of column {} is {}",
                            column, field.data_type
                        )));
                    }
                    if column_set.contains(&field.column_id) {
                        return Err(ErrorCode::UnsupportedIndex(format!(
                            "Vector index column must be unique, but column {} is duplicate",
                            column.name
                        )));
                    }
                    column_set.insert(field.column_id);
                }
                Err(_) => {
                    return Err(ErrorCode::UnsupportedIndex(format!(
                        "Table does not have column {}",
                        column
                    )));
                }
            }
        }
        Ok(Vec::from_iter(column_set))
    }

    pub(in crate::planner::binder) fn validate_vector_index_options(
        &self,
        index_options: &BTreeMap<String, String>,
    ) -> Result<BTreeMap<String, String>> {
        let mut options = BTreeMap::new();
        for (opt, val) in index_options.iter() {
            let key = opt.to_lowercase();
            let value = val.to_lowercase();
            match key.as_str() {
                "m" => {
                    match value.parse::<usize>() {
                        Ok(num) => {
                            if num == 0 {
                                return Err(ErrorCode::IndexOptionInvalid("`m` cannot be 0"));
                            }
                        }
                        Err(_) => {
                            return Err(ErrorCode::IndexOptionInvalid(format!(
                                "value `{value}` is not a legal number",
                            )));
                        }
                    }
                    options.insert("m".to_string(), value);
                }
                "ef_construct" => {
                    match value.parse::<usize>() {
                        Ok(num) => {
                            if num < 4 {
                                return Err(ErrorCode::IndexOptionInvalid(
                                    "`ef_construct` cannot less than 4",
                                ));
                            }
                        }
                        Err(_) => {
                            return Err(ErrorCode::IndexOptionInvalid(format!(
                                "value `{value}` is not a legal number",
                            )));
                        }
                    }
                    options.insert("ef_construct".to_string(), value);
                }
                "distance" => {
                    let raw_distances: Vec<&str> = value.split(',').collect();
                    let mut distances = BTreeSet::new();
                    for raw_distance in raw_distances {
                        let distance = raw_distance.trim();
                        if !is_valid_index_distance_values(distance) {
                            return Err(ErrorCode::IndexOptionInvalid(format!(
                                "value `{distance}` is invalid index distance type",
                            )));
                        }
                        distances.insert(distance);
                    }
                    options.insert(
                        "distance".to_string(),
                        distances.into_iter().join(",").to_string(),
                    );
                }
                _ => {
                    return Err(ErrorCode::IndexOptionInvalid(format!(
                        "index option `{key}` is invalid key for create vector index statement",
                    )));
                }
            }
        }
        if !options.contains_key("distance") {
            return Err(ErrorCode::IndexOptionInvalid(
                "must specify `distance` option, valid values are: `cosine`, `l1` and `l2`"
                    .to_string(),
            ));
        }
        Ok(options)
    }

    pub(in crate::planner::binder) fn validate_spatial_index_columns(
        &self,
        table_schema: TableSchemaRef,
        columns: &[Identifier],
    ) -> Result<Vec<ColumnId>> {
        let mut column_set = BTreeSet::new();
        for column in columns {
            match table_schema.field_with_name(&column.name) {
                Ok(field) => {
                    if !matches!(field.data_type.remove_nullable(), TableDataType::Geometry) {
                        return Err(ErrorCode::UnsupportedIndex(format!(
                            "Spatial index only supports Geometry type, but the type of column {} is {}",
                            column, field.data_type
                        )));
                    }
                    if column_set.contains(&field.column_id) {
                        return Err(ErrorCode::UnsupportedIndex(format!(
                            "Spatial index column must be unique, but column {} is duplicate",
                            column.name
                        )));
                    }
                    column_set.insert(field.column_id);
                }
                Err(_) => {
                    return Err(ErrorCode::UnsupportedIndex(format!(
                        "Table does not have column {}",
                        column
                    )));
                }
            }
        }
        Ok(Vec::from_iter(column_set))
    }

    pub(in crate::planner::binder) fn validate_spatial_index_options(
        &self,
        _index_options: &BTreeMap<String, String>,
    ) -> Result<BTreeMap<String, String>> {
        let options = BTreeMap::new();
        // todo
        Ok(options)
    }

    pub(in crate::planner::binder) fn validate_bloom_index_columns(
        &self,
        table_schema: TableSchemaRef,
        columns: &[Identifier],
    ) -> Result<Vec<ColumnId>> {
        let mut column_set = BTreeSet::new();
        for column in columns {
            match table_schema.field_with_name(&column.name) {
                Ok(field) => {
                    if !is_bloom_supported_type(&field.data_type) {
                        return Err(ErrorCode::UnsupportedIndex(format!(
                            "Bloom index does not support the type of column {}: {}",
                            column, field.data_type
                        )));
                    }
                    if column_set.contains(&field.column_id) {
                        return Err(ErrorCode::UnsupportedIndex(format!(
                            "Bloom index column must be unique, but column {} is duplicate",
                            column.name
                        )));
                    }
                    column_set.insert(field.column_id);
                }
                Err(_) => {
                    return Err(ErrorCode::UnsupportedIndex(format!(
                        "Table does not have column {}",
                        column
                    )));
                }
            }
        }
        Ok(Vec::from_iter(column_set))
    }

    pub(in crate::planner::binder) fn validate_bloom_index_options(
        &self,
        index_options: &BTreeMap<String, String>,
    ) -> Result<BTreeMap<String, String>> {
        let mut options = BTreeMap::new();
        for (opt, val) in index_options {
            let key = opt.to_lowercase();
            let value = val.to_lowercase();
            match key.as_str() {
                "filter_type" => {
                    if !matches!(value.as_str(), "xor8" | "binary_fuse32") {
                        return Err(ErrorCode::IndexOptionInvalid(format!(
                            "value `{value}` is invalid bloom index filter type, must be one of: xor8, binary_fuse32"
                        )));
                    }
                    options.insert(key, value);
                }
                _ => {
                    return Err(ErrorCode::IndexOptionInvalid(format!(
                        "index option `{key}` is invalid key for create bloom index statement"
                    )));
                }
            }
        }
        Ok(options)
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_drop_table_index(
        &mut self,
        _bind_context: &mut BindContext,
        stmt: &DropTableIndexStmt,
    ) -> Result<Plan> {
        let DropTableIndexStmt {
            if_exists,
            index_name,
            index_type,
            catalog,
            database,
            table,
        } = stmt;

        let (catalog, database, table) =
            self.normalize_object_identifier_triple(catalog, database, table);

        let table = self.ctx.get_table(&catalog, &database, &table).await?;
        if !table.support_index() {
            return Err(ErrorCode::UnsupportedIndex(format!(
                "Table engine {} does not support create index",
                table.engine()
            )));
        }
        let table_id = table.get_id();
        let index_name = self.normalize_object_identifier(index_name);

        let plan = DropTableIndexPlan {
            index_type: *index_type,
            if_exists: *if_exists,
            catalog,
            database,
            table: table.name().to_string(),
            index_name,
            table_id,
        };
        Ok(Plan::DropTableIndex(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_refresh_table_index(
        &mut self,
        _bind_context: &mut BindContext,
        stmt: &RefreshTableIndexStmt,
    ) -> Result<Plan> {
        let RefreshTableIndexStmt {
            index_name,
            index_type,
            catalog,
            database,
            table,
            limit: _,
        } = stmt;

        let (catalog, database, table) =
            self.normalize_object_identifier_triple(catalog, database, table);
        let index_name = self.normalize_object_identifier(index_name);

        let plan = RefreshTableIndexPlan {
            index_type: *index_type,
            catalog,
            database,
            table,
            index_name,
            segment_locs: None,
        };
        Ok(Plan::RefreshTableIndex(Box::new(plan)))
    }
}
