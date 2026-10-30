// Copyright 2026 Datafuse Labs
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

//! Heap bytes owned by common metadata types. Inline bytes are counted by the container.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::mem::size_of;

use databend_common_expression::ComputedExpr;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::types::VectorScalar;
use databend_storages_common_table_meta::meta::BlockIndexMeta;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::Statistics;

pub(crate) fn vec<T>(v: &Vec<T>, heap: impl Fn(&T) -> usize) -> usize {
    v.capacity() * size_of::<T>() + v.iter().map(heap).sum::<usize>()
}

pub(crate) fn hash_map<K, V, S>(m: &HashMap<K, V, S>, heap: impl Fn(&K, &V) -> usize) -> usize {
    let buckets = if m.capacity() == 0 {
        0
    } else {
        (m.capacity() * 8 / 7).next_power_of_two()
    };
    buckets * (size_of::<(K, V)>() + 1) + m.iter().map(|(k, v)| heap(k, v)).sum::<usize>()
}

pub(crate) fn btree_map<K, V>(m: &BTreeMap<K, V>, heap: impl Fn(&K, &V) -> usize) -> usize {
    const NODE_CAPACITY: usize = 11;
    let nodes = m.len().div_ceil(NODE_CAPACITY * 2 / 3);
    let node_bytes = NODE_CAPACITY * (size_of::<K>() + size_of::<V>()) + 2 * size_of::<usize>();
    nodes * node_bytes + m.iter().map(|(k, v)| heap(k, v)).sum::<usize>()
}

pub(crate) fn option<T>(o: &Option<T>, heap: impl Fn(&T) -> usize) -> usize {
    o.as_ref().map_or(0, heap)
}

pub(crate) fn none<T>(_: &T) -> usize {
    0
}

pub(crate) fn scalar(s: &Scalar) -> usize {
    match s {
        Scalar::String(v) => v.capacity(),
        Scalar::Binary(v) | Scalar::Bitmap(v) | Scalar::Variant(v) | Scalar::Geometry(v) => {
            v.capacity()
        }
        Scalar::Geography(v) => v.0.capacity(),
        Scalar::Tuple(v) => vec(v, scalar),
        Scalar::Array(c) | Scalar::Map(c) => c.memory_size(false),
        Scalar::Vector(VectorScalar::Int8(v)) => v.capacity(),
        Scalar::Vector(VectorScalar::Float32(v)) => v.capacity() * size_of::<f32>(),
        _ => 0,
    }
}

pub(crate) fn location(l: &Location) -> usize {
    l.0.capacity()
}

pub(crate) fn column_statistics(c: &ColumnStatistics) -> usize {
    scalar(&c.min) + scalar(&c.max)
}

pub(crate) fn cluster_statistics(c: &ClusterStatistics) -> usize {
    vec(&c.min, scalar) + vec(&c.max, scalar) + option(&c.pages, |p| vec(p, scalar))
}

pub(crate) fn statistics(s: &Statistics) -> usize {
    hash_map(&s.col_stats, |_, c| column_statistics(c))
        + option(&s.virtual_col_stats, |m| {
            hash_map(m, |_, c| column_statistics(c))
        })
        + option(&s.spatial_stats, |m| hash_map(m, |_, _| 0))
        + option(&s.cluster_stats, cluster_statistics)
        + option(&s.partition_stats, |p| vec(&p.values, scalar))
        + option(&s.virtual_segment_schema, |v| {
            vec(&v.column_paths, |p| vec(&p.paths, none))
        })
        + option(&s.additional_stats_meta, |a| {
            location(&a.location) + option(&a.hll, Vec::capacity)
        })
}

pub(crate) fn table_data_type(t: &TableDataType) -> usize {
    let boxed = |inner: &TableDataType| size_of::<TableDataType>() + table_data_type(inner);
    match t {
        TableDataType::Nullable(inner)
        | TableDataType::Array(inner)
        | TableDataType::Map(inner) => boxed(inner),
        TableDataType::Tuple {
            fields_name,
            fields_type,
        } => vec(fields_name, |s| s.capacity()) + vec(fields_type, table_data_type),
        TableDataType::AggregateState {
            function_name,
            params,
            argument_types,
            state_type,
            ..
        } => {
            function_name.capacity()
                + vec(params, none)
                + vec(argument_types, table_data_type)
                + boxed(state_type)
        }
        _ => 0,
    }
}

fn table_field(f: &TableField) -> usize {
    f.name.capacity()
        + option(&f.default_expr, |s| s.capacity())
        + table_data_type(&f.data_type)
        + option(&f.computed_expr, |e| match e {
            ComputedExpr::Virtual(s) | ComputedExpr::Stored(s) => s.capacity(),
        })
}

pub(crate) fn table_schema(s: &TableSchema) -> usize {
    vec(&s.fields, table_field) + btree_map(&s.metadata, |k, v| k.capacity() + v.capacity())
}

fn block_index_metas(metas: &Vec<BlockIndexMeta>) -> usize {
    vec(metas, |m| m.index_name.capacity() + location(&m.location))
}

pub(crate) fn block_meta(b: &BlockMeta) -> usize {
    hash_map(&b.col_stats, |_, c| column_statistics(c))
        + hash_map(&b.col_metas, |_, _| 0)
        + option(&b.cluster_stats, cluster_statistics)
        + option(&b.partition_stats, |p| vec(&p.values, scalar))
        + location(&b.location)
        + option(&b.bloom_filter_index_location, location)
        + option(&b.inverted_index_metas, block_index_metas)
        + option(&b.vector_index_location, location)
        + option(&b.vector_index_metas, block_index_metas)
        + option(&b.spatial_index_location, location)
        + option(&b.spatial_stats, |m| hash_map(m, |_, _| 0))
        + option(&b.vector_stats, |m| hash_map(m, |_, _| 0))
        + option(&b.virtual_block_meta, |v| {
            hash_map(&v.virtual_column_metas, |_, _| 0) + location(&v.virtual_location)
        })
        + option(&b.virtual_path_statistics, |m| {
            hash_map(m, |_, s| vec(&s.path_counts, none))
        })
}
