// Copyright Qdrant
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

use std::sync::OnceLock;
use std::sync::atomic::AtomicBool;

use databend_common_column::bitmap::TrueIdxIter;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::types::F32;
use databend_common_expression::types::VectorColumn;
use databend_common_expression::types::VectorColumnBuilder;
use databend_common_expression::types::VectorScalar;
use log::error;
use rand::thread_rng;
use rayon::ThreadPool;
use rayon::ThreadPoolBuilder;
use rayon::iter::IntoParallelIterator;
use rayon::prelude::*;

use crate::hnsw_index::common::hugepage::HugePageBytes;
use crate::hnsw_index::common::types::PointOffsetType;
use crate::hnsw_index::common::types::ScoredPointOffset;
use crate::hnsw_index::common::utils::check_process_stopped;
use crate::hnsw_index::graph_layers::FilteredPoints;
use crate::hnsw_index::graph_layers::GraphLayers;
use crate::hnsw_index::graph_layers::SearchAlgorithm;
use crate::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::hnsw_index::graph_links::GraphLinksFormat;
use crate::hnsw_index::point_scorer::FilteredScorer;
use crate::hnsw_index::point_scorer::OriginalRawScorer;
use crate::hnsw_index::point_scorer::QuantizedRawScorer;
use crate::hnsw_index::point_scorer::RawScorer;
use crate::hnsw_index::quantization::DistanceType;
use crate::hnsw_index::quantization::EncodedVectorsU8;
use crate::hnsw_index::quantization::VectorParameters;
use crate::hnsw_index::quantization::encoded_vectors::EncodedVectors;

const SINGLE_THREADED_HNSW_BUILD_THRESHOLD: usize = 256;

/// Longest run of points a rayon job inserts without splitting further during an HNSW build.
///
/// Rayon's adaptive splitter stops splitting once its budget is spent unless a job gets stolen, so
/// with a non-power-of-two thread count one thread can be left with up to half the points while
/// the others idle. Capping the job length keeps the work stealable to the end (qdrant#10686).
const HNSW_BUILD_MAX_PAR_LEN: usize = 64;

/// Threads for HNSW builds in this process.
///
/// The write pipeline builds the indexes of several blocks at the same time. A
/// pool per block multiplied the threads by the number of blocks being built (16
/// pipeline threads of 16 threads each on a 16-core machine), and those blocks
/// fought for the last level cache. One shared pool keeps the threads bounded and
/// lets the scheduler spread them over however many blocks are being built.
fn hnsw_build_pool() -> &'static ThreadPool {
    static POOL: OnceLock<ThreadPool> = OnceLock::new();
    POOL.get_or_init(|| {
        let parallelism = match std::thread::available_parallelism() {
            Ok(degree) => degree.get(),
            Err(e) => {
                error!(
                    "failed to detect the number of parallelism: {}, fallback to 8",
                    e
                );
                8
            }
        };
        ThreadPoolBuilder::new()
            .thread_name(|index| format!("hnsw-build-{}", index))
            .num_threads(parallelism)
            .build()
            .expect("failed to build hnsw build thread pool")
    })
}

/// Whether the points left after the single-threaded prefix go to
/// [`hnsw_build_pool`]. The pool finishes the work it has started before it
/// takes a new build, so a small block sent there would wait behind the large
/// blocks being built. It is inserted on the calling thread instead, which would
/// otherwise sit blocked until the pool returns.
fn builds_in_parallel(points_left: usize) -> bool {
    points_left > SINGLE_THREADED_HNSW_BUILD_THRESHOLD
}

/// Name suffixes of the four HNSW components, in the order `HNSWIndex::build`
/// produces and `HNSWIndex::open` expects them.
const HNSW_GRAPH_LINKS_SUFFIX: &str = "_graph_links";
const HNSW_GRAPH_DATA_SUFFIX: &str = "_graph_data";
const HNSW_ENCODED_U8_META_SUFFIX: &str = "_encoded_u8_meta";
const HNSW_ENCODED_U8_DATA_SUFFIX: &str = "_encoded_u8_data";
const HNSW_COMPONENT_SUFFIXES: [&str; 4] = [
    HNSW_GRAPH_LINKS_SUFFIX,
    HNSW_GRAPH_DATA_SUFFIX,
    HNSW_ENCODED_U8_META_SUFFIX,
    HNSW_ENCODED_U8_DATA_SUFFIX,
];

/// Component name: `{column_id}-{distance_type}{suffix}`.
fn hnsw_component_name(column_id: ColumnId, distance_type: DistanceType, suffix: &str) -> String {
    format!("{column_id}-{distance_type}{suffix}")
}

/// Names of the four HNSW components of one column and distance, in `open` order.
pub fn hnsw_component_names(column_id: ColumnId, distance_type: DistanceType) -> Vec<String> {
    HNSW_COMPONENT_SUFFIXES
        .iter()
        .map(|suffix| hnsw_component_name(column_id, distance_type, suffix))
        .collect()
}

pub struct HNSWIndex {
    graph_layers: GraphLayers,
    distance_type: DistanceType,
    encoded_vectors: EncodedVectorsU8<HugePageBytes>,
    // Column validity, present only when the column has NULLs (none in old files).
    // Only `generate_scores` needs it: NULL rows are not linked into the graph.
    validity: Option<Bitmap>,
}

/// Maximum selectivity of filters to enable ACORN: the share of the rows of a
/// block that pass them. ACORN improves the recall of searches with
/// low-selectivity filters, at cost of performance. Qdrant's default; measured
/// on 1M Cohere 768-dim rows in 66k-row blocks, ACORN keeps recall@100 at 0.994
/// up to 25% of the rows and 0.975 at 40%, where the plain search gets 0.953 at
/// 1.5 times the throughput. Beyond, ACORN costs 30% to 45% of the throughput
/// for 2 to 3 points of recall.
pub const ACORN_MAX_SELECTIVITY: f64 = 0.4;

/// The graph search algorithm for a filter: ACORN when the filter passes at most
/// `max_selectivity` of the rows, as the links of the rows it rejects are then
/// needed to reach the rows it passes.
pub fn filtered_graph_algorithm(filter: &Bitmap, max_selectivity: f64) -> SearchAlgorithm {
    let selectivity = if filter.is_empty() {
        1.0
    } else {
        filter.true_count() as f64 / filter.len() as f64
    };
    if selectivity <= max_selectivity {
        SearchAlgorithm::Acorn
    } else {
        SearchAlgorithm::Hnsw
    }
}

impl HNSWIndex {
    /// Opens the four serialized components in `HNSW_COMPONENT_SUFFIXES` order.
    pub fn open(
        distance_type: DistanceType,
        dim: usize,
        count: usize,
        components: &[impl AsRef<[u8]>],
        validity: Option<&[u8]>,
    ) -> Result<HNSWIndex> {
        let [graph_links, graph_data, encoded_meta, encoded_data] = components else {
            return Err(ErrorCode::StorageOther("Expected four HNSW components"));
        };
        let graph_layers = GraphLayers::open(graph_links.as_ref(), graph_data.as_ref())?;

        let invert = match distance_type {
            DistanceType::Dot => false,
            DistanceType::L1 | DistanceType::L2 => true,
        };

        let params = VectorParameters {
            dim,
            count,
            distance_type,
            invert,
        };

        let encoded_vectors: EncodedVectorsU8<HugePageBytes> =
            EncodedVectorsU8::load(encoded_data.as_ref(), encoded_meta.as_ref(), &params)?;

        let validity = validity
            .map(|bytes| {
                Bitmap::try_new(bytes.to_vec(), count).map_err(|error| {
                    ErrorCode::StorageOther(format!("Invalid vector validity: {error}"))
                })
            })
            .transpose()?;

        Ok(Self {
            graph_layers,
            distance_type,
            encoded_vectors,
            validity,
        })
    }

    /// Column validity; `None` when every row is valid.
    pub fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }

    /// Memory retained by this index, including the maximum retained visited-list pool.
    /// Reserving the pool up front keeps cache accounting stable as searches reuse it.
    pub fn memory_size(&self) -> usize {
        std::mem::size_of::<Self>() + self.graph_layers.memory_size()
            - std::mem::size_of::<GraphLayers>()
            + self.encoded_vectors.memory_size()
            - std::mem::size_of::<EncodedVectorsU8<HugePageBytes>>()
            + self
                .validity
                .as_ref()
                .map_or(0, |bitmap| bitmap.as_slice().0.len())
    }

    /// The `limit` nearest rows found, plus, with `keep_ties`, the other rows
    /// found at the distance of the farthest of them: another sort key, such as
    /// `id` in `ORDER BY d, id`, may choose any tied row.
    ///
    /// With `filter`, one bit per row of the block, only its rows are found: the
    /// search neither scores nor returns the others. A filter that passes few
    /// rows leaves the graph with holes, so the search then explores the links
    /// of the rejected rows too (ACORN, when the filter passes at most
    /// [`ACORN_MAX_SELECTIVITY`] of the rows, see `filtered_graph_algorithm`);
    /// callers score blocks with very few passing rows with `generate_scores`
    /// instead.
    pub fn search_with_ef(
        &self,
        limit: usize,
        search_ef: Option<usize>,
        query_values: &[f32],
        keep_ties: bool,
        filter: Option<&Bitmap>,
    ) -> Result<Vec<ScoredPointOffset>> {
        let algorithm = filter.map_or(SearchAlgorithm::Hnsw, |filter| {
            filtered_graph_algorithm(filter, ACORN_MAX_SELECTIVITY)
        });
        self.search_with_algorithm(limit, search_ef, query_values, keep_ties, filter, algorithm)
    }

    /// [`Self::search_with_ef`] with the given search algorithm.
    pub fn search_with_algorithm(
        &self,
        limit: usize,
        search_ef: Option<usize>,
        query_values: &[f32],
        keep_ties: bool,
        filter: Option<&Bitmap>,
        algorithm: SearchAlgorithm,
    ) -> Result<Vec<ScoredPointOffset>> {
        let query_encode = self.encoded_vectors.encode_query(query_values);

        let raw_scorer = RawScorer::Quantized(QuantizedRawScorer {
            query: query_encode,
            vector: &self.encoded_vectors,
        });

        // ef is used to maintain the size of the set of candidate points in the search process,
        // the larger the search precision is higher, the smaller the speed is faster,
        // take 4 times the limit is a more balanced parameter.
        let ef = search_ef
            .unwrap_or_else(|| limit.saturating_mul(4))
            .max(limit);
        let query_filter_scorer = FilteredScorer::new(&raw_scorer, filter);

        let is_stopped = AtomicBool::new(false);
        // To find the ties, return all `ef` rows: the search is the same as for
        // `limit` rows, which are the first of them.
        let top = if keep_ties { ef } else { limit };
        // When no entry point of the graph passes the filter, the search starts
        // from the first rows that do.
        let filtered_points = filter.map(|filter| {
            move |n: usize| -> Vec<PointOffsetType> {
                TrueIdxIter::new(filter.len(), Some(filter))
                    .take(n)
                    .map(|idx| idx as PointOffsetType)
                    .collect()
            }
        });
        let filtered_points = filtered_points
            .as_ref()
            .map(|reader| reader as &FilteredPoints<'_>);
        let values = self.graph_layers.search(
            top,
            ef,
            algorithm,
            query_filter_scorer,
            None,
            filtered_points,
            &is_stopped,
        )?;

        let values = Self::postprocess_score(self.distance_type, values);
        Ok(truncate_with_ties(values, limit))
    }

    /// The SQ8 scores of the valid rows of `filter` (one bit per row of the block),
    /// or of every valid row without one: a scan of the rows, not a graph search,
    /// for blocks where a filter passes too few rows for the graph.
    pub fn generate_scores(
        &self,
        row_nums: u32,
        query_values: &[f32],
        filter: Option<&Bitmap>,
    ) -> Result<Vec<ScoredPointOffset>> {
        let query_encode = self.encoded_vectors.encode_query(query_values);

        let raw_scorer = RawScorer::Quantized(QuantizedRawScorer {
            query: query_encode,
            vector: &self.encoded_vectors,
        });

        let score = |idx: u32| ScoredPointOffset {
            idx,
            score: raw_scorer.score_point(idx),
        };
        // NULL rows keep their slot, so `idx` is the block row; skip them here as
        // this path does not go through the graph. The bitmaps cover the whole
        // block, which is exactly `row_nums` rows.
        let rows = match (&self.validity, filter) {
            (Some(validity), Some(filter)) => Some(validity & filter),
            (Some(validity), None) => Some(validity.clone()),
            (None, Some(filter)) => Some(filter.clone()),
            (None, None) => None,
        };
        let values = match &rows {
            Some(bitmap) => TrueIdxIter::new(row_nums as usize, Some(bitmap))
                .map(|idx| score(idx as u32))
                .collect(),
            None => (0..row_nums).map(score).collect(),
        };

        let values = Self::postprocess_score(self.distance_type, values);
        Ok(values)
    }

    pub fn build(
        m: usize,
        ef_construct: usize,
        column_id: ColumnId,
        column: Column,
        distance_type: DistanceType,
    ) -> Result<(Vec<TableField>, Vec<BlockEntry>)> {
        let m0 = m * 2;
        let entry_points_num = 2;
        let use_heuristic = true;
        let num_vectors = column.len();
        let validity = column
            .as_nullable()
            .map(|nullable| nullable.validity().clone())
            .filter(|bitmap| bitmap.null_count() != 0);

        let column = column.remove_nullable();
        let vector_column = column.as_vector().unwrap();
        let vector_column = preprocess(distance_type, vector_column.clone());
        // Link the graph with the vectors that queries are scored against. For
        // cosine, those are normalized (the SQ8 codes, the query and the FP16
        // refinement all use cosine); linking raw vectors would build an inner
        // product graph that favours long vectors and loses recall.
        let column = Column::Vector(vector_column.clone());

        let mut rng = thread_rng();
        let mut graph_layers_builder = GraphLayersBuilder::new(
            num_vectors,
            m,
            m0,
            ef_construct,
            entry_points_num,
            use_heuristic,
        );

        for i in 0..column.len() {
            let vector_id = i as PointOffsetType;
            // NULL rows keep an empty level-0 slot so point ids stay block rows; they
            // are never inserted below, so they are not reachable in the graph.
            let level = if validity.as_ref().is_none_or(|bitmap| bitmap.get_bit(i)) {
                graph_layers_builder.get_random_layer(&mut rng)
            } else {
                0
            };
            graph_layers_builder.set_levels(vector_id, level);
        }

        let mut ids: Vec<usize> = TrueIdxIter::new(column.len(), validity.as_ref()).collect();
        let first_few_ids: Vec<usize> = ids
            .drain(..SINGLE_THREADED_HNSW_BUILD_THRESHOLD.min(ids.len()))
            .collect();

        let (data, dim) = vector_column.as_float32().unwrap();
        let data = as_f32_slice(data.as_slice());

        let stopped = AtomicBool::new(false);

        let mut index_fields = Vec::with_capacity(4);
        let mut index_columns = Vec::with_capacity(4);

        let insert_point = |vector_id| {
            check_process_stopped(&stopped)?;

            let raw_scorer = RawScorer::Original(OriginalRawScorer {
                distance_type,
                index: vector_id,
                column: &column,
            });
            let points_scorer = FilteredScorer::new(&raw_scorer, None);
            graph_layers_builder.link_new_point(vector_id as PointOffsetType, points_scorer);

            Ok::<_, ErrorCode>(())
        };

        for vector_id in first_few_ids {
            insert_point(vector_id)?;
        }
        if builds_in_parallel(ids.len()) {
            hnsw_build_pool().install(|| {
                ids.into_par_iter()
                    .with_max_len(HNSW_BUILD_MAX_PAR_LEN)
                    .try_for_each(insert_point)
            })?;
        } else {
            ids.into_iter().try_for_each(insert_point)?;
        }

        let (graph_links, graph_data) =
            graph_layers_builder.into_graph_data(GraphLinksFormat::Compressed)?;

        index_columns.push(BlockEntry::new_const_column(
            DataType::Binary,
            Scalar::Binary(graph_links),
            1,
        ));
        index_columns.push(BlockEntry::new_const_column(
            DataType::Binary,
            Scalar::Binary(graph_data),
            1,
        ));

        index_fields.push(TableField::new(
            &hnsw_component_name(column_id, distance_type, HNSW_GRAPH_LINKS_SUFFIX),
            TableDataType::Binary,
        ));
        index_fields.push(TableField::new(
            &hnsw_component_name(column_id, distance_type, HNSW_GRAPH_DATA_SUFFIX),
            TableDataType::Binary,
        ));

        // Set invert parameter to query the closest vector (the most similar vector).
        // For Dot distances: invert = false (because a larger dot product means more similar)
        // For L1 distances: invert = true (because a smaller Manhattan distance means more similar)
        // For L2 distances: invert = true (because a smaller Euclidean distance means more similar)
        let invert = match distance_type {
            DistanceType::Dot => false,
            DistanceType::L1 | DistanceType::L2 => true,
        };

        let params = VectorParameters {
            dim: *dim,
            count: column.len(),
            distance_type,
            invert,
        };

        let builder = Vec::new();
        // NULL rows hold a placeholder vector: encode them to keep point ids as
        // block rows, but fit the value range on the real vectors only.
        let encoded_vectors = EncodedVectorsU8::encode(
            (0..params.count).map(|i| &data[i * params.dim..(i + 1) * params.dim]),
            (0..params.count).map(|i| validity.as_ref().is_none_or(|bitmap| bitmap.get_bit(i))),
            builder,
            &params,
            None,
            &stopped,
        )?;

        let encoded_meta = encoded_vectors.build_meta()?;
        let encoded_data = encoded_vectors.build_data()?;

        index_columns.push(BlockEntry::new_const_column(
            DataType::Binary,
            Scalar::Binary(encoded_meta),
            1,
        ));
        index_columns.push(BlockEntry::new_const_column(
            DataType::Binary,
            Scalar::Binary(encoded_data),
            1,
        ));
        index_fields.push(TableField::new(
            &hnsw_component_name(column_id, distance_type, HNSW_ENCODED_U8_META_SUFFIX),
            TableDataType::Binary,
        ));
        index_fields.push(TableField::new(
            &hnsw_component_name(column_id, distance_type, HNSW_ENCODED_U8_DATA_SUFFIX),
            TableDataType::Binary,
        ));

        Ok((index_fields, index_columns))
    }

    pub fn preprocess_query(distance_type: DistanceType, query_values: Vec<f32>) -> Vec<f32> {
        match distance_type {
            DistanceType::Dot => cosine_preprocess(query_values),
            DistanceType::L1 | DistanceType::L2 => query_values,
        }
    }

    fn postprocess_score(
        distance_type: DistanceType,
        mut values: Vec<ScoredPointOffset>,
    ) -> Vec<ScoredPointOffset> {
        match distance_type {
            DistanceType::L1 => {
                for value in &mut values {
                    value.score = value.score.abs();
                }
            }
            DistanceType::L2 => {
                for value in &mut values {
                    value.score = value.score.abs().sqrt();
                }
            }
            DistanceType::Dot => {
                for value in &mut values {
                    value.score = (1.0_f32 - value.score).abs();
                }
            }
        }
        values
    }
}

/// The first `limit` of `values`, plus the later ones whose score equals the
/// largest of those `limit` scores. `values` are in search order, which the
/// postprocessed scores may not follow exactly (`(1 - s).abs()` for Dot).
fn truncate_with_ties(mut values: Vec<ScoredPointOffset>, limit: usize) -> Vec<ScoredPointOffset> {
    let rest = values.split_off(limit.min(values.len()));
    let last = values
        .iter()
        .map(|value| value.score)
        .fold(f32::NEG_INFINITY, f32::max);
    values.extend(rest.into_iter().filter(|value| value.score == last));
    values
}

fn preprocess(distance_type: DistanceType, column: VectorColumn) -> VectorColumn {
    match distance_type {
        DistanceType::Dot => {
            let ty = column.data_type();
            let len = column.len();
            let mut builder = VectorColumnBuilder::with_capacity(&ty, len);
            for scalar in column.iter() {
                let val = as_f32_slice(scalar.as_float32().unwrap());
                let new_val = match cosine_norm(val) {
                    Some(length) => val.iter().map(|x| F32::from(x / length)).collect(),
                    None => val.iter().copied().map(F32::from).collect(),
                };
                let new_scalar = VectorScalar::Float32(new_val);
                builder.push(&new_scalar.as_ref());
            }
            builder.build()
        }
        DistanceType::L1 | DistanceType::L2 => column,
    }
}

fn is_length_zero_or_normalized(length: f32) -> bool {
    length < f32::EPSILON || (length - 1.0).abs() <= 1.0e-6
}

/// The length that normalizes `vector` for cosine distance; `None` when it is
/// zero or the vector is already normalized.
fn cosine_norm(vector: &[f32]) -> Option<f32> {
    let length: f32 = vector.iter().map(|x| x * x).sum();
    (!is_length_zero_or_normalized(length)).then(|| length.sqrt())
}

fn cosine_preprocess(vector: Vec<f32>) -> Vec<f32> {
    match cosine_norm(&vector) {
        Some(length) => vector.iter().map(|x| x / length).collect(),
        None => vector,
    }
}

/// `values` as plain `f32`s, without copying them.
fn as_f32_slice(values: &[F32]) -> &[f32] {
    // SAFETY: `F32` is `OrderedFloat<f32>`, which is `#[repr(transparent)]` over
    // `f32` and permits every value, so both slices have the same layout.
    unsafe { std::slice::from_raw_parts(values.as_ptr().cast::<f32>(), values.len()) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_truncate_with_ties() {
        let scores = [0.1, 0.3, 0.2, 0.3, 0.4, 0.3];
        let ids = |limit| {
            let values = scores
                .iter()
                .enumerate()
                .map(|(idx, &score)| ScoredPointOffset {
                    idx: idx as u32,
                    score,
                })
                .collect();
            truncate_with_ties(values, limit)
                .iter()
                .map(|value| value.idx)
                .collect::<Vec<_>>()
        };
        // The largest of the first scores is 0.3, also the score of rows 3 and 5.
        assert_eq!(ids(2), [0, 1, 3, 5]);
        assert_eq!(ids(3), [0, 1, 2, 3, 5]);
        assert_eq!(ids(1), [0]);
        assert_eq!(ids(6), [0, 1, 2, 3, 4, 5]);
        assert!(ids(0).is_empty());
    }

    /// Two rows of one index at the same nearest distance are both returned for
    /// limit 1.
    #[test]
    fn test_search_keeps_ties() -> Result<()> {
        // Rows 3 and 9 are the same vector, the nearest to the query; the others
        // are at least 2 away.
        let vectors: Vec<[f32; 4]> = (0..16)
            .map(|i| match i {
                3 | 9 => [1.0; 4],
                _ => [i as f32, -(i as f32), 2.0, 0.5 * i as f32],
            })
            .collect();
        let values: Vec<F32> = vectors.iter().flatten().map(|&v| F32::from(v)).collect();
        let column = Column::Vector(VectorColumn::Float32((values.into(), 4)));
        let (_, entries) = HNSWIndex::build(4, 8, 0, column, DistanceType::L2)?;
        // Each component is a single-row binary column.
        let columns: Vec<_> = entries.iter().map(|entry| entry.to_column()).collect();
        let components: Vec<_> = columns
            .iter()
            .map(|column| column.as_binary().unwrap().index(0).unwrap())
            .collect();
        let index = HNSWIndex::open(DistanceType::L2, 4, vectors.len(), &components, None)?;

        // An ef of every row visits the whole graph, so the result is exact.
        let search = |limit, keep_ties| -> Result<Vec<u32>> {
            let rows =
                index.search_with_ef(limit, Some(vectors.len()), &[1.0; 4], keep_ties, None)?;
            assert!(rows.iter().all(|row| row.score == rows[0].score));
            let mut ids: Vec<_> = rows.iter().map(|row| row.idx).collect();
            ids.sort_unstable();
            Ok(ids)
        };
        assert_eq!(search(1, true)?, [3, 9]);
        assert_eq!(search(2, true)?, [3, 9]);
        // Without `keep_ties`, one of them.
        let one = search(1, false)?;
        assert!(one == [3] || one == [9], "{one:?}");
        assert_eq!(search(2, false)?, [3, 9]);
        Ok(())
    }

    /// A filtered search returns only rows of the filter, the nearest of them
    /// first; a scan of the filter's rows scores exactly those rows.
    #[test]
    fn test_search_with_filter() -> Result<()> {
        // Row i is at distance i from the query [0,0,0,0].
        let vectors: Vec<[f32; 4]> = (0..32).map(|i| [i as f32, 0.0, 0.0, 0.0]).collect();
        let values: Vec<F32> = vectors.iter().flatten().map(|&v| F32::from(v)).collect();
        let column = Column::Vector(VectorColumn::Float32((values.into(), 4)));
        let (_, entries) = HNSWIndex::build(4, 16, 0, column, DistanceType::L2)?;
        let columns: Vec<_> = entries.iter().map(|entry| entry.to_column()).collect();
        let components: Vec<_> = columns
            .iter()
            .map(|column| column.as_binary().unwrap().index(0).unwrap())
            .collect();
        let index = HNSWIndex::open(DistanceType::L2, 4, vectors.len(), &components, None)?;
        let query = [0.0; 4];

        // Rows 5, 7, 9, ... pass: odd rows from 5. On a line, the graph links a row
        // mostly to its adjacent rows, which the filter rejects: the plain filtered
        // search cannot leave its entry point, while ACORN crosses one rejected row
        // to the next passing one. 14 of 32 rows pass, above `ACORN_MAX_SELECTIVITY`,
        // so `search_with_ef` would take the plain search: the algorithm is given.
        let filter: Bitmap = (0..vectors.len()).map(|i| i % 2 == 1 && i >= 5).collect();
        let search = |algorithm| {
            index.search_with_algorithm(
                3,
                Some(vectors.len()),
                &query,
                false,
                Some(&filter),
                algorithm,
            )
        };
        let found = search(SearchAlgorithm::Acorn)?;
        let mut ids: Vec<_> = found.iter().map(|point| point.idx).collect();
        ids.sort_unstable();
        assert_eq!(ids, [5, 7, 9]);
        assert!(found.iter().all(|point| filter.get_bit(point.idx as usize)));
        // The plain search returns rows of the filter only, if not the nearest.
        let plain = search(SearchAlgorithm::Hnsw)?;
        assert!(!plain.is_empty());
        assert!(plain.iter().all(|point| filter.get_bit(point.idx as usize)));

        // The same rows by scanning them; the whole block without a filter.
        let scanned = index.generate_scores(vectors.len() as u32, &query, Some(&filter))?;
        let mut ids: Vec<_> = scanned.iter().map(|point| point.idx).collect();
        ids.sort_unstable();
        assert_eq!(ids, (5..32).step_by(2).collect::<Vec<u32>>());
        assert_eq!(
            index
                .generate_scores(vectors.len() as u32, &query, None)?
                .len(),
            vectors.len()
        );

        // A filter that passes nothing finds nothing, through the graph or the scan.
        let none: Bitmap = (0..vectors.len()).map(|_| false).collect();
        assert!(
            index
                .search_with_ef(3, Some(vectors.len()), &query, false, Some(&none),)?
                .is_empty()
        );
        assert!(
            index
                .generate_scores(vectors.len() as u32, &query, Some(&none))?
                .is_empty()
        );
        Ok(())
    }

    /// Blocks with few points build serially, without going through the pool.
    #[test]
    fn test_builds_in_parallel() {
        assert!(!builds_in_parallel(0));
        assert!(!builds_in_parallel(1));
        assert!(!builds_in_parallel(
            SINGLE_THREADED_HNSW_BUILD_THRESHOLD - 1
        ));
        assert!(!builds_in_parallel(SINGLE_THREADED_HNSW_BUILD_THRESHOLD));
        assert!(builds_in_parallel(SINGLE_THREADED_HNSW_BUILD_THRESHOLD + 1));
        assert!(builds_in_parallel(100_000));
    }

    /// One pool serves every build, whatever the number of blocks being built.
    #[test]
    fn test_build_pool_is_shared() {
        let pool = hnsw_build_pool();
        assert!(std::ptr::eq(pool, hnsw_build_pool()));
        assert_eq!(
            pool.current_num_threads(),
            std::thread::available_parallelism().map_or(8, |degree| degree.get())
        );
        let name = pool.install(|| std::thread::current().name().map(str::to_string));
        assert!(
            name.as_deref()
                .is_some_and(|name| name.starts_with("hnsw-build-")),
            "{name:?}"
        );
    }

    /// A block large enough to insert through the shared pool stays searchable:
    /// every row is reachable, and a stored vector is its own nearest row.
    #[test]
    fn test_build_with_many_points() -> Result<()> {
        // 256 points are inserted first, serially, and 512 more go through the pool.
        let num_points = 3 * SINGLE_THREADED_HNSW_BUILD_THRESHOLD;
        let dim = 4;
        // A coarse grid: SQ8 fits one alpha and offset to the whole column, so the
        // rows must stay apart by more than one quantization step to keep distinct
        // codes. Then a stored vector is its own nearest row.
        let vectors: Vec<[f32; 4]> = (0..num_points)
            .map(|i| {
                [
                    ((i % 16) * 16) as f32,
                    (((i / 16) % 16) * 16) as f32,
                    (((i / 256) % 4) * 16) as f32,
                    0.0,
                ]
            })
            .collect();
        let values: Vec<F32> = vectors.iter().flatten().map(|&v| F32::from(v)).collect();
        let column = Column::Vector(VectorColumn::Float32((values.into(), dim)));
        let (_, entries) = HNSWIndex::build(16, 64, 0, column, DistanceType::L2)?;
        let columns: Vec<_> = entries.iter().map(|entry| entry.to_column()).collect();
        let components: Vec<_> = columns
            .iter()
            .map(|column| column.as_binary().unwrap().index(0).unwrap())
            .collect();
        let index = HNSWIndex::open(DistanceType::L2, dim, num_points, &components, None)?;

        // An ef of every row visits the whole graph, so a row that is not returned
        // was never linked, or linked where the entry points do not reach.
        let reached: Vec<usize> = index
            .search_with_ef(num_points, Some(num_points), &vectors[0], false, None)?
            .iter()
            .map(|row| row.idx as usize)
            .collect();
        let missing: Vec<usize> = (0..num_points).filter(|id| !reached.contains(id)).collect();
        assert!(
            missing.is_empty(),
            "{} of {num_points} rows unreachable, first missing {:?}",
            missing.len(),
            &missing[..missing.len().min(20)],
        );

        for row in (0..num_points).step_by(64) {
            let found = index.search_with_ef(1, Some(num_points), &vectors[row], false, None)?;
            let nearest = found.first().map(|point| point.idx as usize);
            assert_eq!(nearest, Some(row), "nearest row of {row}");
        }
        Ok(())
    }

    /// A cosine graph is linked by the angles between rows, not their lengths:
    /// with lengths from 1 to 1000, an inner product graph links rows to the
    /// longest ones and a small ef misses most nearest neighbours.
    #[test]
    fn test_cosine_graph_ignores_vector_lengths() -> Result<()> {
        use rand::Rng as _;
        use rand::SeedableRng as _;
        use rand::rngs::StdRng;

        // Measured 0.904 with the normalized graph and 0.196 with the inner
        // product graph; the threshold sits far from both.
        const RECALL_THRESHOLD: f64 = 0.6;
        let (dim, num_points, num_queries, limit, ef) = (16, 3000, 50, 10, 16);
        let mut rng = StdRng::seed_from_u64(7);
        let mut direction = || -> Vec<f32> { (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect() };
        let queries: Vec<Vec<f32>> = (0..num_queries).map(|_| direction()).collect();
        let directions: Vec<Vec<f32>> = (0..num_points).map(|_| direction()).collect();
        let rows: Vec<Vec<f32>> = directions
            .iter()
            .enumerate()
            .map(|(i, row)| {
                // Lengths spread evenly over 1..1000 on a log scale.
                let length = 10f32.powf(3.0 * (i * 7919 % num_points) as f32 / num_points as f32);
                row.iter().map(|value| value * length).collect()
            })
            .collect();
        let values: Vec<F32> = rows.iter().flatten().map(|&v| F32::from(v)).collect();
        let column = Column::Vector(VectorColumn::Float32((values.into(), dim)));
        let (_, entries) = HNSWIndex::build(8, 64, 0, column, DistanceType::Dot)?;
        let columns: Vec<_> = entries.iter().map(|entry| entry.to_column()).collect();
        let components: Vec<_> = columns
            .iter()
            .map(|column| column.as_binary().unwrap().index(0).unwrap())
            .collect();
        let index = HNSWIndex::open(DistanceType::Dot, dim, num_points, &components, None)?;

        let cosine = |a: &[f32], b: &[f32]| {
            let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
            let norm = |v: &[f32]| v.iter().map(|x| x * x).sum::<f32>().sqrt();
            dot / (norm(a) * norm(b))
        };
        let mut hits = 0;
        for query in &queries {
            let mut exact: Vec<usize> = (0..num_points).collect();
            exact.sort_by(|&a, &b| cosine(query, &rows[b]).total_cmp(&cosine(query, &rows[a])));
            let query = HNSWIndex::preprocess_query(DistanceType::Dot, query.clone());
            let found = index.search_with_ef(limit, Some(ef), &query, false, None)?;
            hits += found
                .iter()
                .filter(|point| exact[..limit].contains(&(point.idx as usize)))
                .count();
        }
        let recall = hits as f64 / (num_queries * limit) as f64;
        assert!(recall >= RECALL_THRESHOLD, "cosine recall {recall}");
        Ok(())
    }

    /// A sparse filter leaves the graph with holes: the search expands only the
    /// rows it passes, so it may get stuck. ACORN explores the links of the
    /// rejected rows too and reaches the passing rows; `search_with_ef` picks
    /// it for a filter that passes at most `ACORN_MAX_SELECTIVITY`.
    #[test]
    fn test_acorn_search_recall_with_sparse_filter() -> Result<()> {
        use rand::Rng as _;
        use rand::SeedableRng as _;
        use rand::rngs::StdRng;

        // Measured: ACORN 0.97, plain filtered search 0.68; the threshold sits
        // between them.
        const RECALL_THRESHOLD: f64 = 0.85;
        let (dim, num_points, num_queries, limit, ef) = (16, 4000, 50, 10, 32);
        let mut rng = StdRng::seed_from_u64(11);
        let mut vector = || -> Vec<f32> { (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect() };
        let queries: Vec<Vec<f32>> = (0..num_queries).map(|_| vector()).collect();
        let rows: Vec<Vec<f32>> = (0..num_points).map(|_| vector()).collect();
        let values: Vec<F32> = rows.iter().flatten().map(|&v| F32::from(v)).collect();
        let column = Column::Vector(VectorColumn::Float32((values.into(), dim)));
        let (_, entries) = HNSWIndex::build(8, 64, 0, column, DistanceType::L2)?;
        let columns: Vec<_> = entries.iter().map(|entry| entry.to_column()).collect();
        let components: Vec<_> = columns
            .iter()
            .map(|column| column.as_binary().unwrap().index(0).unwrap())
            .collect();
        let index = HNSWIndex::open(DistanceType::L2, dim, num_points, &components, None)?;

        // One row in ten passes.
        let filter: Bitmap = (0..num_points).map(|i| i % 10 == 0).collect();
        assert_eq!(
            filtered_graph_algorithm(&filter, ACORN_MAX_SELECTIVITY),
            SearchAlgorithm::Acorn
        );
        let dense: Bitmap = (0..num_points).map(|i| i % 2 == 0).collect();
        assert_eq!(
            filtered_graph_algorithm(&dense, ACORN_MAX_SELECTIVITY),
            SearchAlgorithm::Hnsw
        );
        assert_eq!(
            filtered_graph_algorithm(&dense, 0.5),
            SearchAlgorithm::Acorn
        );

        let l2 =
            |a: &[f32], b: &[f32]| -> f32 { a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum() };
        let recall = |algorithm: Option<SearchAlgorithm>| -> Result<f64> {
            let mut hits = 0;
            for query in &queries {
                let mut exact: Vec<usize> = (0..num_points).filter(|i| i % 10 == 0).collect();
                exact.sort_by(|&a, &b| l2(query, &rows[a]).total_cmp(&l2(query, &rows[b])));
                let found = match algorithm {
                    Some(algorithm) => index.search_with_algorithm(
                        limit,
                        Some(ef),
                        query,
                        false,
                        Some(&filter),
                        algorithm,
                    )?,
                    None => index.search_with_ef(limit, Some(ef), query, false, Some(&filter))?,
                };
                assert!(found.iter().all(|point| filter.get_bit(point.idx as usize)));
                hits += found
                    .iter()
                    .filter(|point| exact[..limit].contains(&(point.idx as usize)))
                    .count();
            }
            Ok(hits as f64 / (num_queries * limit) as f64)
        };
        let acorn = recall(Some(SearchAlgorithm::Acorn))?;
        let plain = recall(Some(SearchAlgorithm::Hnsw))?;
        let chosen = recall(None)?;
        assert!(acorn >= RECALL_THRESHOLD, "acorn recall {acorn}");
        assert!(
            acorn > plain,
            "acorn recall {acorn} <= plain filtered recall {plain}"
        );
        assert_eq!(chosen, acorn, "search_with_ef did not pick ACORN");
        Ok(())
    }
}
