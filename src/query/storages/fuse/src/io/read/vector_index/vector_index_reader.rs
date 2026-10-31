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
use std::sync::Arc;
use std::time::Instant;

use databend_common_column::bitmap::TrueIdxIter;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_storages_common_index::DistanceType;
use databend_storages_common_index::Fp16QueryScorer;
use databend_storages_common_index::HNSWIndex;
use databend_storages_common_index::ScoredPointOffset;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::IndexFileLocation;
use log::debug;
use opendal::Operator;

use super::VectorIndexIdentity;
use super::vector_index_loader::RefineData;
use super::vector_index_object_loader::VectorIndexLoader;
use crate::io::VectorIndexSearchOptions;

#[derive(Clone)]
pub struct VectorIndexReader {
    loader: VectorIndexLoader,
    query_values: Vec<f32>,
    refine_scorer: Option<Arc<Fp16QueryScorer>>,
    operator: Operator,
    settings: ReadSettings,
    options: VectorIndexSearchOptions,
}

impl VectorIndexReader {
    pub fn create(
        operator: Operator,
        settings: ReadSettings,
        distance_type: DistanceType,
        query_values: Vec<f32>,
        identity: VectorIndexIdentity,
        options: VectorIndexSearchOptions,
    ) -> Self {
        let dim = query_values.len();
        // The FP16 scorer uses the original query; SQ8 search the preprocessed one.
        let refine_scorer = options
            .refine_factor
            .map(|_| Arc::new(Fp16QueryScorer::new(query_values.clone(), distance_type)));
        let query_values = HNSWIndex::preprocess_query(distance_type, query_values);
        let loader =
            VectorIndexLoader::new(operator.clone(), settings, distance_type, dim, identity);
        Self {
            loader,
            query_values,
            refine_scorer,
            operator,
            settings,
            options,
        }
    }

    pub async fn prune(
        &self,
        limit: usize,
        row_count: usize,
        location: &IndexFileLocation,
    ) -> Result<Vec<ScoredPointOffset>> {
        if limit == 0 || row_count == 0 {
            return Ok(Vec::new());
        }
        let index = self.loader.load(row_count, location).await?;
        index.index().search_with_ef(
            limit.min(row_count),
            self.options.search_ef.map(|ef| ef.min(row_count)),
            &self.query_values,
        )
    }

    /// SQ8 scores of every valid row.
    pub async fn generate_scores(
        &self,
        row_count: usize,
        location: &IndexFileLocation,
    ) -> Result<Vec<ScoredPointOffset>> {
        let index = self.loader.load(row_count, location).await?;
        index
            .index()
            .generate_scores(row_count as u32, &self.query_values)
    }

    /// FP16 scores of every valid row, for paths that output the scores of all rows
    /// with refinement enabled. Reads the whole FP16 data of the block in one
    /// request, bypassing the refine data cache.
    #[fastrace::trace]
    pub async fn generate_refined_scores(
        &self,
        row_count: usize,
        location: &IndexFileLocation,
    ) -> Result<Vec<ScoredPointOffset>> {
        let index = self.loader.load(row_count, location).await?;
        let offsets: Vec<u32> = TrueIdxIter::new(row_count, index.index().validity())
            .map(|row| row as u32)
            .collect();
        if offsets.is_empty() {
            return Ok(Vec::new());
        }
        let start = Instant::now();
        let (scorer, refine) = self.open_refine(row_count, location).await?;
        let data = refine.read_all(&self.settings).await?;
        let scores = scorer.score_page(&data, 0, &offsets)?;
        debug!(
            "Vector FP16 full read: rows={}, bytes={}, elapsed_ms={}",
            offsets.len(),
            data.len(),
            start.elapsed().as_millis()
        );
        Ok(scores)
    }

    /// The FP16 scorer and the block's FP16 data, checked to match.
    async fn open_refine(
        &self,
        row_count: usize,
        location: &IndexFileLocation,
    ) -> Result<(&Fp16QueryScorer, RefineData)> {
        let scorer = self
            .refine_scorer
            .as_deref()
            .ok_or_else(|| ErrorCode::BadArguments("FP16 refinement is not enabled"))?;
        let refine =
            RefineData::open(&self.operator, location, self.loader.identity(), row_count).await?;
        if refine.dimension != scorer.dimension() {
            return Err(ErrorCode::StorageOther("FP16 bundle dimension mismatch"));
        }
        Ok((scorer, refine))
    }

    /// Rescores `offsets` from the FP16 data. `offsets` must be valid (non-NULL)
    /// rows: HNSW search and `generate_scores` never return NULL rows.
    #[fastrace::trace]
    pub async fn refine_scores(
        &self,
        row_count: usize,
        location: &IndexFileLocation,
        offsets: &[u32],
    ) -> Result<Vec<ScoredPointOffset>> {
        if offsets.is_empty() {
            return Ok(Vec::new());
        }
        let start = Instant::now();
        if offsets.iter().any(|&offset| offset as usize >= row_count) {
            return Err(ErrorCode::StorageOther("FP16 row offset outside block"));
        }
        let (scorer, refine) = self.open_refine(row_count, location).await?;
        // Group the rows by page; read only those pages, exactly (no gap merging).
        let mut pages: BTreeMap<usize, Vec<u32>> = BTreeMap::new();
        for &offset in offsets {
            pages
                .entry(offset as usize / refine.rows_per_page)
                .or_default()
                .push(offset);
        }
        let ordinals: Vec<_> = pages.keys().copied().collect();
        let mut settings = self.settings;
        settings.max_gap_size = 0;
        let data = refine.read_pages(&settings, &ordinals).await?;
        let selected_bytes: usize = data.iter().map(|bytes| bytes.len()).sum();
        let mut scores = Vec::with_capacity(offsets.len());
        for ((ordinal, offsets), bytes) in pages.into_iter().zip(data) {
            let rows = refine.page_row_range(ordinal)?;
            scores.extend(scorer.score_page(&bytes, rows.start as usize, &offsets)?);
        }
        scores.sort_unstable_by_key(|score| score.idx);
        debug!(
            "Vector FP16 read: selected_pages={}, candidate_rows={}, selected_bytes={}, elapsed_ms={}",
            ordinals.len(),
            offsets.len(),
            selected_bytes,
            start.elapsed().as_millis()
        );
        Ok(scores)
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::Column;
    use databend_common_expression::types::F32;
    use databend_common_expression::types::VectorColumn;
    use databend_storages_common_index::VectorBundleBuilder;
    use opendal::services::Memory;

    use super::*;

    const DIM: usize = 8;
    const ROWS: usize = 100;
    const COLUMN_ID: u32 = 3;

    fn reader(operator: Operator) -> Result<VectorIndexReader> {
        let options = BTreeMap::from([("refine".to_owned(), "true".to_owned())]);
        Ok(VectorIndexReader::create(
            operator,
            ReadSettings {
                max_gap_size: 1024,
                max_range_size: 1024 * 1024,
                parquet_fast_read_bytes: 0,
            },
            DistanceType::L2,
            vec![1.0; DIM],
            VectorIndexIdentity {
                name: "vi".to_owned(),
                version: "v1".to_owned(),
                column_id: COLUMN_ID,
            },
            VectorIndexSearchOptions::new(&options, 4, 0)?,
        ))
    }

    /// A bundle as the writer produces it. Row `r` is `[r * 0.01; DIM]`; every 10th
    /// row is NULL.
    async fn write_bundle(operator: &Operator) -> Result<IndexFileLocation> {
        let values = (0..ROWS * DIM)
            .map(|i| F32::from((i / DIM) as f32 * 0.01))
            .collect::<Vec<_>>();
        let column = Column::Vector(VectorColumn::Float32((values.into(), DIM)))
            .wrap_nullable(Some((0..ROWS).map(|row| row % 10 != 0).collect()));
        let mut builder = VectorBundleBuilder::new("vi".to_owned(), "v1".to_owned(), ROWS as u64);
        let (fields, entries) =
            HNSWIndex::build(16, 64, COLUMN_ID, column.clone(), DistanceType::L2)?;
        for (field, entry) in fields.iter().zip(entries) {
            let bytes = entry.as_scalar().unwrap().as_binary().unwrap();
            builder.add_hnsw(field.name().clone(), bytes)?;
        }
        builder.add_validity(COLUMN_ID, column.as_nullable().unwrap().validity())?;
        builder.add_fp16_data(COLUMN_ID, &column)?;
        let data = builder.finish()?;
        let path = "test_vector_index_reader/_i_v_v2/v1/h0.index".to_owned();
        let size = data.len() as u64;
        operator.write(&path, data).await?;
        Ok(IndexFileLocation::Bundle { path, size })
    }

    #[tokio::test]
    async fn test_bundle_search_scores_and_refine() -> Result<()> {
        crate::test_utils::init_test_globals()?;
        let operator = Operator::new(Memory::default())?.finish();
        let location = write_bundle(&operator).await?;
        let index = reader(operator)?;
        let is_null = |idx: u32| idx.is_multiple_of(10);

        // NULL rows are neither searched nor scored.
        let found = index.prune(10, ROWS, &location).await?;
        assert_eq!(found.len(), 10);
        assert!(found.iter().all(|score| !is_null(score.idx)));
        let scores = index.generate_scores(ROWS, &location).await?;
        assert_eq!(scores.len(), 90);
        assert!(scores.iter().all(|score| !is_null(score.idx)));
        // A full read scores every valid row like page reads do.
        let all = index.generate_refined_scores(ROWS, &location).await?;
        assert_eq!(all.len(), 90);
        let paged = index.refine_scores(ROWS, &location, &[1, 98, 99]).await?;
        for score in &paged {
            let full = all.iter().find(|full| full.idx == score.idx).unwrap();
            assert_eq!(full.score, score.score);
        }

        // FP16 scores, sorted by row: L2 distance to [1.0; DIM] is
        // sqrt(DIM) * (1 - r * 0.01), up to FP16 rounding.
        let refined = index.refine_scores(ROWS, &location, &[99, 1, 98]).await?;
        assert_eq!(
            refined.iter().map(|score| score.idx).collect::<Vec<_>>(),
            vec![1, 98, 99]
        );
        for score in &refined {
            let expected = (DIM as f32).sqrt() * (1.0 - score.idx as f32 * 0.01);
            assert!((score.score - expected).abs() < 1e-2);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_refine_pages_follow_reader_page_size() -> Result<()> {
        crate::test_utils::init_test_globals()?;
        let operator = Operator::new(Memory::default())?.finish();
        let location = write_bundle(&operator).await?;
        let identity = VectorIndexIdentity {
            name: "vi".to_owned(),
            version: "v1".to_owned(),
            column_id: COLUMN_ID,
        };
        let settings = ReadSettings {
            max_gap_size: 0,
            max_range_size: 1024 * 1024,
            parquet_fast_read_bytes: 0,
        };
        let mut refine = RefineData::open(&operator, &location, &identity, ROWS).await?;
        let whole = refine.read_pages(&settings, &[0]).await?.remove(0);
        assert_eq!(whole.len(), ROWS * DIM * 2);

        // Another page size reads the same rows, cached under other names.
        refine.rows_per_page = 64;
        assert_eq!(refine.page_row_range(1)?, 64..100);
        assert!(refine.page_row_range(2).is_err());
        let pages = refine.read_pages(&settings, &[0, 1]).await?;
        assert_eq!([&pages[0][..], &pages[1][..]].concat(), whole);
        let small = refine.page_request(0)?.0;
        refine.rows_per_page = ROWS;
        assert_ne!(small, refine.page_request(0)?.0);
        Ok(())
    }
}
