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

use databend_common_exception::Result;
use databend_storages_common_index::DistanceType;
use databend_storages_common_index::HNSWIndex;
use databend_storages_common_index::ScoredPointOffset;
use databend_storages_common_io::ReadSettings;
use opendal::Operator;

use crate::io::read::vector_index::vector_index_loader::load_vector_index_files;

#[derive(Clone)]
pub struct VectorIndexReader {
    operator: Operator,
    settings: ReadSettings,
    dim: usize,
    distance_type: DistanceType,
    columns: Vec<String>,
    query_values: Vec<f32>,
}

impl VectorIndexReader {
    pub fn create(
        operator: Operator,
        settings: ReadSettings,
        distance_type: DistanceType,
        columns: Vec<String>,
        query_values: Vec<f32>,
    ) -> Self {
        let dim = query_values.len();
        let processed_query_values = HNSWIndex::preprocess_query(distance_type, query_values);

        Self {
            operator,
            settings,
            dim,
            distance_type,
            columns,
            query_values: processed_query_values,
        }
    }

    pub async fn prune(
        &self,
        limit: usize,
        row_count: usize,
        location: &str,
    ) -> Result<Vec<ScoredPointOffset>> {
        let binary_columns = load_vector_index_files(
            self.operator.clone(),
            &self.settings,
            &self.columns,
            location,
        )
        .await?;

        let hnsw_index = HNSWIndex::open(self.distance_type, self.dim, row_count, binary_columns)?;
        hnsw_index.search(limit, &self.query_values)
    }

    pub async fn generate_scores(
        &self,
        row_count: usize,
        location: &str,
    ) -> Result<Vec<ScoredPointOffset>> {
        let binary_columns = load_vector_index_files(
            self.operator.clone(),
            &self.settings,
            &self.columns,
            location,
        )
        .await?;

        let hnsw_index = HNSWIndex::open(self.distance_type, self.dim, row_count, binary_columns)?;
        hnsw_index.generate_scores(row_count as u32, &self.query_values)
    }
}
