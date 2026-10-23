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

use super::vector_index_object_loader::VectorIndexLoader;

#[derive(Clone)]
pub struct VectorIndexReader {
    loader: VectorIndexLoader,
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
            loader: VectorIndexLoader::new(operator, settings, distance_type, dim, columns),
            query_values: processed_query_values,
        }
    }

    pub async fn prune(
        &self,
        limit: usize,
        row_count: usize,
        location: &str,
    ) -> Result<Vec<ScoredPointOffset>> {
        let index = self.loader.load(row_count, location).await?;
        index.index().search(limit, &self.query_values)
    }

    pub async fn generate_scores(
        &self,
        row_count: usize,
        location: &str,
    ) -> Result<Vec<ScoredPointOffset>> {
        let index = self.loader.load(row_count, location).await?;
        index
            .index()
            .generate_scores(row_count as u32, &self.query_values)
    }
}
