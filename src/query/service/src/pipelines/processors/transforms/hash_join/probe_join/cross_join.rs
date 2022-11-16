// Copyright 2022 Datafuse Labs.
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

use common_exception::Result;
use common_expression::Chunk;

use crate::pipelines::processors::transforms::hash_join::ProbeState;
use crate::pipelines::processors::JoinHashTable;

impl JoinHashTable {
    pub(crate) fn probe_cross_join(
        &self,
        input: &Chunk,
        _probe_state: &mut ProbeState,
    ) -> Result<Vec<Chunk>> {
        let build_chunks = self.row_space.data_chunks();
        let num_rows = build_chunks
            .iter()
            .fold(0, |acc, chunk| acc + chunk.num_rows());
        if build_chunks.is_empty() || num_rows == 0 {
            return Ok(vec![Chunk::empty()]);
        }
        let build_chunk = Chunk::concat(&build_chunks)?;
        let mut results = Vec::with_capacity(input.num_rows());
        for i in 0..input.num_rows() {
            let probe_chunk = Chunk::take(input, &[i as u32])?;
            results.push(self.merge_with_constant_chunk(&build_chunk, &probe_chunk)?);
        }
        Ok(results)
    }

    // Merge build chunk and probe chunk (1 row chunk)
    pub(crate) fn merge_with_constant_chunk(
        &self,
        build_chunk: &Chunk,
        probe_chunk: &Chunk,
    ) -> Result<Chunk> {
        let columns = Vec::with_capacity(build_chunk.num_columns() + probe_chunk.num_columns());
        let mut replicated_probe_chunk = Chunk::new(columns, build_chunk.num_rows());
        for (col, ty) in probe_chunk.columns().iter() {
            replicated_probe_chunk = replicated_probe_chunk.add_column(col.clone(), ty.clone())?;
        }
        for ((col, ty)) in build_chunk.columns().iter() {
            replicated_probe_chunk = replicated_probe_chunk.add_column(col.clone(), ty.clone())?;
        }
        Ok(replicated_probe_chunk)
    }
}
