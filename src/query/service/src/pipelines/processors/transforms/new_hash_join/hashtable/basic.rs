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

use crate::pipelines::processors::transforms::hash_join_table::RowPtr;

#[derive(Debug)]
pub struct ProbedRows {
    pub unmatched: Vec<u64>,
    pub matched_probe: Vec<u64>,
    pub matched_build: Vec<RowPtr>,
}

impl ProbedRows {
    pub fn empty() -> ProbedRows {
        ProbedRows::new(vec![], vec![], vec![])
    }

    pub fn new(
        unmatched: Vec<u64>,
        matched_probe: Vec<u64>,
        matched_build: Vec<RowPtr>,
    ) -> ProbedRows {
        assert_eq!(matched_build.len(), matched_probe.len());

        ProbedRows {
            unmatched,
            matched_probe,
            matched_build,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.matched_build.is_empty() && self.unmatched.is_empty()
    }

    pub fn is_all_unmatched(&self) -> bool {
        self.matched_build.is_empty() && !self.unmatched.is_empty()
    }

    pub fn all_unmatched(unmatched: Vec<u64>) -> ProbedRows {
        ProbedRows::new(unmatched, vec![], vec![])
    }

    pub fn clear(&mut self) {
        self.unmatched.clear();
        self.matched_probe.clear();
        self.matched_build.clear();
    }
}

pub trait ProbeStream {
    fn advance(&mut self, res: &mut ProbedRows, max_rows: usize) -> Result<()>;
}

pub struct EmptyProbeStream;

impl ProbeStream for EmptyProbeStream {
    fn advance(&mut self, _res: &mut ProbedRows, _max_rows: usize) -> Result<()> {
        Ok(())
    }
}

pub struct AllUnmatchedProbeStream {
    idx: u64,
    size: u64,
}

impl AllUnmatchedProbeStream {
    pub fn create(size: usize) -> Box<dyn ProbeStream> {
        Box::new(AllUnmatchedProbeStream {
            idx: 0,
            size: size as u64,
        })
    }
}

impl ProbeStream for AllUnmatchedProbeStream {
    fn advance(&mut self, rows: &mut ProbedRows, max_rows: usize) -> Result<()> {
        if self.idx >= self.size {
            return Ok(());
        }

        let unmatched_rows = std::cmp::min(self.size - self.idx, max_rows as u64);
        rows.unmatched.extend(self.idx..self.idx + unmatched_rows);
        self.idx += unmatched_rows;
        Ok(())
    }
}
