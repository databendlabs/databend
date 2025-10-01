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
use databend_common_hashtable::RowPtr;

pub struct PerformanceStatistics {
    matched_hash: usize,
}

pub struct ProbedRows {
    pub unmatched: Vec<usize>,
    pub matched_probe: Vec<u64>,
    pub matched_build: Vec<RowPtr>,

    // pub performance_statistics: PerformanceStatistics,
}

impl ProbedRows {
    pub fn empty() -> ProbedRows {
        ProbedRows::new(vec![], vec![], vec![])
    }

    pub fn new(
        unmatched: Vec<usize>,
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

    pub fn all_unmatched(unmatched: Vec<usize>) -> ProbedRows {
        ProbedRows::new(unmatched, vec![], vec![])
    }
}

pub trait ProbeStream {
    fn next(&mut self, max_rows: usize) -> Result<ProbedRows>;

    fn both(&mut self, rows: &mut ProbedRows, max_rows: usize) -> Result<()> {
        unimplemented!()
    }

    fn matched(&mut self, res: &mut ProbedRows, max_rows: usize) -> Result<()> {
        unimplemented!()
    }
}

pub struct AllUnmatchedProbeStream {
    idx: usize,
    size: usize,
}

impl AllUnmatchedProbeStream {
    pub fn create(size: usize) -> Box<dyn ProbeStream> {
        Box::new(AllUnmatchedProbeStream { idx: 0, size })
    }
}

impl ProbeStream for AllUnmatchedProbeStream {
    fn next(&mut self, max_rows: usize) -> Result<ProbedRows> {
        if self.idx >= self.size {
            return Ok(ProbedRows::empty());
        }

        let res = std::cmp::min(self.size - self.idx, max_rows);
        let res = (self.idx..self.idx + res).collect::<Vec<_>>();
        self.idx += res.len();
        Ok(ProbedRows::all_unmatched(res))
    }

    fn matched(&mut self, res: &mut ProbedRows, max_rows: usize) -> Result<()> {
        if self.idx >= self.size {
            return Ok(());
        }

        let unmatched_rows = std::cmp::min(self.size - self.idx, max_rows);
        res.unmatched.extend(self.idx..self.idx + unmatched_rows);
        self.idx += unmatched_rows;
        Ok(())
    }
}
