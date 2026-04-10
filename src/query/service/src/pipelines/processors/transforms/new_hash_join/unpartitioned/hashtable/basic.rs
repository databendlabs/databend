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

use crate::pipelines::processors::transforms::new_hash_join::common::probe_stream::ProbeStream;
use crate::pipelines::processors::transforms::new_hash_join::common::probe_stream::ProbedRows;

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
