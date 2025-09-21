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

use display_more::DisplaySliceExt;

use crate::leveled_store::immutable_levels::ImmutableLevels;

impl ImmutableLevels {
    /// Determine if the layout needs compaction based on the total size of all levels.
    #[allow(dead_code)]
    pub(crate) fn need_compact(&self) -> Result<(), String> {
        let min_size = self.newest_to_oldest().map(|l| l.size()).min().unwrap_or(0);
        let max_size = self.newest_to_oldest().map(|l| l.size()).max().unwrap_or(0);

        let n_levels = self.immutables.len() as u64;

        if n_levels < 6 {
            return Err("not enough levels(<6) to compact".to_string());
        }

        if n_levels > 20 {
            return Ok(());
        }

        if max_size <= min_size * 4 {
            return Err(format!(
                "size difference between levels(min: {}, max: {}) is not large enough, stat: {}",
                min_size,
                max_size,
                self.stat().display()
            ));
        }

        Ok(())
    }
}
