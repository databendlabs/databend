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

use std::fmt;
use std::sync::Arc;

use display_more::DisplaySliceExt;
use log::info;
use seq_marked::InternalSeq;

use crate::leveled_store::leveled_map::LeveledMap;
use crate::sm_v003::SMV003;
use crate::sm_v003::compactor_acquirer::CompactorPermit;
use crate::sm_v003::writer_acquirer::WriterPermit;

/// Compact `ImmutableLevels` to reduce the number of levels.
pub struct InMemoryCompactor {
    writer_permit: WriterPermit,
    immutable_compactor: ImmutableCompactor,
}

impl fmt::Display for InMemoryCompactor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "InMemoryCompactor({})", self.name())
    }
}

impl InMemoryCompactor {
    pub async fn new(sm: Arc<SMV003>, name: impl ToString) -> Self {
        let name = name.to_string();

        // Always acquire the compactor permit first to avoid deadlock.
        let compactor_permit = sm.new_compactor_acquirer(name.clone()).acquire().await;
        let writer_permit = sm.new_writer_acquirer().acquire().await;

        Self {
            writer_permit,
            immutable_compactor: ImmutableCompactor::new(
                sm.leveled_map().clone(),
                compactor_permit,
                name,
            ),
        }
    }

    /// Move the writable level to immutable levels,
    /// and return the `ImmutableCompactor` to do the further compaction of immutable levels.
    pub fn freeze(mut self) -> ImmutableCompactor {
        let leveled_map = self.leveled_map().clone();

        let writable_stat = leveled_map.writable_stat();

        leveled_map.freeze_writable(
            &mut self.writer_permit,
            &mut self.immutable_compactor.compactor_permit,
        );

        let immutable_stat = leveled_map.immutable_levels().stat();

        info!(
            "{} DONE freeze writable: {}; immutables: {}",
            self,
            writable_stat,
            immutable_stat.display_n(64)
        );

        self.immutable_compactor
    }

    pub fn leveled_map(&self) -> &LeveledMap {
        &self.immutable_compactor.leveled_map
    }

    pub(crate) fn name(&self) -> &str {
        &self.immutable_compactor.name
    }
}

pub struct ImmutableCompactor {
    name: String,
    compactor_permit: CompactorPermit,
    leveled_map: LeveledMap,
}

impl fmt::Display for ImmutableCompactor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ImmutableCompactor({})", self.name())
    }
}

impl ImmutableCompactor {
    pub fn new(
        leveled_map: LeveledMap,
        compactor_permit: CompactorPermit,
        name: impl ToString,
    ) -> Self {
        Self {
            name: name.to_string(),
            leveled_map,
            compactor_permit,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn compact(self, min_snapshot_seq: InternalSeq) {
        //
        let immutable_levels = self.leveled_map.immutable_levels();

        let old_stat = immutable_levels.stat();

        info!(
            "{} Start compact immutables: {}",
            self,
            old_stat.display_n(64)
        );

        if let Err(e) = immutable_levels.need_compact() {
            info!("{} Skip compact immutables because: {}", self, e);
            return;
        }

        let new_immutable_levels = immutable_levels.compact_min_adjacent(min_snapshot_seq);

        let old_stat = immutable_levels.stat();
        let new_stat = new_immutable_levels.stat();

        if old_stat == new_stat {
            info!(
                "{} Done compact immutables: No compaction: {}",
                self,
                old_stat.display_n(64),
            );
            return;
        } else {
            for i in 0..new_stat.len() {
                let old = &old_stat[i];
                let new = &new_stat[i];

                if old.level_index != new.level_index {
                    info!(
                        "{} Done compact {} = {} + {}, final: {}",
                        self,
                        &new_stat[i - 1],
                        &old_stat[i - 1],
                        &old_stat[i],
                        new_stat.display_n(64)
                    );
                    break;
                }
            }
        }

        self.leveled_map
            .replace_immutable_levels(new_immutable_levels);
    }
}
