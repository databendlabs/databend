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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::registry::Pool;

const MAX_TOTAL_RATIO: u64 = 80;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PoolCaps {
    pub meta: usize,
    pub index: usize,
    pub data: usize,
}

impl PoolCaps {
    pub fn from_ratios(memory_limit: usize, meta: u64, index: u64, data: u64) -> Result<Self> {
        let total = meta + index + data;
        if total > MAX_TOTAL_RATIO {
            return Err(ErrorCode::InvalidConfig(format!(
                "cache pool ratios sum to {total}%, must be at most {MAX_TOTAL_RATIO}%"
            )));
        }
        let of = |ratio: u64| (memory_limit as u128 * ratio as u128 / 100) as usize;
        Ok(PoolCaps {
            meta: of(meta),
            index: of(index),
            data: of(data),
        })
    }

    pub fn get(&self, pool: Pool) -> usize {
        match pool {
            Pool::Meta => self.meta,
            Pool::Index => self.index,
            Pool::Data => self.data,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_ratios() {
        let caps = PoolCaps::from_ratios(1000, 10, 40, 10).unwrap();
        assert_eq!(caps, PoolCaps {
            meta: 100,
            index: 400,
            data: 100
        });
        assert!(PoolCaps::from_ratios(1000, 30, 40, 11).is_err());
    }
}
