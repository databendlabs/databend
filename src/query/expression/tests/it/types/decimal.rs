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

use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::OwnedMemoryUsageSize;
use databend_common_base::runtime::ThreadTracker;
use databend_common_expression::types::decimal::DecimalColumn;
use databend_common_expression::types::DecimalSize;
use ethnum::i256;

#[test]
fn test_decimal_128_column_owned_memory_usage() {
    for len in 0..100 {
        let mem_stat = MemStat::create("TEST".to_string());
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.mem_stat = Some(mem_stat.clone());

        let _guard = ThreadTracker::tracking(payload);
        let mut column = DecimalColumn::Decimal128(Buffer::from(vec![0_i128; len]), DecimalSize {
            scale: 0,
            precision: 0,
        });

        drop(_guard);
        assert_ne!(mem_stat.get_memory_usage(), 0);
        assert_eq!(
            mem_stat.get_memory_usage(),
            column.owned_memory_usage() as i64
        );
    }
}

#[test]
fn test_decimal_256_column_owned_memory_usage() {
    for len in 0..100 {
        let mem_stat = MemStat::create("TEST".to_string());
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.mem_stat = Some(mem_stat.clone());

        let _guard = ThreadTracker::tracking(payload);
        let mut column =
            DecimalColumn::Decimal256(Buffer::from(vec![i256::new(0_i128); len]), DecimalSize {
                scale: 0,
                precision: 0,
            });

        drop(_guard);
        assert_ne!(mem_stat.get_memory_usage(), 0);
        assert_eq!(
            mem_stat.get_memory_usage(),
            column.owned_memory_usage() as i64
        );
    }
}
