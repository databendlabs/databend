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

use std::collections::HashMap;

use databend_common_catalog::plan::PartInfoPtr;
use databend_common_expression::ColumnId;
use databend_common_expression::Scalar;
use databend_common_storages_fuse::FuseBlockPartInfo;
use databend_common_storages_fuse::FuseLazyPartInfo;
use databend_common_storages_fuse::operations::expected_decoded_bytes;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::Compression;

/// A block part of `rows` rows whose columns decode to the given sizes; `None` for a part
/// without column statistics.
fn block(rows: u64, sizes: Option<&[(ColumnId, u64)]>) -> PartInfoPtr {
    let stats = sizes.map(|sizes| {
        sizes
            .iter()
            .map(|(id, size)| {
                (
                    *id,
                    ColumnStatistics::new(Scalar::Null, Scalar::Null, 0, *size, None),
                )
            })
            .collect::<HashMap<_, _>>()
    });
    FuseBlockPartInfo::create(
        format!("blocks/{rows}"),
        None,
        0,
        rows,
        HashMap::new(),
        stats,
        Compression::Zstd,
        None,
        None,
        None,
    )
}

#[test]
fn sums_the_columns_asked_for_over_the_blocks() {
    let parts = vec![
        block(1000, Some(&[(0, 8_000), (1, 4_000), (2, 50_000)])),
        block(500, Some(&[(0, 4_000), (1, 2_000), (2, 25_000)])),
    ];
    assert_eq!(expected_decoded_bytes(&parts, &[0, 1]), 18_000);
    assert_eq!(expected_decoded_bytes(&parts, &[2]), 75_000);
    assert_eq!(expected_decoded_bytes(&parts, &[]), 0);
    assert_eq!(expected_decoded_bytes(&[], &[0, 1]), 0);
}

/// Parts that are not blocks with statistics add nothing: segments still to be pruned, blocks
/// without statistics, columns a block has no statistics for. The result is a lower bound.
#[test]
fn parts_without_statistics_add_nothing() {
    let parts = vec![
        FuseLazyPartInfo::create(0, ("segments/0".to_string(), 1)),
        block(1000, None),
        block(1000, Some(&[(0, 8_000)])),
    ];
    assert_eq!(expected_decoded_bytes(&parts, &[0, 1]), 8_000);
    assert_eq!(expected_decoded_bytes(&parts[..2], &[0, 1]), 0);
}
