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

use bytes::Bytes;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::types::F32;
use databend_common_expression::types::VectorColumn;
use databend_storages_common_index::VectorBundleBuilder;
use databend_storages_common_index::VectorBundleFooter;
use databend_storages_common_index::VectorBundlePageRead;
use databend_storages_common_index::vector_fp16_data_name;

const ROWS: u64 = 100;
const DIM: usize = 4;

fn read(bundle: &[u8], read: VectorBundlePageRead) -> Result<Bytes> {
    let range = read.range()?;
    read.decode(Bytes::copy_from_slice(
        &bundle[range.start as usize..range.end as usize],
    ))
}

/// The reads of all FP16 rows, by page, join into the whole component, checked against its
/// size and checksum; reads that miss rows or hold other bytes are rejected.
#[test]
fn test_join_rows() -> Result<()> {
    let values = (0..ROWS as usize * DIM)
        .map(|i| F32::from(i as f32 * 0.25))
        .collect::<Vec<_>>();
    let column = Column::Vector(VectorColumn::Float32((values.into(), DIM)));
    let mut builder = VectorBundleBuilder::new("vi".to_owned(), "v1".to_owned(), ROWS);
    builder.add_fp16_data(0, &column)?;
    let bundle = builder.finish()?;
    let footer = VectorBundleFooter::from_bundle(&bundle)?;
    let fp16 = &footer.components[&vector_fp16_data_name(0)];

    let whole = read(&bundle, fp16.component_read())?;
    let pages = [0..30, 30..64, 64..ROWS]
        .into_iter()
        .map(|rows| read(&bundle, fp16.row_range_read(rows, ROWS)?))
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(fp16.join_rows(pages.clone())?, whole);
    assert_eq!(fp16.join_rows(vec![whole.clone()])?, whole);
    assert!(fp16.join_rows(pages[..2].to_vec()).is_err());

    let mut corrupt = pages[1].to_vec();
    corrupt[0] ^= 1;
    let corrupt = vec![pages[0].clone(), Bytes::from(corrupt), pages[2].clone()];
    assert!(fp16.join_rows(corrupt).is_err());
    Ok(())
}
