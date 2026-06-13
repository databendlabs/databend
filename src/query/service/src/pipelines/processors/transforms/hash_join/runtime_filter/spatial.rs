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
use geo_index::rtree::RTreeBuilder;
use geo_index::rtree::RTreeIndex;
use geo_index::rtree::RTreeRef;
use geo_index::rtree::sort::HilbertSort;

fn build_rtree_from_boxes(boxes: &[f64]) -> Result<Vec<u8>> {
    if boxes.is_empty() {
        return Ok(Vec::new());
    }
    if boxes.len() % 4 != 0 {
        return Err(ErrorCode::Internal(
            "Invalid spatial boxes length".to_string(),
        ));
    }
    let num_items = boxes.len() / 4;
    let num_items = u32::try_from(num_items)
        .map_err(|_| ErrorCode::Internal("Spatial runtime filter is too large".to_string()))?;
    let mut builder = RTreeBuilder::<f64>::new(num_items);
    for chunk in boxes.chunks_exact(4) {
        builder.add(chunk[0], chunk[1], chunk[2], chunk[3]);
    }
    Ok(builder.finish::<HilbertSort>().into_inner())
}

// If the rtree contains too many values, merge them to reduce the size of rtree.
pub fn compact_rtree_to_threshold(tree_bytes: &[u8], max_items: usize) -> Result<Vec<u8>> {
    if tree_bytes.is_empty() || max_items == 0 {
        return Ok(Vec::new());
    }
    let tree = RTreeRef::<f64>::try_new(&tree_bytes)
        .map_err(|e| ErrorCode::Internal(format!("Invalid spatial index: {e}")))?;
    if tree.num_items() as usize <= max_items {
        return Ok(tree_bytes.to_vec());
    }
    let mut chosen_level = tree.num_levels().saturating_sub(1);
    for level in 0..tree.num_levels() {
        let boxes = tree
            .boxes_at_level(level)
            .map_err(|e| ErrorCode::Internal(format!("Invalid spatial index: {e}")))?;
        let count = boxes.len() / 4;
        if count <= max_items {
            chosen_level = level;
            break;
        }
    }
    let boxes = tree
        .boxes_at_level(chosen_level)
        .map_err(|e| ErrorCode::Internal(format!("Invalid spatial index: {e}")))?;
    build_rtree_from_boxes(boxes)
}

pub fn build_rtree_from_rects_with_threshold<I>(
    rects: I,
    rect_count: usize,
    max_items: usize,
) -> Result<Vec<u8>>
where
    I: IntoIterator<Item = (f64, f64, f64, f64)>,
{
    if rect_count == 0 || max_items == 0 {
        return Ok(Vec::new());
    }
    let num_items = u32::try_from(rect_count)
        .map_err(|_| ErrorCode::Internal("Spatial runtime filter is too large".to_string()))?;
    let mut builder = RTreeBuilder::<f64>::new(num_items);
    for (min_x, min_y, max_x, max_y) in rects {
        builder.add(min_x, min_y, max_x, max_y);
    }
    let tree = builder.finish::<HilbertSort>().into_inner();
    if rect_count > max_items {
        compact_rtree_to_threshold(&tree, max_items)
    } else {
        Ok(tree)
    }
}

// Merge multiple rtrees into one. If the merged values exceeds the threshold,
// perform a compact operation.
pub fn merge_rtrees_to_threshold<'a, I>(rtrees: I, max_items: usize) -> Result<Vec<u8>>
where I: IntoIterator<Item = &'a [u8]> {
    if max_items == 0 {
        return Ok(Vec::new());
    }

    let mut sources = Vec::new();
    let mut total_items = 0usize;
    for bytes in rtrees {
        if bytes.is_empty() {
            continue;
        }
        let tree = RTreeRef::<f64>::try_new(&bytes)
            .map_err(|e| ErrorCode::Internal(format!("Invalid spatial index: {e}")))?;
        let num_items = tree.num_items() as usize;
        if num_items == 0 {
            continue;
        }
        total_items = total_items.saturating_add(num_items);
        sources.push(bytes);
    }

    if total_items == 0 {
        return Ok(Vec::new());
    }

    let total_items_u32 = u32::try_from(total_items)
        .map_err(|_| ErrorCode::Internal("Spatial runtime filter is too large".to_string()))?;
    let mut builder = RTreeBuilder::<f64>::new(total_items_u32);
    for bytes in sources {
        let tree = RTreeRef::<f64>::try_new(&bytes)
            .map_err(|e| ErrorCode::Internal(format!("Invalid spatial index: {e}")))?;
        let boxes = tree
            .boxes_at_level(0)
            .map_err(|e| ErrorCode::Internal(format!("Invalid spatial index: {e}")))?;
        for chunk in boxes.chunks_exact(4) {
            builder.add(chunk[0], chunk[1], chunk[2], chunk[3]);
        }
    }
    let merged = builder.finish::<HilbertSort>().into_inner();
    if total_items > max_items {
        compact_rtree_to_threshold(&merged, max_items)
    } else {
        Ok(merged)
    }
}

pub fn rtree_bounds_from_bytes(tree_bytes: &[u8]) -> Result<Option<[f64; 4]>> {
    if tree_bytes.is_empty() {
        return Ok(None);
    }
    let tree = RTreeRef::<f64>::try_new(&tree_bytes)
        .map_err(|e| ErrorCode::Internal(format!("Invalid spatial index: {e}")))?;
    if tree.num_items() == 0 {
        return Ok(None);
    }
    let level = tree.num_levels().saturating_sub(1);
    let boxes = tree
        .boxes_at_level(level)
        .map_err(|e| ErrorCode::Internal(format!("Invalid spatial index: {e}")))?;
    if boxes.len() < 4 {
        return Ok(None);
    }
    let mut min_x = boxes[0];
    let mut min_y = boxes[1];
    let mut max_x = boxes[2];
    let mut max_y = boxes[3];
    for chunk in boxes.chunks_exact(4).skip(1) {
        min_x = min_x.min(chunk[0]);
        min_y = min_y.min(chunk[1]);
        max_x = max_x.max(chunk[2]);
        max_y = max_y.max(chunk[3]);
    }
    Ok(Some([min_x, min_y, max_x, max_y]))
}
