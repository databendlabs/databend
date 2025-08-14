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

use databend_common_column::binview::Utf8ViewColumn;
use databend_common_column::binview::View;
use databend_common_column::buffer::Buffer;
use databend_common_exception::ErrorCode;
use databend_common_expression::Column;
use parquet::encodings::rle::RleDecoder;
use parquet2::encoding::Encoding;
use parquet2::page::Page;
use parquet2::schema::types::PhysicalType;

use crate::reader::decompressor::Decompressor;

pub struct StringIter<'a> {
    /// Page decompressor for reading Parquet pages
    pages: Decompressor<'a>,
    /// Optional chunk size for batched processing
    chunk_size: Option<usize>,
    /// Total number of rows to process
    num_rows: usize,
    /// Dictionary entries
    dictionary: Option<Vec<Vec<u8>>>,
    // Cached dictionary views
    cached_dict_views: Option<Vec<View>>,
    cached_dict_lengths: Option<Vec<u8>>,
    // Scratch buffer for rle decoding
    rle_index_buffer: Option<Vec<i32>>,
}

impl<'a> StringIter<'a> {
    pub fn new(pages: Decompressor<'a>, num_rows: usize, chunk_size: Option<usize>) -> Self {
        Self {
            pages,
            chunk_size,
            num_rows,
            dictionary: None,
            cached_dict_views: None,
            cached_dict_lengths: None,
            rle_index_buffer: None,
        }
    }

    /// Process a dictionary page and store the dictionary entries
    fn process_dictionary_page(
        &mut self,
        dict_page: &parquet2::page::DictPage,
    ) -> Result<(), ErrorCode> {
        assert!(self.dictionary.is_none());
        let mut dict_values = Vec::new();
        let mut offset = 0;
        let buffer = &dict_page.buffer;

        while offset < buffer.len() {
            if offset + 4 > buffer.len() {
                return Err(ErrorCode::Internal(
                    "Invalid dictionary page: incomplete length prefix".to_string(),
                ));
            }

            let length = u32::from_le_bytes([
                buffer[offset],
                buffer[offset + 1],
                buffer[offset + 2],
                buffer[offset + 3],
            ]) as usize;
            offset += 4;

            if offset + length > buffer.len() {
                return Err(ErrorCode::Internal(
                    "Invalid dictionary page: string length exceeds buffer".to_string(),
                ));
            }

            dict_values.push(buffer[offset..offset + length].to_vec());
            offset += length;
        }

        self.dictionary = Some(dict_values);
        // Clear cached views when dictionary changes
        self.cached_dict_views = None;
        Ok(())
    }

    /// Create a View from a string slice, handling both inline and buffer storage
    fn create_view_from_string(
        string_data: &[u8],
        page_bytes: &mut Vec<u8>,
        page_offset: &mut usize,
        buffer_index: u32,
    ) -> View {
        let len = string_data.len() as u32;
        if len <= 12 {
            // Inline small strings directly in the View
            unsafe {
                let mut payload = [0u8; 16];
                payload
                    .as_mut_ptr()
                    .cast::<u32>()
                    .write_unaligned(len.to_le());
                std::ptr::copy_nonoverlapping(
                    string_data.as_ptr(),
                    payload.as_mut_ptr().add(4),
                    len as usize,
                );
                std::mem::transmute::<[u8; 16], View>(payload)
            }
        } else {
            // Store large strings in buffer and reference them
            let current_offset = *page_offset;
            // TODO use memcpy
            page_bytes.extend_from_slice(string_data);
            *page_offset += string_data.len();

            unsafe {
                let mut payload = [0u8; 16];
                // Length
                payload
                    .as_mut_ptr()
                    .cast::<u32>()
                    .write_unaligned(len.to_le());
                // Prefix (first 4 bytes of string)
                let prefix_len = std::cmp::min(4, string_data.len());
                std::ptr::copy_nonoverlapping(
                    string_data.as_ptr(),
                    payload.as_mut_ptr().add(4),
                    prefix_len,
                );
                // Buffer index
                payload
                    .as_mut_ptr()
                    .add(8)
                    .cast::<u32>()
                    .write_unaligned(buffer_index.to_le());
                // Offset in buffer
                payload
                    .as_mut_ptr()
                    .add(12)
                    .cast::<u32>()
                    .write_unaligned((current_offset as u32).to_le());

                std::mem::transmute::<[u8; 16], View>(payload)
            }
        }
    }

    /// Process plain encoded data page
    fn process_plain_encoding(
        &self,
        values_buffer: &[u8],
        remaining: usize,
        views: &mut Vec<View>,
        buffers: &mut Vec<Buffer<u8>>,
        total_bytes_len: &mut usize,
    ) -> Result<(), ErrorCode> {
        let mut offset = 0;
        let estimated_capacity = values_buffer.len();
        let mut page_bytes = Vec::with_capacity(estimated_capacity);
        let mut page_offset = 0;
        let buffer_index = buffers.len() as u32;

        for _ in 0..remaining {
            if offset + 4 > values_buffer.len() {
                return Err(ErrorCode::Internal(
                    "Invalid plain encoding: incomplete length prefix".to_string(),
                ));
            }

            let length = u32::from_le_bytes([
                values_buffer[offset],
                values_buffer[offset + 1],
                values_buffer[offset + 2],
                values_buffer[offset + 3],
            ]) as usize;
            offset += 4;

            if offset + length > values_buffer.len() {
                return Err(ErrorCode::Internal(
                    "Invalid plain encoding: string length exceeds buffer".to_string(),
                ));
            }

            let string_data = &values_buffer[offset..offset + length];
            let view = Self::create_view_from_string(
                string_data,
                &mut page_bytes,
                &mut page_offset,
                buffer_index,
            );
            views.push(view);
            *total_bytes_len += length;
            offset += length;
        }

        if !page_bytes.is_empty() {
            buffers.push(Buffer::from(page_bytes));
        }

        Ok(())
    }

    /// Process RLE dictionary encoded data page with optimized paths for different scenarios.
    fn process_rle_dictionary_encoding(
        &mut self,
        values_buffer: &[u8],
        remaining: usize,
        views: &mut Vec<View>,
        buffers: &mut Vec<Buffer<u8>>,
        total_bytes_len: &mut usize,
    ) -> Result<(), ErrorCode> {
        if values_buffer.is_empty() {
            return Err(ErrorCode::Internal("Empty RLE dictionary data".to_string()));
        }

        let bit_width = values_buffer[0];

        // Clone dictionary to avoid borrowing issues
        if let Some(dict) = self.dictionary.clone() {
            // Check if we can use the optimized small string fast path
            if self.can_use_small_string_fast_path(&dict) {
                return self.process_small_string_fast_path(
                    &dict,
                    values_buffer,
                    bit_width,
                    remaining,
                    views,
                    total_bytes_len,
                );
            }
        }

        // General path for large dictionaries or mixed string sizes
        self.process_general_rle_path(
            values_buffer,
            bit_width,
            remaining,
            views,
            buffers,
            total_bytes_len,
        )
    }

    /// Check if dictionary qualifies for small string fast path optimization.
    fn can_use_small_string_fast_path(&self, dict: &[Vec<u8>]) -> bool {
        dict.len() <= 16 && dict.iter().all(|s| s.len() <= 12)
    }

    /// Process RLE dictionary encoding using the optimized small string fast path.
    fn process_small_string_fast_path(
        &mut self,
        dict: &[Vec<u8>],
        values_buffer: &[u8],
        bit_width: u8,
        remaining: usize,
        views: &mut Vec<View>,
        total_bytes_len: &mut usize,
    ) -> Result<(), ErrorCode> {
        views.reserve_exact(remaining);

        if bit_width == 0 {
            // Special case: all indices are 0, repeat dictionary[0]
            return self.process_bit_width_zero(dict, remaining, views, total_bytes_len);
        }

        // General small string case with RLE decoding
        self.process_small_string_rle(
            dict,
            values_buffer,
            bit_width,
            remaining,
            views,
            total_bytes_len,
        )
    }

    /// Handle the special case where bit_width=0 (all values are dictionary[0]).
    fn process_bit_width_zero(
        &self,
        dict: &[Vec<u8>],
        remaining: usize,
        views: &mut Vec<View>,
        total_bytes_len: &mut usize,
    ) -> Result<(), ErrorCode> {
        if dict.is_empty() {
            return Err(ErrorCode::Internal(
                "Empty dictionary for RLE dictionary encoding".to_string(),
            ));
        }

        let dict_entry = &dict[0];
        let inline_view = Self::create_inline_view(dict_entry);

        // TODO: Use slice::fill when available for better performance
        for _ in 0..remaining {
            views.push(inline_view);
            *total_bytes_len += dict_entry.len();
        }

        Ok(())
    }

    /// Process small string RLE decoding with cached dictionary views.
    fn process_small_string_rle(
        &mut self,
        dict: &[Vec<u8>],
        values_buffer: &[u8],
        bit_width: u8,
        remaining: usize,
        views: &mut Vec<View>,
        total_bytes_len: &mut usize,
    ) -> Result<(), ErrorCode> {
        // Create RLE decoder
        let mut rle_decoder = RleDecoder::new(bit_width);
        rle_decoder.set_data(bytes::Bytes::copy_from_slice(&values_buffer[1..]));

        // Ensure dictionary views are cached
        // TODO any better way?
        self.ensure_dict_views_cached(dict);
        let dict_views = self.cached_dict_views.as_ref().unwrap();

        // Decode indices and populate views in single pass
        let start_len = views.len();
        // TODO hotspot
        // let mut indices = vec![0i32; remaining];

        let indices: &mut Vec<i32> = if let Some(indices) = self.rle_index_buffer.as_mut() {
            if indices.capacity() < remaining {
                indices.reserve_exact(remaining - indices.capacity());
            }
            indices
        } else {
            let indices: Vec<i32> = Vec::with_capacity(remaining);
            self.rle_index_buffer = Some(indices);
            self.rle_index_buffer.as_mut().unwrap()
        };
        unsafe {
            indices.set_len(remaining);
        }

        let decoded_count = rle_decoder
            .get_batch(indices)
            .map_err(|e| ErrorCode::Internal(format!("Failed to decode RLE indices: {}", e)))?;
        if decoded_count != remaining {
            return Err(ErrorCode::Internal(format!(
                "RleDecoder returned wrong count: expected={}, got={}",
                remaining, decoded_count
            )));
        }

        let mut local_bytes_len = 0usize;
        // Single pass: populate views and calculate total_bytes_len simultaneously
        unsafe {
            let views_ptr = views.as_mut_ptr().add(start_len);
            let dict_views_len = dict_views.len();

            let dict_views_ptr = dict_views.as_ptr();
            let dict_lengths_ptr = self.cached_dict_lengths.as_ref().unwrap().as_ptr();

            let pairs = indices.chunks_exact(2);
            let remainder = pairs.remainder();

            let mut i = 0;

            // Process pairs of elements
            for pair in pairs {
                let dict_idx1 = pair[0] as usize;
                let dict_idx2 = pair[1] as usize;

                debug_assert!(dict_idx1 < dict_views_len);
                debug_assert!(dict_idx2 < dict_views_len);

                // Process two elements in one iteration
                *views_ptr.add(i) = *dict_views_ptr.add(dict_idx1);
                *views_ptr.add(i + 1) = *dict_views_ptr.add(dict_idx2);

                local_bytes_len += *dict_lengths_ptr.add(dict_idx1) as usize;
                local_bytes_len += *dict_lengths_ptr.add(dict_idx2) as usize;

                i += 2;
            }

            // Process remaining single element if any
            if let [index] = remainder {
                let dict_idx = *index as usize;
                debug_assert!(dict_idx < dict_views_len);

                *views_ptr.add(i) = *dict_views_ptr.add(dict_idx);
                local_bytes_len += *dict_lengths_ptr.add(dict_idx) as usize;
            }
            // TODO Make sure this is panic safe
            views.set_len(start_len + remaining);
        }

        *total_bytes_len += local_bytes_len;

        Ok(())
    }

    /// Ensure dictionary views are cached for the current dictionary.
    fn ensure_dict_views_cached(&mut self, dict: &[Vec<u8>]) {
        if self.cached_dict_views.is_none() {
            self.cached_dict_views = Some(
                dict.iter()
                    .map(|s| Self::create_inline_view(s))
                    .collect::<Vec<_>>(),
            );

            // Working on small strings, u8 is enough for lengths
            let lengths: Vec<u8> = dict.iter().map(|s| s.len() as u8).collect();
            self.cached_dict_lengths = Some(lengths);
        }
    }

    /// Process RLE dictionary encoding using the general path for large dictionaries.
    fn process_general_rle_path(
        &mut self,
        values_buffer: &[u8],
        bit_width: u8,
        remaining: usize,
        views: &mut Vec<View>,
        buffers: &mut Vec<Buffer<u8>>,
        total_bytes_len: &mut usize,
    ) -> Result<(), ErrorCode> {
        // Create new RleDecoder for general path
        let mut rle_decoder = RleDecoder::new(bit_width);
        rle_decoder.set_data(bytes::Bytes::copy_from_slice(&values_buffer[1..]));

        if let Some(ref dict) = self.dictionary {
            // Initialize buffer management variables for general case
            let mut page_bytes = Vec::new();
            let mut page_offset = 0;
            let buffer_index = buffers.len() as u32;

            // Decode indices and process each one
            let mut indices = vec![0i32; remaining];
            let decoded_count = rle_decoder
                .get_batch(&mut indices)
                .map_err(|e| ErrorCode::Internal(format!("Failed to decode RLE indices: {}", e)))?;

            if decoded_count != remaining {
                return Err(ErrorCode::Internal(format!(
                    "RleDecoder returned wrong count: expected={}, got={}",
                    remaining, decoded_count
                )));
            }

            // Process each index and create views
            for &index in &indices {
                let dict_idx = index as usize;
                if dict_idx >= dict.len() {
                    return Err(ErrorCode::Internal(format!(
                        "Dictionary index {} out of bounds (dictionary size: {})",
                        dict_idx,
                        dict.len()
                    )));
                }

                let string_data = &dict[dict_idx];
                let view = Self::create_view_from_string(
                    string_data,
                    &mut page_bytes,
                    &mut page_offset,
                    buffer_index,
                );
                views.push(view);
                *total_bytes_len += string_data.len();
            }

            // Add buffer if any data was written
            if !page_bytes.is_empty() {
                buffers.push(Buffer::from(page_bytes));
            }
        } else {
            return Err(ErrorCode::Internal(
                "No dictionary found for RLE dictionary encoding".to_string(),
            ));
        }

        Ok(())
    }

    /// Create an inline View for small strings (≤12 bytes) with maximum performance.
    fn create_inline_view(string_data: &[u8]) -> View {
        debug_assert!(
            string_data.len() <= 12,
            "create_inline_view called with string longer than 12 bytes"
        );

        unsafe {
            let mut payload = [0u8; 16];
            let len = string_data.len() as u32;

            // Write length prefix (little-endian)
            payload
                .as_mut_ptr()
                .cast::<u32>()
                .write_unaligned(len.to_le());

            // Copy string data directly
            std::ptr::copy_nonoverlapping(
                string_data.as_ptr(),
                payload.as_mut_ptr().add(4),
                len as usize,
            );

            // Convert to View with zero cost
            std::mem::transmute::<[u8; 16], View>(payload)
        }
    }

    /// Process a data page based on its encoding type.
    fn process_data_page(
        &mut self,
        data_page: &parquet2::page::DataPage,
        views: &mut Vec<View>,
        buffers: &mut Vec<Buffer<u8>>,
        total_bytes_len: &mut usize,
    ) -> Result<(), ErrorCode> {
        let (_, _, values_buffer) = parquet2::page::split_buffer(data_page)
            .map_err(|e| ErrorCode::StorageOther(format!("Failed to split buffer: {}", e)))?;
        let remaining = data_page.num_values();

        match data_page.encoding() {
            Encoding::Plain => self.process_plain_encoding(
                values_buffer,
                remaining,
                views,
                buffers,
                total_bytes_len,
            ),
            Encoding::RleDictionary | Encoding::PlainDictionary => self
                .process_rle_dictionary_encoding(
                    values_buffer,
                    remaining,
                    views,
                    buffers,
                    total_bytes_len,
                ),
            _ => Err(ErrorCode::Internal(format!(
                "Unsupported encoding for string column: {:?}",
                data_page.encoding()
            ))),
        }
    }
}

impl<'a> Iterator for StringIter<'a> {
    type Item = Result<Column, ErrorCode>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.num_rows == 0 {
            return None;
        }

        // let chunk_size = self.chunk_size.unwrap_or(self.num_rows);
        // let limit = std::cmp::min(chunk_size, self.num_rows);
        let limit = self.chunk_size.unwrap_or(self.num_rows);

        let mut views = Vec::with_capacity(limit);
        let mut buffers = Vec::new();
        let mut total_bytes_len = 0;
        let mut processed_rows = 0;

        while processed_rows < limit {
            let page = match self.pages.next_owned() {
                Ok(Some(page)) => page,
                Ok(None) => break,
                Err(e) => return Some(Err(ErrorCode::StorageOther(e.to_string()))),
            };

            match page {
                Page::Data(data_page) => {
                    if data_page.descriptor.primitive_type.physical_type != PhysicalType::ByteArray
                    {
                        return Some(Err(ErrorCode::Internal(
                            "Expected ByteArray type for string column".to_string(),
                        )));
                    }

                    let remaining_in_chunk = limit - processed_rows;
                    let page_rows = std::cmp::min(data_page.num_values(), remaining_in_chunk);

                    if let Err(e) = self.process_data_page(
                        &data_page,
                        &mut views,
                        &mut buffers,
                        &mut total_bytes_len,
                    ) {
                        return Some(Err(e));
                    }

                    processed_rows += page_rows;
                }
                Page::Dict(dict_page) => {
                    if let Err(e) = self.process_dictionary_page(&dict_page) {
                        return Some(Err(e));
                    }
                }
            }
        }

        if processed_rows == 0 {
            return None;
        }

        self.num_rows -= processed_rows;

        // Calculate total buffer length for new_unchecked
        let total_buffer_len = buffers.iter().map(|b| b.len()).sum();

        let column = Utf8ViewColumn::new_unchecked(
            views.into(),
            buffers.into(),
            total_bytes_len,
            total_buffer_len,
        );

        Some(Ok(Column::String(column)))
    }
}
