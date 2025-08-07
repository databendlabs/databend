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
use databend_common_column::types::NativeType;
use databend_common_exception::ErrorCode;
use databend_common_expression::Column;
use parquet2::encoding::Encoding;
use parquet2::page::Page;
use parquet2::schema::types::PhysicalType;
use parquet2::FallibleStreamingIterator;

use crate::reader::decompressor::Decompressor;

pub struct StringIter<'a> {
    pages: Decompressor<'a>,
    chunk_size: Option<usize>,
    num_rows: usize,
}

impl<'a> StringIter<'a> {
    pub fn new(
        pages: Decompressor<'a>,
        num_rows: usize,
        chunk_size: Option<usize>,
    ) -> StringIter<'a> {
        Self {
            pages,
            chunk_size,
            num_rows,
        }
    }
}

impl Iterator for StringIter<'_> {
    type Item = databend_common_exception::Result<Column>;

    fn next(&mut self) -> Option<Self::Item> {
        let limit = self.chunk_size.unwrap_or(self.num_rows);
        // Use View structure and buffer directly, similar to read_view_col implementation
        let mut views = Vec::with_capacity(limit);
        let mut buffers = Vec::new();

        let mut total_bytes_len = 0;

        while views.len() < limit {
            let page = match self.pages.next() {
                Err(e) => {
                    return Some(Err(ErrorCode::StorageOther(format!(
                        "Failed to get next page: {}",
                        e
                    ))))
                }
                Ok(Some(page)) => page,
                Ok(None) => {
                    if views.is_empty() {
                        return None;
                    } else {
                        break;
                    }
                }
            };

            match page {
                Page::Data(data_page) => {
                    let physical_type = &data_page.descriptor.primitive_type.physical_type;
                    let is_optional = data_page.descriptor.primitive_type.field_info.repetition
                        == parquet2::schema::Repetition::Optional;

                    if physical_type != &PhysicalType::ByteArray || is_optional {
                        return Some(Err(ErrorCode::StorageOther(
                            "Only BYTE_ARRAY required fields are supported in this implementation"
                                .to_string(),
                        )));
                    }

                    let (_, _, values_buffer) = match parquet2::page::split_buffer(data_page) {
                        Ok(result) => result,
                        Err(e) => {
                            return Some(Err(ErrorCode::StorageOther(format!(
                                "Failed to split buffer: {}",
                                e
                            ))))
                        }
                    };

                    match data_page.encoding() {
                        Encoding::Plain => {
                            let estimated_capacity = values_buffer.len();
                            let mut page_bytes = Vec::with_capacity(estimated_capacity);
                            let mut page_offset = 0usize;
                            let current_buffer_index = buffers.len() as u32;

                            // Parse binary data - Parquet ByteArray format is:
                            // [4-byte length][data bytes]...[4-byte length][data bytes]...
                            let mut binary_values = values_buffer;
                            let remaining = limit - views.len();
                            let mut count = 0;

                            while !binary_values.is_empty() && count < remaining {
                                if binary_values.len() < 4 {
                                    return Some(Err(ErrorCode::StorageOther(
                                        "Invalid binary data: not enough bytes for length prefix"
                                            .to_string(),
                                    )));
                                }

                                // Extract length (first 4 bytes as little-endian u32)
                                // Optimized for little-endian machines
                                let length_array = [
                                    binary_values[0],
                                    binary_values[1],
                                    binary_values[2],
                                    binary_values[3],
                                ];
                                let length = u32::from_le_bytes(length_array) as usize;

                                // Skip the length bytes
                                binary_values = &binary_values[4..];

                                // Check if there are enough bytes for the string
                                if binary_values.len() < length {
                                    return Some(Err(ErrorCode::StorageOther(
                                        "Invalid binary data: not enough bytes for string content"
                                            .to_string(),
                                    )));
                                }

                                // Extract the string value
                                let str_bytes = &binary_values[0..length];

                                // Create View record using the same approach as BinaryViewColumnBuilder
                                let len: u32 = length as u32;
                                let mut payload = [0u8; 16];
                                payload[0..4].copy_from_slice(&len.to_le_bytes());

                                if len <= 12 {
                                    // |   len   |  prefix  |  remaining(zero-padded)  |
                                    //     ^          ^             ^
                                    // | 4 bytes | 4 bytes |      8 bytes              |
                                    // For small strings (≤12 bytes), store data directly in the View
                                    payload[4..4 + length].copy_from_slice(str_bytes);
                                } else {
                                    // |   len   |  prefix  |  buffer |  offsets  |
                                    //     ^          ^          ^         ^
                                    // | 4 bytes | 4 bytes | 4 bytes |  4 bytes  |
                                    //
                                    // For larger strings, store prefix + buffer reference

                                    // Set prefix (first 4 bytes)
                                    payload[4..8].copy_from_slice(&str_bytes[..4]);

                                    payload[8..12]
                                        .copy_from_slice(&current_buffer_index.to_le_bytes());

                                    // Set offset within current page buffer
                                    let offset_u32 = page_offset as u32;
                                    payload[12..16].copy_from_slice(&offset_u32.to_le_bytes());

                                    // Append string bytes to the current page buffer
                                    page_bytes.extend_from_slice(str_bytes);
                                    page_offset += length;
                                }

                                // Create View from bytes
                                let view = View::from_le_bytes(payload);
                                views.push(view);
                                total_bytes_len += view.length as usize;
                                count += 1;

                                // Move to next string
                                binary_values = &binary_values[length..];
                            }

                            if !page_bytes.is_empty() {
                                buffers.push(Buffer::from(page_bytes));
                            }

                            if views.len() >= limit {
                                break;
                            }
                        }
                        encoding => {
                            return Some(Err(ErrorCode::StorageOther(format!(
                                "Encoding {:?} is not supported in this implementation",
                                encoding
                            ))))
                        }
                    }
                }
                _ => {
                    return Some(Err(ErrorCode::StorageOther(
                        "Only data pages are supported".to_string(),
                    )))
                }
            }
        }

        if views.is_empty() {
            return None;
        }

        let total_buffer_len = buffers.iter().map(|b| b.len()).sum();

        // Convert views Vec to Buffer
        let views_buffer = Buffer::from(views);

        // Safely create Utf8ViewColumn
        let column = Utf8ViewColumn::new_unchecked(
            views_buffer,
            buffers.into(),
            total_bytes_len,
            total_buffer_len,
        );

        Some(Ok(Column::String(column)))
    }
}
