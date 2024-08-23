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

use crate::hilbert::LUT;

pub fn hilbert_index(point: &[&[u8]], width: usize) -> Vec<u8> {
    let n = point.len();
    assert!((2..=5).contains(&n));

    let per_bits = width * 8;
    let num_bits = per_bits * n;
    let num_bytes = (num_bits + 7) / 8;
    let initial_offset = (num_bytes * 8) - num_bits;

    let states = LUT[n - 2];
    let mut current_state = 0;
    let mut result = vec![0u8; num_bytes];
    for i in 0..per_bits {
        let mut z = 0;

        for v in point {
            let byte_index = i / 8;
            let bit_index = 7 - (i % 8);

            let byte = *v.get(byte_index).unwrap_or(&0);
            z = (z << 1) | ((byte >> bit_index) & 1);
        }

        let value = states[current_state as usize * (1 << n) + z as usize];
        let new_bits = (value >> 8) as u8;
        let next_state = (value & 0xFF) as u8;
        let offset = initial_offset + (i * n);

        let mut bits = (new_bits as u16) << (16 - n);
        let mut remaining_bits = n;
        let mut key_index = offset / 8;
        let mut key_offset = offset % 8;

        while remaining_bits > 0 {
            result[key_index] |= (bits >> (8 + key_offset)) as u8;
            remaining_bits -= (8 - key_offset).min(remaining_bits);
            bits <<= 8 - key_offset;
            key_offset = 0;
            key_index += 1;
        }
        current_state = next_state;
    }

    result
}

pub fn hilbert_decompress(key: &[u8], width: usize, states: &[u16]) -> Vec<Vec<u8>> {
    let n = key.len() / width;
    let per_bits = width * 8;
    let initial_offset = key.len() * 8 - per_bits * n;

    let mut current_state = 0;
    let mut result = vec![vec![0u8; width]; n];
    for i in 0..per_bits {
        let offset = initial_offset + i * n;
        let mut h = 0;
        let mut remaining_bits = n;
        let mut key_index = offset / 8;
        let mut key_offset = offset - (key_index * 8);

        while remaining_bits > 0 {
            let bits_from_idx = remaining_bits.min(8 - key_offset);
            let new_int = key[key_index] >> (8 - key_offset - bits_from_idx);
            h = (h << bits_from_idx) | (new_int & ((1 << bits_from_idx) - 1));

            remaining_bits -= bits_from_idx;
            key_offset = 0;
            key_index += 1;
        }

        let value = states[current_state as usize * (1 << n) + h as usize];
        let z = (value >> 8) as u8;
        let next_state = (value & 255) as u8;

        for (j, item) in result.iter_mut().enumerate() {
            let v = (z >> (n - 1 - j)) & 1;
            let current_value = (*item)[i / 8];
            (*item)[i / 8] = (current_value << 1) | v;
        }

        current_state = next_state;
    }

    result
}
