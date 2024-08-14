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
use std::collections::VecDeque;
use std::vec::Vec;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

pub fn hilbert_compact_state_list(dimension: usize) -> Result<Vec<u16>> {
    let state_map = generate_state_map(dimension)?;
    let nums = 1 << dimension;
    let mut state_list = vec![0u16; nums * state_map.len()];
    for (state_idx, state) in state_map {
        let state_start_idx = state_idx as usize * nums;

        for (y, x1, state) in &state.point_states {
            state_list[state_start_idx + *x1 as usize] = ((*y as u16) << 8) | *state as u16;
        }
    }
    Ok(state_list)
}

#[inline]
fn left_shift(n: u8, i: u8, shift: u8) -> u8 {
    ((i << shift) | (i >> (n - shift))) & ((1 << n) - 1)
}

#[derive(Clone, Debug)]
struct State {
    id: u8,

    x2: u8,
    dy: u8,
    // (y,x1,state)
    point_states: Vec<(u8, u8, u8)>,
}

fn get_x2_gray_codes(n: u8) -> Vec<u8> {
    if n == 1 {
        return vec![0, 1, 0, 1];
    }

    let mask = 1 << (n - 1);
    let mut base = get_x2_gray_codes(n - 1);

    let last_index = base.len() - 1;
    base[last_index] = base[last_index - 1] + mask;

    let len = base.len() * 2;
    let mut result = vec![0; len];
    for i in 0..base.len() {
        result[i] = base[i];
        result[len - 1 - i] = base[i] ^ mask;
    }

    result
}

fn generate_state_map(dimension: usize) -> Result<HashMap<u8, State>> {
    struct GrayCodeEntry {
        y: u8,
        x1: u8,
        x2: u8,
        dy: u8,
    }
    if !(2..=5).contains(&dimension) {
        return Err(ErrorCode::Internal(
            "Only support dimension between 2 and 5",
        ));
    }
    let n = dimension as u8;

    let x2s = get_x2_gray_codes(n);
    let len = 1u8 << n;
    let gray_codes = (0..len)
        .map(|i| {
            let idx = (i << 1) as usize;
            let x21 = x2s[idx];
            let x22 = x2s[idx + 1];
            let dy = x21 ^ x22;

            GrayCodeEntry {
                y: i,
                x1: i ^ (i >> 1),
                x2: x21,
                dy: n - 1 - dy.trailing_zeros() as u8,
            }
        })
        .collect::<Vec<_>>();
    let row_len = gray_codes.len();
    assert!(row_len < 256);

    let mut state_map: HashMap<u8, State> = HashMap::new();
    let mut list: VecDeque<u8> = VecDeque::new();
    let mut next_state_num = 1;

    let initial_state = State {
        id: 0,
        x2: 0,
        dy: 0,
        point_states: gray_codes.iter().map(|r| (r.y, r.x1, 0)).collect(),
    };
    state_map.insert(0, initial_state);

    for entry in &gray_codes {
        let s_id = state_map
            .values()
            .find_map(|s| (s.x2 == entry.x2 && s.dy == entry.dy).then_some(s.id));
        if let Some(id) = s_id {
            let initial_state = state_map.get_mut(&0).unwrap();
            initial_state.point_states[entry.y as usize].2 = id;
        } else {
            let initial_state = state_map.get_mut(&0).unwrap();
            initial_state.point_states[entry.y as usize].2 = next_state_num;
            let new_state = State {
                id: next_state_num,
                x2: entry.x2,
                dy: entry.dy,
                point_states: Vec::new(),
            };
            state_map.insert(next_state_num, new_state);
            list.push_back(next_state_num);
            next_state_num += 1;
        }
    }

    let rows_len = row_len as u8;
    while let Some(current_state_id) = list.pop_front() {
        let mut current_state = state_map.get(&current_state_id).unwrap().clone();
        current_state.point_states = (0..rows_len).map(|r| (r, 0, 0)).collect();

        for i in 0..rows_len {
            let j = left_shift(n, i ^ current_state.x2, current_state.dy);
            let initial_state = state_map.get(&0).unwrap();
            let p = initial_state
                .point_states
                .iter()
                .find(|(_, x1, _)| *x1 == j)
                .expect("PointState not found");

            let current_point_state = &mut current_state.point_states[p.0 as usize];
            current_point_state.1 = i;

            let (x2, dy) = state_map
                .get(&p.2)
                .map(|v| {
                    let right_shift =
                        ((v.x2 >> current_state.dy) | (v.x2 << (n - current_state.dy))) & (len - 1);
                    (
                        right_shift ^ current_state.x2,
                        (v.dy + current_state.dy) % n,
                    )
                })
                .expect("State not found");

            let s_id = state_map
                .values()
                .find_map(|s| (s.x2 == x2 && s.dy == dy).then_some(s.id));
            if let Some(id) = s_id {
                current_point_state.2 = id;
            } else {
                current_point_state.2 = next_state_num;
                let new_state = State {
                    id: next_state_num,
                    x2,
                    dy,
                    point_states: vec![],
                };
                state_map.insert(next_state_num, new_state);
                list.push_back(next_state_num);
                next_state_num += 1;
            }
        }

        state_map.insert(current_state_id, current_state);
    }

    Ok(state_map)
}

macro_rules! hilbert_index {
    ($name:ident, $t:ty, $l: literal) => {
        pub fn $name(point: &[$t], states: &[u16]) -> Vec<u8> {
            let n = point.len();
            let num_bits = $l * n;
            let num_bytes = (num_bits + 7) / 8;
            let mut result = vec![0u8; num_bytes];
            let initial_offset = (num_bytes * 8) - num_bits;
            let mut current_state = 0;

            for i in 0..$l {
                let mut z = 0;
                point.iter().for_each(|v| {
                    z = (z << 1) | ((*v >> ($l - 1 - i)) & 1) as u8;
                });

                let value = states[current_state as usize * (1 << n) + z as usize];
                let new_bits = (value >> 8) as u8;
                let next_state = (value & 255) as u8;
                let offset = initial_offset + (i * n);

                let mut bits = (new_bits as u16) << (16 - n);
                let mut remaining_bits = n;
                let mut key_index = offset / 8;
                let mut key_offset = offset - (key_index * 8);

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
    };
}

hilbert_index!(hilbert_index_u8, u8, 8);
hilbert_index!(hilbert_index_u16, u16, 16);
hilbert_index!(hilbert_index_u32, u32, 32);
hilbert_index!(hilbert_index_u64, u64, 64);

#[cfg(test)]
mod tests {
    use databend_common_exception::Result;

    use super::*;

    fn hilbert_decompress_state_list(dimension: usize) -> Result<Vec<u16>> {
        let state_map = generate_state_map(dimension)?;
        let nums = 1 << dimension;
        let mut state_list = vec![0u16; nums * state_map.len()];
        for (state_idx, state) in state_map {
            let state_start_idx = state_idx as usize * nums;

            for (y, x1, state) in &state.point_states {
                state_list[state_start_idx + *y as usize] = ((*x1 as u16) << 8) | *state as u16;
            }
        }
        Ok(state_list)
    }

    macro_rules! hilbert_points {
        ($name:ident, $t:ty, $l: literal) => {
            fn $name(key: &[u8], n: usize, states: &[u16]) -> Vec<$t> {
                let mut result = vec![0; n];
                let initial_offset = key.len() * 8 - $l * n;
                let mut current_state = 0;

                for i in 0..$l {
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
                        *item = (*item << 1) | (v as $t);
                    }

                    current_state = next_state;
                }

                result
            }
        };
    }

    // hilbert_points!(hilbert_points_u8, u8, 8);
    // hilbert_points!(hilbert_points_u16, u16, 16);
    hilbert_points!(hilbert_points_u32, u32, 32);
    // hilbert_points!(hilbert_points_u64, u64, 64);

    #[test]
    fn tests() -> Result<()> {
        let n = 5;
        let state_list = hilbert_compact_state_list(n)?;
        println!(
            "state_list len: {}, data: {:?}",
            state_list.len(),
            state_list
        );

        let point = vec![1, 2, 3, 99999, 206];
        let key = hilbert_index_u32(&point, &state_list);
        println!("res: {:?}", key);

        let state_list = hilbert_decompress_state_list(n)?;
        println!(
            "state_list len: {}, data: {:?}",
            state_list.len(),
            state_list
        );
        let res = hilbert_points_u32(&key, n, &state_list);
        println!("res: {:?}", res);

        Ok(())
    }
}
