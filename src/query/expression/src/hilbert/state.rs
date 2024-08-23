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

pub fn hilbert_compact_state_list(dimension: usize) -> Vec<u16> {
    assert!((2..=5).contains(&dimension));
    let state_map = generate_state_map(dimension);
    let nums = 1 << dimension;
    let mut state_list = vec![0u16; nums * state_map.len()];
    for (state_idx, state) in state_map {
        let state_start_idx = state_idx as usize * nums;

        for (y, x1, state) in &state.point_states {
            state_list[state_start_idx + *x1 as usize] = ((*y as u16) << 8) | *state as u16;
        }
    }
    state_list
}

pub fn hilbert_decompress_state_list(dimension: usize) -> Vec<u16> {
    assert!((2..=5).contains(&dimension));
    let state_map = generate_state_map(dimension);
    let nums = 1 << dimension;
    let mut state_list = vec![0u16; nums * state_map.len()];
    for (state_idx, state) in state_map {
        let state_start_idx = state_idx as usize * nums;

        for (y, x1, state) in &state.point_states {
            state_list[state_start_idx + *y as usize] = ((*x1 as u16) << 8) | *state as u16;
        }
    }
    state_list
}

fn left_shift(n: u8, i: u8, shift: u8) -> u8 {
    ((i << shift) | (i >> (n - shift))) & ((1 << n) - 1)
}

#[derive(Clone)]
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

fn generate_state_map(dimension: usize) -> HashMap<u8, State> {
    struct GrayCodeEntry {
        y: u8,
        x1: u8,
        x2: u8,
        dy: u8,
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

    state_map
}
