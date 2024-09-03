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

/// Compresses the state list for an n-dimensional Hilbert curve into a compact representation.
/// Used for generate n points to hilbert index.
/// refer to https://citeseerx.ist.psu.edu/document?repid=rep1&type=pdf&doi=bfd6d94c98627756989b0147a68b7ab1f881a0d
///
/// The function generates a compact state list by mapping each point's state in Hilbert curve to a 16-bit value.
/// The high byte represents the seq number, and the low byte represents next state index.
///
/// The function works for dimensions between 2 and 5 (inclusive).
///
/// - `dimension`: The dimension of the Hilbert curve (between 2 and 5).
///
/// Returns a `Vec<u16>` representing the compacted state list.
pub fn hilbert_compact_state_list(dimension: usize) -> Vec<u16> {
    assert!((2..=5).contains(&dimension));
    let state_map = generate_state_map(dimension);
    let nums = 1 << dimension;
    let mut state_list = vec![0u16; nums * state_map.len()];
    for (state_idx, state) in state_map {
        let state_start_idx = state_idx as usize * nums;

        for (seq_num, point, next_state) in &state.point_states {
            state_list[state_start_idx + *point as usize] =
                ((*seq_num as u16) << 8) | *next_state as u16;
        }
    }
    state_list
}

/// Decompresses a compacted state list for an n-dimensional Hilbert curve.
/// Used for generate hilbert index to n points.
pub fn hilbert_decompress_state_list(dimension: usize) -> Vec<u16> {
    assert!((2..=5).contains(&dimension));
    let state_map = generate_state_map(dimension);
    let nums = 1 << dimension;
    let mut state_list = vec![0u16; nums * state_map.len()];
    for (state_idx, state) in state_map {
        let state_start_idx = state_idx as usize * nums;

        for (seq_num, point, next_state) in &state.point_states {
            state_list[state_start_idx + *seq_num as usize] =
                ((*point as u16) << 8) | *next_state as u16;
        }
    }
    state_list
}

fn left_shift(n: u8, i: u8, shift: u8) -> u8 {
    ((i << shift) | (i >> (n - shift))) & ((1 << n) - 1)
}

#[derive(Clone)]
struct State {
    /// The unique identifier for the state.
    id: u8,
    /// A tuple representing the transformation matrix associated with this state.
    /// The first element is a bitmask indicating which columns have negative values,
    /// and the second element represents the amount of right shifting
    matrix: (u8, u8),
    /// A list of tuples representing the mapping of points in this state.
    /// Each tuple contains:
    /// - `seq_num`: which is the index in the generator table.
    /// - `point`: which represents coordinates on the Hilbert curve expressed as an n-point.
    /// - `next_state`: The next state that this point transitions to.
    point_states: Vec<(u8, u8, u8)>,
}

/// Generates the X2 sequence for an n-dimensional Hilbert curve using Gray code principles.
///
/// The X2 sequence is used in the construction of the Hilbert curve to determine
/// the transformations needed to map between different states. Specifically, X2 indicates
/// which coordinates of the curve need to be flipped or mirrored during the recursive
/// construction process.
///
/// The values in the X2 sequence have a specific characteristic: each consecutive pair in
/// the X2 sequence corresponds to the start and end points of a first-order Hilbert curve.
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

/// Generates a mapping of Hilbert curve states for a given dimension.
///
/// This function constructs a map of states for an n-dimensional Hilbert curve. Each state is represented by a `State`
/// struct, which includes an ID, a transformation matrix, and a list of point states. The transformation matrix describes
/// how the curve transitions between states, while the point states indicate the mapping of curve points within each state.
///
/// The algorithm works as follows:
/// 1. Compute the X2 sequence for the specified dimension using Gray code principles.
/// 2. Initialize the state map with the base state (state 0), which has an identity matrix and point states indicating the
///    initial curve configuration.
/// 3. Iterate over each point in the initial state, determining the next state based on the transformation matrix and updating
///    the point states with the correct state transitions.
/// 4. For each new state, compute the matrix and point state mappings, creating new states as necessary.
/// 5. Return a `HashMap` where each key is a state ID and the value is the corresponding `State` struct, representing all
///    possible states for the given dimension.
///
/// # Arguments
///
/// * `dimension` - The number of dimensions for the Hilbert curve, which should be between 2 and 5.
///
/// # Returns
///
/// A `HashMap<u8, State>` where each key is a unique state ID and each value is the corresponding `State` struct.
fn generate_state_map(dimension: usize) -> HashMap<u8, State> {
    struct StateInfo {
        seq_num: u8,
        point: u8,
        matrix: (u8, u8),
    }
    let n = dimension as u8;

    let x2_vec = get_x2_gray_codes(n);
    let point_count = 1u8 << n;
    let mask = point_count - 1;
    let initial_state_infos = (0..point_count)
        .map(|i| {
            let idx = (i << 1) as usize;
            let start = x2_vec[idx];
            let end = x2_vec[idx + 1];
            let delta = start ^ end;
            let right_shift = n - 1 - delta.trailing_zeros() as u8;

            StateInfo {
                seq_num: i,
                point: i ^ (i >> 1),
                matrix: (start, right_shift),
            }
        })
        .collect::<Vec<_>>();
    // Safe to convert u8, because the dimension between 2 and 5, the max row len is 32.
    let rows_len = initial_state_infos.len() as u8;

    let mut state_map: HashMap<u8, State> = HashMap::new();
    let mut new_state_ids: VecDeque<u8> = VecDeque::new();
    let mut next_state_id = 1;

    let initial_state = State {
        id: 0,
        matrix: (0, 0),
        point_states: initial_state_infos
            .iter()
            .map(|r| (r.seq_num, r.point, 0))
            .collect(),
    };
    state_map.insert(0, initial_state);

    // Set the initial state's point states.
    for entry in &initial_state_infos {
        let state_id = state_map
            .values()
            .find_map(|s| (s.matrix == entry.matrix).then_some(s.id));
        if let Some(id) = state_id {
            // set the next_state id.
            let initial_state = state_map.get_mut(&0).unwrap();
            initial_state.point_states[entry.seq_num as usize].2 = id;
        } else {
            let initial_state = state_map.get_mut(&0).unwrap();
            initial_state.point_states[entry.seq_num as usize].2 = next_state_id;
            // create new state.
            let new_state = State {
                id: next_state_id,
                matrix: entry.matrix,
                point_states: Vec::new(),
            };
            state_map.insert(next_state_id, new_state);
            new_state_ids.push_back(next_state_id);
            next_state_id += 1;
        }
    }

    // Set the other states.
    while let Some(current_state_id) = new_state_ids.pop_front() {
        let mut current_state = state_map.get(&current_state_id).unwrap().clone();
        current_state.point_states = (0..rows_len).map(|r| (r, 0, 0)).collect();

        for i in 0..rows_len {
            let new_point = left_shift(n, i ^ current_state.matrix.0, current_state.matrix.1);
            let initial_state = state_map.get(&0).unwrap();
            let point_state = initial_state
                .point_states
                .iter()
                .find(|(_, point, _)| *point == new_point)
                .unwrap();

            let current_point_state = &mut current_state.point_states[point_state.0 as usize];
            // set point.
            current_point_state.1 = i;

            // generate the matrix.
            let new_matrix = state_map
                .get(&point_state.2)
                .map(|v| {
                    let right_shift = ((v.matrix.0 >> current_state.matrix.1)
                        | (v.matrix.0 << (n - current_state.matrix.1)))
                        & mask;
                    (
                        right_shift ^ current_state.matrix.0,
                        (v.matrix.1 + current_state.matrix.1) % n,
                    )
                })
                .unwrap();

            let state_id = state_map
                .values()
                .find_map(|s| (s.matrix == new_matrix).then_some(s.id));
            // set the next_state id.
            if let Some(id) = state_id {
                current_point_state.2 = id;
            } else {
                current_point_state.2 = next_state_id;
                // create new state.
                let new_state = State {
                    id: next_state_id,
                    matrix: new_matrix,
                    point_states: vec![],
                };
                state_map.insert(next_state_id, new_state);
                new_state_ids.push_back(next_state_id);
                next_state_id += 1;
            }
        }

        state_map.insert(current_state_id, current_state);
    }

    state_map
}
