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

use std::intrinsics::unlikely;
use std::sync::Arc;
use std::sync::RwLock;

use databend_common_base::base::WatchNotify;
use databend_common_exception::Result;
use databend_common_expression::compare_columns;
use databend_common_expression::types::BinaryType;
use databend_common_expression::FromData;

pub struct SampleState {
    pub inner: RwLock<SampleStateInner>,
    pub done: Arc<WatchNotify>,
}

impl SampleState {
    pub fn new(total_inputs: usize, partitions: usize) -> Arc<Self> {
        Arc::new(SampleState {
            inner: RwLock::new(SampleStateInner {
                partitions,
                total_inputs,
                completed_inputs: 0,
                values: vec![],
                bounds: vec![],
                max_value: None,
            }),
            done: Arc::new(WatchNotify::new()),
        })
    }

    pub fn merge_sample(&self, values: Vec<(u64, Vec<Vec<u8>>)>) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        inner.completed_inputs += 1;
        inner.values.extend_from_slice(&values);

        if inner.completed_inputs >= inner.total_inputs {
            inner.determine_bounds()?;
            self.done.notify_waiters();
        }
        Ok(())
    }

    pub fn get_bounds(&self) -> (Vec<Vec<u8>>, Option<Vec<u8>>) {
        let inner = self.inner.read().unwrap();
        (inner.bounds.clone(), inner.max_value.clone())
    }
}

pub struct SampleStateInner {
    partitions: usize,
    total_inputs: usize,

    completed_inputs: usize,
    bounds: Vec<Vec<u8>>,
    max_value: Option<Vec<u8>>,

    values: Vec<(u64, Vec<Vec<u8>>)>,
}

impl SampleStateInner {
    fn determine_bounds(&mut self) -> Result<()> {
        if self.partitions < 2 {
            return Ok(());
        }

        let (total_samples, total_rows) = self
            .values
            .iter()
            .fold((0, 0), |(acc_samples, acc_rows), (rows, vals)| {
                (acc_samples + vals.len(), acc_rows + *rows)
            });
        let step = total_rows as f64 / self.partitions as f64;
        let values = std::mem::take(&mut self.values);
        let mut data = Vec::with_capacity(total_samples);
        let mut weights = Vec::with_capacity(total_samples);

        for (num, values) in values.into_iter() {
            let weight = num as f64 / values.len() as f64;
            values.into_iter().for_each(|v| {
                data.push(v);
                weights.push(weight);
            });
        }
        let col = BinaryType::from_data(data.clone());
        let indices = compare_columns(vec![col], total_samples)?;

        let max_index = indices[total_samples - 1] as usize;
        let max_val = &data[max_index];

        let mut cum_weight = 0.0;
        let mut target = step;
        let mut bounds = Vec::with_capacity(self.partitions - 1);
        let mut previous_bound = None;

        let mut i = 0;
        let mut j = 0;
        while i < total_samples && j < self.partitions - 1 {
            let idx = indices[i] as usize;
            let value = &data[idx];
            let weight = weights[idx];
            cum_weight += weight;

            if cum_weight >= target && previous_bound.is_none_or(|prev| value > prev) {
                if unlikely(value == max_val) {
                    self.max_value = Some(max_val.clone());
                    break;
                }

                bounds.push(value.clone());
                previous_bound = Some(value);
                target += step;
                j += 1;
            }
            i += 1;
        }
        self.bounds = bounds;
        Ok(())
    }
}
