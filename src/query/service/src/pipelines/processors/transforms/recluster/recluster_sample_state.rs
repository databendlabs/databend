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

use std::sync::Arc;
use std::sync::RwLock;

use databend_common_base::base::WatchNotify;
use databend_common_exception::Result;
use databend_common_expression::compare_columns;
use databend_common_expression::types::ArgType;
use databend_common_expression::Scalar;

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
            }),
            done: Arc::new(WatchNotify::new()),
        })
    }

    pub fn merge_sample<T>(&self, values: Vec<(u64, Vec<Scalar>)>) -> Result<()>
    where
        T: ArgType,
        T::Scalar: Ord,
    {
        let mut inner = self.inner.write().unwrap();
        inner.completed_inputs += 1;
        inner.values.extend_from_slice(&values);

        if inner.completed_inputs >= inner.total_inputs {
            inner.determine_bounds::<T>()?;
            self.done.notify_waiters();
        }
        Ok(())
    }

    pub fn get_bounds<T>(&self) -> Vec<T::Scalar>
    where
        T: ArgType,
        T::Scalar: Ord,
    {
        let inner = self.inner.read().unwrap();
        inner
            .bounds
            .iter()
            .map(|v| T::to_owned_scalar(T::try_downcast_scalar(&v.as_ref()).unwrap()))
            .collect()
    }
}

pub struct SampleStateInner {
    partitions: usize,
    total_inputs: usize,

    completed_inputs: usize,
    bounds: Vec<Scalar>,

    values: Vec<(u64, Vec<Scalar>)>,
}

impl SampleStateInner {
    fn determine_bounds<T>(&mut self) -> Result<()>
    where
        T: ArgType,
        T::Scalar: Ord,
    {
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
                let val = T::to_owned_scalar(T::try_downcast_scalar(&v.as_ref()).unwrap());
                data.push(val);
                weights.push(weight);
            });
        }
        let col = T::upcast_column(T::column_from_vec(data.clone(), &[]));
        let indices = compare_columns(vec![col], total_samples)?;

        let mut cum_weight = 0.0;
        let mut target = step;
        let mut bounds = Vec::with_capacity(self.partitions - 1);
        let mut previous_bound = None;

        let mut i = 0;
        let mut j = 0;
        while i < total_samples && j < self.partitions - 1 {
            let idx = indices[i] as usize;
            let weight = weights[idx];
            cum_weight += weight;
            if cum_weight >= target {
                let data = &data[idx];
                if previous_bound.as_ref().is_none_or(|prev| data > prev) {
                    bounds.push(T::upcast_scalar(data.clone()));
                    target += step;
                    j += 1;
                    previous_bound = Some(data.clone());
                }
            }
            i += 1;
        }
        self.bounds = bounds;
        Ok(())
    }
}
