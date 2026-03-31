// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{collections::VecDeque, ops::Range};

use snafu::location;

use crate::decoder::{
    FilterExpression, NextDecodeTask, PriorityRange, ScheduledScanLine, SchedulerContext,
};

use arrow_schema::DataType;
use futures::future::BoxFuture;
use lance_core::{Error, Result};

pub trait SchedulingJob: std::fmt::Debug {
    fn schedule_next(
        &mut self,
        context: &mut SchedulerContext,
        priority: &dyn PriorityRange,
    ) -> Result<ScheduledScanLine>;

    fn num_rows(&self) -> u64;
}

/// A scheduler for a field's worth of data
///
/// Each field in a reader's output schema maps to one field scheduler.  This scheduler may
/// map to more than one column.  For example, one field of struct data may
/// cover many columns of child data.  In fact, the entire file is treated as one
/// top-level struct field.
///
/// The scheduler is responsible for calculating the necessary I/O.  One schedule_range
/// request could trigger multiple batches of I/O across multiple columns.  The scheduler
/// should emit decoders into the sink as quickly as possible.
///
/// As soon as the scheduler encounters a batch of data that can decoded then the scheduler
/// should emit a decoder in the "unloaded" state.  The decode stream will pull the decoder
/// and start decoding.
///
/// The order in which decoders are emitted is important.  Pages should be emitted in
/// row-major order allowing decode of complete rows as quickly as possible.
///
/// The `FieldScheduler` should be stateless and `Send` and `Sync`.  This is
/// because it might need to be shared.  For example, a list page has a reference to
/// the field schedulers for its items column.  This is shared with the follow-up I/O
/// task created when the offsets are loaded.
///
/// See [`crate::decoder`] for more information
pub trait FieldScheduler: Send + Sync + std::fmt::Debug {
    /// Called at the beginning of scheduling to initialize the scheduler
    fn initialize<'a>(
        &'a self,
        filter: &'a FilterExpression,
        context: &'a SchedulerContext,
    ) -> BoxFuture<'a, Result<()>>;
    /// Schedules I/O for the requested portions of the field.
    ///
    /// Note: `ranges` must be ordered and non-overlapping
    /// TODO: Support unordered or overlapping ranges in file scheduler
    fn schedule_ranges<'a>(
        &'a self,
        ranges: &[Range<u64>],
        filter: &FilterExpression,
    ) -> Result<Box<dyn SchedulingJob + 'a>>;
    /// The number of rows in this field
    fn num_rows(&self) -> u64;
}

#[derive(Debug)]
pub struct DecoderReady {
    // The decoder that is ready to be decoded
    pub decoder: Box<dyn LogicalPageDecoder>,
    // The path to the decoder, the first value is the column index
    // following values, if present, are nested child indices
    //
    // For example, a path of [1, 1, 0] would mean to grab the second
    // column, then the second child, and then the first child.
    //
    // It could represent x in the following schema:
    //
    // score: float64
    // points: struct
    //   color: string
    //   location: struct
    //     x: float64
    //
    // Currently, only struct decoders have "children" although other
    // decoders may at some point as well.  List children are only
    // handled through indirect I/O at the moment and so they don't
    // need to be represented (yet)
    pub path: VecDeque<u32>,
}

/// A decoder for a field's worth of data
///
/// The decoder is initially "unloaded" (doesn't have all its data).  The [`Self::wait`]
/// method should be called to wait for the needed I/O data before attempting to decode
/// any further.
///
/// Unlike the other decoder types it is assumed that `LogicalPageDecoder` is stateful
/// and only `Send`.  This is why we don't need a `rows_to_skip` argument in [`Self::drain`]
pub trait LogicalPageDecoder: std::fmt::Debug + Send {
    /// Add a newly scheduled child decoder
    ///
    /// The default implementation does not expect children and returns
    /// an error.
    fn accept_child(&mut self, _child: DecoderReady) -> Result<()> {
        Err(Error::Internal {
            message: format!(
                "The decoder {:?} does not expect children but received a child",
                self
            ),
            location: location!(),
        })
    }
    /// Waits until at least `num_rows` have been loaded
    fn wait_for_loaded(&'_ mut self, loaded_need: u64) -> BoxFuture<'_, Result<()>>;
    /// The number of rows loaded so far
    fn rows_loaded(&self) -> u64;
    /// The number of rows that still need loading
    fn rows_unloaded(&self) -> u64 {
        self.num_rows() - self.rows_loaded()
    }
    /// The total number of rows in the field
    fn num_rows(&self) -> u64;
    /// The number of rows that have been drained so far
    fn rows_drained(&self) -> u64;
    /// The number of rows that are still available to drain
    fn rows_left(&self) -> u64 {
        self.num_rows() - self.rows_drained()
    }
    /// Creates a task to decode `num_rows` of data into an array
    fn drain(&mut self, num_rows: u64) -> Result<NextDecodeTask>;
    /// The data type of the decoded data
    fn data_type(&self) -> &DataType;
}
