// Copyright 2023 Datafuse Labs.
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

use std::{collections::HashMap, fmt::Write, sync::Arc};

use arrow::{
    array::{Array, ArrayDataBuilder, LargeBinaryArray, LargeStringArray},
    csv::WriterBuilder,
    datatypes::{DataType, Field, Schema},
    error::ArrowError,
    record_batch::RecordBatch,
};
use arrow_flight::{utils::flight_data_to_arrow_batch, FlightData};
use comfy_table::{Cell, CellAlignment, Table};

use arrow_cast::display::{ArrayFormatter, FormatOptions};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use tonic::Streaming;

use indicatif::{HumanBytes, ProgressBar, ProgressState, ProgressStyle};

#[async_trait::async_trait]
pub trait ChunkDisplay {
    async fn display(&mut self) -> Result<(), ArrowError>;
    fn total_rows(&self) -> usize;
}

pub struct ReplDisplay {
    schema: Schema,
    stream: Streaming<FlightData>,

    rows: usize,
    progress: Option<ProgressBar>,
    start: Instant,
}

impl ReplDisplay {
    pub fn new(schema: Schema, start: Instant, stream: Streaming<FlightData>) -> Self {
        Self {
            schema,
            stream,
            rows: 0,
            progress: None,
            start,
        }
    }
}

#[async_trait::async_trait]
impl ChunkDisplay for ReplDisplay {
    async fn display(&mut self) -> Result<(), ArrowError> {
        let mut batches = Vec::new();
        let mut progress = ProgressValue::default();

        while let Some(datum) = self.stream.next().await {
            match datum {
                Ok(datum) => {
                    if datum.app_metadata[..] == [0x01] {
                        progress = serde_json::from_slice(&datum.data_body)
                            .map_err(|err| ArrowError::ExternalError(Box::new(err)))?;
                        if self.progress.as_mut().is_none() {
                            let pb = ProgressBar::new(progress.total_bytes as u64);
                            pb.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] {msg} {wide_bar:.cyan/blue} ({eta})")
                                .unwrap()
                                .with_key("eta", |state: &ProgressState, w: &mut dyn Write| write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap())
                                .progress_chars("█▓▒░ "));
                            self.progress = Some(pb);
                        }

                        let pb = self.progress.as_mut().unwrap();
                        pb.set_position(progress.read_bytes as u64);
                        pb.set_message(format!(
                            "{}/{} ({} rows/s), {}/{} ({} /s)",
                            humanize_count(progress.read_rows as f64),
                            humanize_count(progress.total_rows as f64),
                            humanize_count(progress.read_rows as f64 / pb.elapsed().as_secs_f64()),
                            HumanBytes(progress.read_bytes as u64),
                            HumanBytes(progress.total_bytes as u64),
                            HumanBytes(
                                (progress.read_bytes as f64 / pb.elapsed().as_secs_f64()) as u64
                            )
                        ));
                    } else {
                        let dicitionaries_by_id = HashMap::new();
                        let batch = flight_data_to_arrow_batch(
                            &datum,
                            Arc::new(self.schema.clone()),
                            &dicitionaries_by_id,
                        )?;

                        self.rows += batch.num_rows();

                        let batch = normalize_record_batch(&batch)?;
                        batches.push(batch);
                    }
                }
                Err(err) => {
                    eprintln!("error: {:?}", err.message());
                }
            }
        }

        if let Some(pb) = self.progress.take() {
            pb.finish_and_clear();
        }
        print_batches(&batches)?;

        println!();

        println!(
            "{} rows result set in {:.3} sec. Processed {} rows, {} ({} rows/s, {}/s)",
            self.rows,
            self.start.elapsed().as_secs_f64(),
            humanize_count(progress.total_rows as f64),
            HumanBytes(progress.total_rows as u64),
            humanize_count(progress.total_rows as f64 / self.start.elapsed().as_secs_f64()),
            HumanBytes((progress.total_bytes as f64 / self.start.elapsed().as_secs_f64()) as u64),
        );
        println!();

        Ok(())
    }

    fn total_rows(&self) -> usize {
        self.rows
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct ProgressValue {
    pub total_rows: usize,
    pub total_bytes: usize,

    pub read_rows: usize,
    pub read_bytes: usize,
}

pub struct FormatDisplay {
    schema: Schema,
    stream: Streaming<FlightData>,
}

impl FormatDisplay {
    pub fn new(schema: Schema, stream: Streaming<FlightData>) -> Self {
        Self { schema, stream }
    }
}

#[async_trait::async_trait]
impl ChunkDisplay for FormatDisplay {
    async fn display(&mut self) -> Result<(), ArrowError> {
        let mut bytes = vec![];
        let builder = WriterBuilder::new()
            .has_headers(false)
            .with_delimiter(b'\t');
        let mut writer = builder.build(&mut bytes);

        while let Some(datum) = self.stream.next().await {
            match datum {
                Ok(datum) => {
                    if datum.app_metadata[..] != [0x01] {
                        let dicitionaries_by_id = HashMap::new();
                        let batch = flight_data_to_arrow_batch(
                            &datum,
                            Arc::new(self.schema.clone()),
                            &dicitionaries_by_id,
                        )?;
                        let batch = normalize_record_batch(&batch)?;
                        writer.write(&batch)?;
                    }
                }
                Err(err) => {
                    eprintln!("error: {:?}", err.message());
                }
            }
        }

        let formatted = std::str::from_utf8(writer.into_inner())
            .map_err(|e| ArrowError::CsvError(e.to_string()))?;
        println!("{}", formatted);
        Ok(())
    }

    fn total_rows(&self) -> usize {
        0
    }
}

fn normalize_record_batch(batch: &RecordBatch) -> Result<RecordBatch, ArrowError> {
    let num_columns = batch.num_columns();
    let mut columns = Vec::with_capacity(num_columns);
    let mut fields = Vec::with_capacity(num_columns);

    for i in 0..num_columns {
        let field = batch.schema().field(i).clone();
        let array = batch.column(i);
        if let Some(binary_array) = array.as_any().downcast_ref::<LargeBinaryArray>() {
            let data = binary_array.data().clone();
            let builder = ArrayDataBuilder::from(data).data_type(DataType::LargeUtf8);
            let data = builder.build()?;

            let utf8_array = LargeStringArray::from(data);

            columns.push(Arc::new(utf8_array) as Arc<dyn Array>);
            fields.push(
                Field::new(field.name(), DataType::LargeUtf8, field.is_nullable())
                    .with_metadata(field.metadata().clone()),
            );
        } else {
            columns.push(array.clone());
            fields.push(field);
        }
    }

    let schema = Schema::new(fields);
    RecordBatch::try_new(Arc::new(schema), columns)
}

/// Prints a visual representation of record batches to stdout
pub fn print_batches(results: &[RecordBatch]) -> Result<(), ArrowError> {
    let options = FormatOptions::default().with_display_error(true);

    println!("{}", create_table(results, &options)?);
    Ok(())
}

/// Convert a series of record batches into a table
fn create_table(results: &[RecordBatch], options: &FormatOptions) -> Result<Table, ArrowError> {
    let mut table = Table::new();
    table.load_preset("││──├─┼┤│    ──┌┐└┘");
    if results.is_empty() {
        return Ok(table);
    }

    let schema = results[0].schema();

    let mut header = Vec::with_capacity(schema.fields().len());
    let mut aligns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let cell = Cell::new(format!(
            "{}\n{}",
            field.name(),
            normalize_datatype(field.data_type())
        ))
        .set_alignment(CellAlignment::Center);

        header.push(cell);

        if field.data_type().is_numeric() {
            aligns.push(CellAlignment::Right);
        } else {
            aligns.push(CellAlignment::Left);
        }
    }
    table.set_header(header);

    for batch in results {
        let formatters = batch
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), options))
            .collect::<Result<Vec<_>, ArrowError>>()?;

        for row in 0..batch.num_rows() {
            let mut cells = Vec::new();
            for (formatter, align) in formatters.iter().zip(aligns.iter()) {
                let cell = Cell::new(formatter.value(row)).set_alignment(*align);
                cells.push(cell);
            }
            table.add_row(cells);
        }
    }

    Ok(table)
}

// LargeUtf8 --> String
fn normalize_datatype(ty: &DataType) -> String {
    match ty {
        DataType::LargeUtf8 => "String".to_owned(),
        _ => format!("{ty}"),
    }
}

pub(crate) fn format_error(error: ArrowError) -> String {
    match error {
        ArrowError::IoError(err) => {
            static START: &str = "Code:";
            static END: &str = ". at";

            let message_index = err.find(START).unwrap_or(0);
            let message_end_index = err.find(END).unwrap_or(err.len());
            let message = &err[message_index..message_end_index];
            message.replace("\\n", "\n")
        }
        other => format!("{}", other),
    }
}

pub fn humanize_count(num: f64) -> String {
    if num == 0.0 {
        return String::from("0");
    }

    let negative = if num.is_sign_positive() { "" } else { "-" };
    let num = num.abs();
    let units = [
        "",
        " thousand",
        " million",
        " billion",
        " trillion",
        " quadrillion",
    ];

    if num < 1_f64 {
        return format!("{}{:.2}", negative, num);
    }
    let delimiter = 1000_f64;
    let exponent = std::cmp::min(
        (num.ln() / delimiter.ln()).floor() as i32,
        (units.len() - 1) as i32,
    );
    let pretty_bytes = format!("{:.2}", num / delimiter.powi(exponent))
        .parse::<f64>()
        .unwrap()
        * 1_f64;
    let unit = units[exponent as usize];
    format!("{}{}{}", negative, pretty_bytes, unit)
}
