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

use std::fmt::Write;

use anyhow::Result;

use comfy_table::{Cell, CellAlignment, Table};

use databend_driver::{QueryProgress, Row, RowProgressIterator, RowWithProgress, SchemaRef};
use futures::StreamExt;
use rustyline::highlight::Highlighter;
use tokio::time::Instant;

use indicatif::{HumanBytes, ProgressBar, ProgressState, ProgressStyle};

use crate::{ast::format_query, config::Settings, helper::CliHelper};

#[async_trait::async_trait]
pub trait ChunkDisplay {
    async fn display(&mut self) -> Result<()>;
    fn total_rows(&self) -> usize;
}

pub struct ReplDisplay<'a> {
    settings: &'a Settings,
    query: &'a str,
    schema: SchemaRef,
    data: RowProgressIterator,

    rows: usize,
    progress: Option<ProgressBar>,
    start: Instant,
}

impl<'a> ReplDisplay<'a> {
    pub fn new(
        settings: &'a Settings,
        query: &'a str,
        start: Instant,
        schema: SchemaRef,
        data: RowProgressIterator,
    ) -> Self {
        Self {
            settings,
            query,
            schema,
            data,
            rows: 0,
            progress: None,
            start,
        }
    }
}

#[async_trait::async_trait]
impl<'a> ChunkDisplay for ReplDisplay<'a> {
    async fn display(&mut self) -> Result<()> {
        let mut rows: Vec<Row> = Vec::new();
        let mut progress = QueryProgress::default();

        if self.settings.display_pretty_sql {
            let format_sql = format_query(self.query);
            let format_sql = CliHelper::new().highlight(&format_sql, format_sql.len());
            println!("\n{}\n", format_sql);
        }

        while let Some(line) = self.data.next().await {
            match line {
                Ok(RowWithProgress::Progress(pg)) => {
                    progress = pg;
                    if self.progress.as_mut().is_none() {
                        let pb = ProgressBar::new(progress.total_bytes as u64);
                        let progress_color = &self.settings.progress_color;
                        let template = "{spinner:.${progress_color}} [{elapsed_precise}] {msg} {wide_bar:.${progress_color}/blue} ({eta})".replace("${progress_color}", progress_color);

                        pb.set_style(
                            ProgressStyle::with_template(&template)
                                .unwrap()
                                .with_key("eta", |state: &ProgressState, w: &mut dyn Write| {
                                    write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap()
                                })
                                .progress_chars("█▓▒░ "),
                        );
                        self.progress = Some(pb);
                    }
                    let pb = self.progress.as_mut().unwrap();
                    pb.set_position(progress.read_bytes as u64);
                    pb.set_message(format!(
                        "Processing {}/{} ({} rows/s), {}/{} ({}/s)",
                        humanize_count(progress.read_rows as f64),
                        humanize_count(progress.total_rows as f64),
                        humanize_count(progress.read_rows as f64 / pb.elapsed().as_secs_f64()),
                        HumanBytes(progress.read_bytes as u64),
                        HumanBytes(progress.total_bytes as u64),
                        HumanBytes(
                            (progress.read_bytes as f64 / pb.elapsed().as_secs_f64()) as u64
                        )
                    ));
                }
                Ok(RowWithProgress::Row(row)) => {
                    rows.push(row);
                    self.rows += 1;
                }
                Err(e) => {
                    eprintln!("error: {}", e);
                    break;
                }
            }
        }

        if let Some(pb) = self.progress.take() {
            pb.finish_and_clear();
        }
        print_rows(self.schema.clone(), &rows, self.settings)?;

        println!();

        let rows_str = if self.rows > 1 { "rows" } else { "row" };
        println!(
            "{} {} in {:.3} sec. Processed {} rows, {} ({} rows/s, {}/s)",
            self.rows,
            rows_str,
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

pub struct FormatDisplay {
    _schema: SchemaRef,
    data: RowProgressIterator,
    rows: usize,
}

impl FormatDisplay {
    pub fn new(schema: SchemaRef, data: RowProgressIterator) -> Self {
        Self {
            _schema: schema,
            data,
            rows: 0,
        }
    }
}

#[async_trait::async_trait]
impl ChunkDisplay for FormatDisplay {
    async fn display(&mut self) -> Result<()> {
        let mut rows = Vec::new();
        while let Some(line) = self.data.next().await {
            match line {
                Ok(RowWithProgress::Row(row)) => {
                    rows.push(row);
                    self.rows += 1;
                }
                Ok(_) => {}
                Err(err) => {
                    eprintln!("error: {}", err);
                    break;
                }
            }
        }
        let mut wtr = csv::WriterBuilder::new()
            .delimiter(b'\t')
            .quote_style(csv::QuoteStyle::NonNumeric)
            .from_writer(std::io::stdout());
        for row in rows {
            let values: Vec<String> = row.values().iter().map(|v| v.to_string()).collect();
            wtr.write_record(values)?;
        }
        Ok(())
    }

    fn total_rows(&self) -> usize {
        self.rows
    }
}

fn print_rows(schema: SchemaRef, results: &[Row], _settings: &Settings) -> Result<()> {
    if !results.is_empty() {
        println!("{}", create_table(schema, results)?);
    }
    Ok(())
}

/// Convert a series of rows into a table
fn create_table(schema: SchemaRef, results: &[Row]) -> Result<Table> {
    let mut table = Table::new();
    table.load_preset("││──├─┼┤│    ──┌┐└┘");
    if results.is_empty() {
        return Ok(table);
    }

    let mut header = Vec::with_capacity(schema.fields().len());
    let mut aligns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let cell = Cell::new(format!("{}\n{}", field.name, field.data_type,))
            .set_alignment(CellAlignment::Center);

        header.push(cell);

        if field.data_type.is_numeric() {
            aligns.push(CellAlignment::Right);
        } else {
            aligns.push(CellAlignment::Left);
        }
    }
    table.set_header(header);

    for row in results {
        let mut cells = Vec::new();
        let values = row.values();
        for (idx, align) in aligns.iter().enumerate() {
            let cell = Cell::new(&values[idx]).set_alignment(*align);
            cells.push(cell);
        }
        table.add_row(cells);
    }

    Ok(table)
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
