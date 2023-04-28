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

use crate::{
    ast::format_query,
    config::{OutputFormat, Settings},
    helper::CliHelper,
};

#[async_trait::async_trait]
pub trait ChunkDisplay {
    async fn display(&mut self) -> Result<()>;
    fn total_rows(&self) -> usize;
}

pub struct FormatDisplay<'a> {
    settings: &'a Settings,
    query: &'a str,
    schema: SchemaRef,
    data: RowProgressIterator,

    rows: usize,
    progress: Option<ProgressBar>,
    start: Instant,
    stats: Option<QueryProgress>,
}

impl<'a> FormatDisplay<'a> {
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
            stats: None,
        }
    }
}

impl<'a> FormatDisplay<'a> {
    async fn display_progress(&mut self, pg: &QueryProgress) {
        if self.settings.show_progress {
            let pgo = self.progress.take();
            self.progress = Some(display_read_progress(pgo, pg));
        }
    }

    async fn display_table(&mut self) -> Result<()> {
        if self.settings.display_pretty_sql {
            let format_sql = format_query(self.query);
            let format_sql = CliHelper::new().highlight(&format_sql, format_sql.len());
            println!("\n{}\n", format_sql);
        }
        let mut rows = Vec::new();
        while let Some(line) = self.data.next().await {
            match line {
                Ok(RowWithProgress::Row(row)) => {
                    self.rows += 1;
                    rows.push(row);
                }
                Ok(RowWithProgress::Progress(pg)) => {
                    self.display_progress(&pg).await;
                    self.stats = Some(pg);
                }
                Err(err) => {
                    eprintln!("error: {}", err);
                    break;
                }
            }
        }
        if let Some(pb) = self.progress.take() {
            pb.finish_and_clear();
        }
        if !rows.is_empty() {
            println!("{}", create_table(self.schema.clone(), &rows)?);
        }
        Ok(())
    }

    async fn display_csv(&mut self) -> Result<()> {
        let mut wtr = csv::WriterBuilder::new()
            .quote_style(csv::QuoteStyle::Necessary)
            .from_writer(std::io::stdout());
        while let Some(line) = self.data.next().await {
            match line {
                Ok(RowWithProgress::Row(row)) => {
                    self.rows += 1;
                    let record = row.into_iter().map(|v| v.to_string()).collect::<Vec<_>>();
                    wtr.write_record(record)?;
                }
                Ok(RowWithProgress::Progress(pg)) => {
                    self.stats = Some(pg);
                }
                Err(err) => {
                    eprintln!("error: {}", err);
                    break;
                }
            }
        }
        Ok(())
    }

    async fn display_tsv(&mut self) -> Result<()> {
        let mut wtr = csv::WriterBuilder::new()
            .delimiter(b'\t')
            .quote_style(csv::QuoteStyle::Necessary)
            .from_writer(std::io::stdout());
        while let Some(line) = self.data.next().await {
            match line {
                Ok(RowWithProgress::Row(row)) => {
                    self.rows += 1;
                    let record = row.into_iter().map(|v| v.to_string()).collect::<Vec<_>>();
                    wtr.write_record(record)?;
                }
                Ok(RowWithProgress::Progress(pg)) => {
                    self.stats = Some(pg);
                }
                Err(err) => {
                    eprintln!("error: {}", err);
                    break;
                }
            }
        }
        Ok(())
    }

    async fn display_null(&mut self) -> Result<()> {
        while let Some(line) = self.data.next().await {
            match line {
                Ok(RowWithProgress::Row(_)) => {
                    self.rows += 1;
                }
                Ok(RowWithProgress::Progress(pg)) => {
                    self.display_progress(&pg).await;
                    self.stats = Some(pg);
                }
                Err(err) => {
                    eprintln!("error: {}", err);
                    break;
                }
            }
        }
        if let Some(pb) = self.progress.take() {
            pb.finish_and_clear();
        }
        Ok(())
    }

    async fn display_stats(&mut self) {
        if !self.settings.show_stats {
            return;
        }
        if let Some(ref mut stats) = self.stats {
            stats.normalize();
            let rows_str = if self.rows > 1 { "rows" } else { "row" };
            eprintln!(
                "{} {} in {:.3} sec. Processed {} rows, {} ({} rows/s, {}/s)",
                self.rows,
                rows_str,
                self.start.elapsed().as_secs_f64(),
                humanize_count(stats.total_rows as f64),
                HumanBytes(stats.total_rows as u64),
                humanize_count(stats.total_rows as f64 / self.start.elapsed().as_secs_f64()),
                HumanBytes((stats.total_bytes as f64 / self.start.elapsed().as_secs_f64()) as u64),
            );
            eprintln!();
        }
    }
}

#[async_trait::async_trait]
impl<'a> ChunkDisplay for FormatDisplay<'a> {
    async fn display(&mut self) -> Result<()> {
        match self.settings.output_format {
            OutputFormat::Table => {
                self.display_table().await?;
            }
            OutputFormat::CSV => {
                self.display_csv().await?;
            }
            OutputFormat::TSV => {
                self.display_tsv().await?;
            }
            OutputFormat::Null => {
                self.display_null().await?;
            }
        }
        self.display_stats().await;
        Ok(())
    }

    fn total_rows(&self) -> usize {
        self.rows
    }
}

fn format_read_progress(progress: &QueryProgress, elapsed: f64) -> String {
    format!(
        "Processing {}/{} ({} rows/s), {}/{} ({}/s)",
        humanize_count(progress.read_rows as f64),
        humanize_count(progress.total_rows as f64),
        humanize_count(progress.read_rows as f64 / elapsed),
        HumanBytes(progress.read_bytes as u64),
        HumanBytes(progress.total_bytes as u64),
        HumanBytes((progress.read_bytes as f64 / elapsed) as u64)
    )
}

pub fn format_write_progress(progress: &QueryProgress, elapsed: f64) -> String {
    format!(
        "Written {} ({} rows/s), {} ({}/s)",
        humanize_count(progress.write_rows as f64),
        humanize_count(progress.write_rows as f64 / elapsed),
        HumanBytes(progress.write_bytes as u64),
        HumanBytes((progress.write_bytes as f64 / elapsed) as u64)
    )
}

fn display_read_progress(pb: Option<ProgressBar>, current: &QueryProgress) -> ProgressBar {
    let pb = pb.unwrap_or_else(|| {
        let pbn = ProgressBar::new(current.total_bytes as u64);
        let progress_color = "green";
        let template = "{spinner:.${progress_color}} [{elapsed_precise}] {msg} {wide_bar:.${progress_color}/blue} ({eta})".replace("${progress_color}", progress_color);
        pbn.set_style(
            ProgressStyle::with_template(&template)
                .unwrap()
                .with_key("eta", |state: &ProgressState, w: &mut dyn Write| {
                    write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap()
                })
                .progress_chars("█▓▒░ "),
        );
        pbn
    });

    pb.set_position(current.read_bytes as u64);
    pb.set_message(format_read_progress(current, pb.elapsed().as_secs_f64()));
    pb
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
