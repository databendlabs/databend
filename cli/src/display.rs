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

use std::collections::HashSet;
use std::fmt::Write;

use anyhow::{anyhow, Result};
use comfy_table::{Cell, CellAlignment, Table};
use databend_driver::{Row, RowStatsIterator, RowWithStats, SchemaRef, ServerStats};
use indicatif::{HumanBytes, ProgressBar, ProgressState, ProgressStyle};
use rustyline::highlight::Highlighter;
use terminal_size::{terminal_size, Width};
use tokio::time::Instant;
use tokio_stream::StreamExt;
use unicode_segmentation::UnicodeSegmentation;

use crate::{
    ast::format_query,
    config::{ExpandMode, OutputFormat, OutputQuoteStyle, Settings},
    helper::CliHelper,
    session::QueryKind,
};

#[async_trait::async_trait]
pub trait ChunkDisplay {
    async fn display(&mut self) -> Result<ServerStats>;
}

pub struct FormatDisplay<'a> {
    settings: &'a Settings,
    query: &'a str,
    kind: QueryKind,
    // whether replace '\n' with '\\n',
    // disable in explain/show create stmts or user config setting false
    replace_newline: bool,
    data: RowStatsIterator,

    rows: usize,
    progress: Option<ProgressBar>,
    start: Instant,
    stats: Option<ServerStats>,
}

impl<'a> FormatDisplay<'a> {
    pub fn new(
        settings: &'a Settings,
        query: &'a str,
        replace_newline: bool,
        start: Instant,
        data: RowStatsIterator,
    ) -> Self {
        Self {
            settings,
            query,
            kind: QueryKind::from(query),
            replace_newline,
            data,
            rows: 0,
            progress: None,
            start,
            stats: None,
        }
    }
}

impl<'a> FormatDisplay<'a> {
    async fn display_progress(&mut self, ss: &ServerStats) {
        if self.settings.show_progress {
            let pb = self.progress.take();
            match self.kind {
                QueryKind::Get | QueryKind::Query => {
                    self.progress = Some(display_progress(pb, ss, "read"));
                }
                QueryKind::Put | QueryKind::Update => {
                    self.progress = Some(display_progress(pb, ss, "write"));
                }
                _ => {}
            }
        }
    }

    async fn display_table(&mut self) -> Result<()> {
        if self.settings.display_pretty_sql {
            let format_sql = format_query(self.query);
            let format_sql = CliHelper::new().highlight(&format_sql, format_sql.len());
            println!("\n{}\n", format_sql);
        }
        let mut rows = Vec::new();
        let mut error = None;
        while let Some(line) = self.data.next().await {
            match line {
                Ok(RowWithStats::Row(row)) => {
                    self.rows += 1;
                    rows.push(row);
                }
                Ok(RowWithStats::Stats(ss)) => {
                    self.display_progress(&ss).await;
                    self.stats = Some(ss);
                }
                Err(err) => {
                    error = Some(err);
                    break;
                }
            }
        }
        if let Some(pb) = self.progress.take() {
            pb.finish_and_clear();
        }
        if let Some(err) = error {
            return Err(anyhow!(
                "error happens after fetched {} rows: {}",
                rows.len(),
                err
            ));
        }
        if rows.is_empty() {
            return Ok(());
        }

        if self.kind == QueryKind::Explain {
            print_explain(&rows)?;
            return Ok(());
        }

        let schema = self.data.schema();
        match self.settings.expand {
            ExpandMode::On => {
                print_expanded(schema, &rows)?;
            }
            ExpandMode::Off => {
                println!(
                    "{}",
                    create_table(
                        schema,
                        &rows,
                        self.replace_newline,
                        self.settings.max_display_rows,
                        self.settings.max_width,
                        self.settings.max_col_width
                    )?
                );
            }
            ExpandMode::Auto => {
                // FIXME: depends on terminal size
                println!(
                    "{}",
                    create_table(
                        schema,
                        &rows,
                        self.replace_newline,
                        self.settings.max_display_rows,
                        self.settings.max_width,
                        self.settings.max_col_width
                    )?
                );
            }
        }

        Ok(())
    }

    async fn display_csv(&mut self) -> Result<()> {
        let quote_style = match self.settings.quote_style {
            OutputQuoteStyle::Always => csv::QuoteStyle::Always,
            OutputQuoteStyle::Necessary => csv::QuoteStyle::Necessary,
            OutputQuoteStyle::NonNumeric => csv::QuoteStyle::NonNumeric,
            OutputQuoteStyle::Never => csv::QuoteStyle::Never,
        };
        let mut wtr = csv::WriterBuilder::new()
            .quote_style(quote_style)
            .from_writer(std::io::stdout());
        while let Some(line) = self.data.next().await {
            match line {
                Ok(RowWithStats::Row(row)) => {
                    self.rows += 1;
                    let record = row.into_iter().map(|v| v.to_string()).collect::<Vec<_>>();
                    wtr.write_record(record)?;
                }
                Ok(RowWithStats::Stats(ss)) => {
                    self.stats = Some(ss);
                }
                Err(err) => {
                    return Err(err.into());
                }
            }
        }
        Ok(())
    }

    async fn display_tsv(&mut self) -> Result<()> {
        let quote_style = match self.settings.quote_style {
            OutputQuoteStyle::Always => csv::QuoteStyle::Always,
            OutputQuoteStyle::Necessary => csv::QuoteStyle::Necessary,
            OutputQuoteStyle::NonNumeric => csv::QuoteStyle::NonNumeric,
            OutputQuoteStyle::Never => csv::QuoteStyle::Never,
        };
        let mut wtr = csv::WriterBuilder::new()
            .delimiter(b'\t')
            .quote(b'"')
            .quote_style(quote_style)
            .from_writer(std::io::stdout());
        while let Some(line) = self.data.next().await {
            match line {
                Ok(RowWithStats::Row(row)) => {
                    self.rows += 1;
                    let record = row.into_iter().map(|v| v.to_string()).collect::<Vec<_>>();
                    wtr.write_record(record)?;
                }
                Ok(RowWithStats::Stats(ss)) => {
                    self.stats = Some(ss);
                }
                Err(err) => {
                    return Err(err.into());
                }
            }
        }
        Ok(())
    }

    async fn display_null(&mut self) -> Result<()> {
        let mut error = None;
        while let Some(line) = self.data.next().await {
            match line {
                Ok(RowWithStats::Row(_)) => {
                    self.rows += 1;
                }
                Ok(RowWithStats::Stats(ss)) => {
                    self.display_progress(&ss).await;
                    self.stats = Some(ss);
                }
                Err(err) => {
                    error = Some(err);
                    break;
                }
            }
        }
        if let Some(pb) = self.progress.take() {
            pb.finish_and_clear();
        }
        if let Some(err) = error {
            return Err(anyhow!(
                "error happens after fetched {} rows: {}",
                self.rows,
                err
            ));
        }
        Ok(())
    }

    async fn display_stats(&mut self) {
        if !self.settings.show_stats {
            return;
        }

        if let Some(ref mut stats) = self.stats {
            stats.normalize();

            let (rows, mut rows_str, kind, total_rows, total_bytes) = match self.kind {
                QueryKind::Explain => (self.rows, "rows", "explain", 0, 0),
                QueryKind::Query => (self.rows, "rows", "read", stats.read_rows, stats.read_bytes),
                QueryKind::Update | QueryKind::AlterUserPassword => (
                    stats.write_rows,
                    "rows",
                    "written",
                    stats.write_rows,
                    stats.write_bytes,
                ),
                QueryKind::Get => (
                    stats.read_rows,
                    "files",
                    "downloaded",
                    stats.read_rows,
                    stats.read_bytes,
                ),
                QueryKind::Put => (
                    stats.write_rows,
                    "files",
                    "uploaded",
                    stats.write_rows,
                    stats.write_bytes,
                ),
            };
            let mut rows_speed_str = rows_str;
            if rows <= 1 {
                rows_str = rows_str.trim_end_matches('s');
            }
            let rows_speed = total_rows as f64 / self.start.elapsed().as_secs_f64();
            if rows_speed <= 1.0 {
                rows_speed_str = rows_speed_str.trim_end_matches('s');
            }
            eprintln!(
                "{} {} {} in {:.3} sec. Processed {} {}, {} ({} {}/s, {}/s)",
                rows,
                rows_str,
                kind,
                self.start.elapsed().as_secs_f64(),
                humanize_count(total_rows as f64),
                rows_str,
                HumanBytes(total_bytes as u64),
                humanize_count(rows_speed),
                rows_speed_str,
                HumanBytes((total_bytes as f64 / self.start.elapsed().as_secs_f64()) as u64),
            );
            eprintln!();
        }
    }
}

#[async_trait::async_trait]
impl<'a> ChunkDisplay for FormatDisplay<'a> {
    async fn display(&mut self) -> Result<ServerStats> {
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
        let stats = self.stats.take().unwrap_or_default();
        Ok(stats)
    }
}

fn format_read_progress(ss: &ServerStats, elapsed: f64) -> String {
    format!(
        "Processing {}/{} ({} rows/s), {}/{} ({}/s)",
        humanize_count(ss.read_rows as f64),
        humanize_count(ss.total_rows as f64),
        humanize_count(ss.read_rows as f64 / elapsed),
        HumanBytes(ss.read_bytes as u64),
        HumanBytes(ss.total_bytes as u64),
        HumanBytes((ss.read_bytes as f64 / elapsed) as u64)
    )
}

pub fn format_write_progress(ss: &ServerStats, elapsed: f64) -> String {
    format!(
        "Written {} ({} rows/s), {} ({}/s)",
        humanize_count(ss.write_rows as f64),
        humanize_count(ss.write_rows as f64 / elapsed),
        HumanBytes(ss.write_bytes as u64),
        HumanBytes((ss.write_bytes as f64 / elapsed) as u64)
    )
}

fn display_progress(pb: Option<ProgressBar>, current: &ServerStats, kind: &str) -> ProgressBar {
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
    match kind {
        "read" => pb.set_message(format_read_progress(current, pb.elapsed().as_secs_f64())),
        "write" => pb.set_message(format_write_progress(current, pb.elapsed().as_secs_f64())),
        _ => {}
    }
    pb
}

// compute render widths
fn compute_render_widths(
    schema: &SchemaRef,
    max_width: usize,
    max_col_width: usize,
    results: &Vec<Vec<String>>,
) -> (Vec<usize>, Vec<i32>) {
    let column_count = schema.fields().len();
    let mut widths = Vec::with_capacity(column_count);
    let mut total_length = 1;

    for field in schema.fields() {
        // head_name = field_name + "\n" + field_data_type
        let col_length = field.name.len().max(field.data_type.to_string().len());
        widths.push(col_length + 3);
    }

    for values in results {
        for (idx, value) in values.iter().enumerate() {
            widths[idx] = widths[idx].max(value.len() + 3);
        }
    }

    for width in &widths {
        // each column has a space at the beginning, and a space plus a pipe (|) at the end
        // hence + 3
        total_length += width;
    }

    let mut pruned_columns = HashSet::new();
    if total_length > max_width {
        for w in &mut widths {
            if *w > max_col_width {
                let max_diff = *w - max_col_width;
                if total_length - max_diff <= max_width {
                    *w -= total_length - max_width;

                    total_length = max_width;
                    break;
                } else {
                    *w = max_col_width;
                    total_length -= max_diff;
                }
            }
        }
        if total_length > max_width {
            // the total length is still too large
            // we need to remove columns!
            // first, we add 6 characters to the total length
            // this is what we need to add the "..." in the middle
            total_length += 6;
            // now select columns to prune
            // we select columns in zig-zag order starting from the middle
            // e.g. if we have 10 columns, we remove #5, then #4, then #6, then #3, then #7, etc
            let mut offset: i32 = 0;
            while total_length > max_width {
                let c = column_count as i32 / 2 + offset;
                if c < 0 {
                    // c < 0 means no column can display
                    return ([3].to_vec(), [-1].to_vec());
                }
                total_length -= widths[c as usize];
                pruned_columns.insert(c);
                if offset >= 0 {
                    offset = -offset - 1;
                } else {
                    offset = -offset;
                }
            }
        }
    }
    let mut added_split_column = false;
    let mut new_widths = vec![];
    let mut column_map = vec![];
    for (c, item) in widths.iter().enumerate().take(column_count) {
        if !pruned_columns.contains(&(c as i32)) {
            column_map.push((c).try_into().unwrap());
            new_widths.push(*item);
        } else if !added_split_column {
            // "..."
            column_map.push(-1);
            new_widths.push(3);
            added_split_column = true;
        }
    }

    (new_widths, column_map)
}

/// Convert a series of rows into a table
fn create_table(
    schema: SchemaRef,
    results: &[Row],
    replace_newline: bool,
    max_rows: usize,
    mut max_width: usize,
    max_col_width: usize,
) -> Result<Table> {
    let mut table = Table::new();
    table.load_preset("││──├─┼┤│    ──┌┐└┘");
    if results.is_empty() {
        return Ok(table);
    }

    let mut widths = vec![];
    let mut column_map = vec![];

    if max_width == 0 {
        let size = terminal_size();
        if let Some((Width(w), _)) = size {
            max_width = w as usize;
        }
    }

    let row_count: usize = results.len();
    let mut rows_to_render = row_count.min(max_rows);
    if !replace_newline {
        max_width = usize::MAX;
        rows_to_render = row_count;
    } else if row_count <= max_rows + 3 {
        // hiding rows adds 3 extra rows
        // so hiding rows makes no sense if we are only slightly over the limit
        // if we are 1 row over the limit hiding rows will actually increase the number of lines we display!
        // in this case render all the rows
        // 	rows_to_render = row_count;
        rows_to_render = row_count;
    }

    let (top_rows, bottom_rows) = if rows_to_render == row_count {
        (row_count, 0usize)
    } else {
        let top_rows = rows_to_render / 2 + (rows_to_render % 2 != 0) as usize;
        (top_rows, rows_to_render - top_rows)
    };

    let mut res_vec: Vec<Vec<String>> = vec![];
    for row in results.iter().take(top_rows) {
        let values = row.values();
        let mut v = vec![];
        for value in values {
            if replace_newline {
                v.push(value.to_string().replace('\n', "\\n"));
            } else {
                v.push(value.to_string());
            }
        }
        res_vec.push(v);
    }

    if bottom_rows != 0 {
        for row in results.iter().skip(row_count - bottom_rows) {
            let values = row.values();
            let mut v = vec![];
            for value in values {
                if replace_newline {
                    v.push(value.to_string().replace('\n', "\\n"));
                } else {
                    v.push(value.to_string());
                }
            }
            res_vec.push(v);
        }
    }

    // "..." take up three lengths
    if max_width > 0 {
        (widths, column_map) =
            compute_render_widths(&schema, max_width, max_col_width + 3, &res_vec);
    }

    let column_count = schema.fields().len();
    let mut header = Vec::with_capacity(column_count);
    let mut aligns = Vec::with_capacity(column_count);

    render_head(schema, &mut widths, &column_map, &mut header, &mut aligns);
    table.set_header(header);

    // render the top rows
    if column_map.is_empty() {
        for values in res_vec.iter().take(top_rows) {
            let mut cells = Vec::new();
            for (idx, align) in aligns.iter().enumerate() {
                let cell = Cell::new(&values[idx]).set_alignment(*align);
                cells.push(cell);
            }
            table.add_row(cells);
        }
    } else {
        for values in res_vec.iter().take(top_rows) {
            let mut cells = Vec::new();
            for (idx, col_index) in column_map.iter().enumerate() {
                if *col_index == -1 {
                    let cell = Cell::new("...").set_alignment(CellAlignment::Center);
                    cells.push(cell);
                } else {
                    let mut value = values[*col_index as usize].clone();
                    if value.len() + 3 > widths[idx] {
                        let element_size = if widths[idx] >= 6 { widths[idx] - 6 } else { 0 };
                        value = String::from_utf8(
                            value
                                .graphemes(true)
                                .take(element_size)
                                .flat_map(|g| g.as_bytes().iter())
                                .copied() // copied converts &u8 into u8
                                .chain(b"...".iter().copied())
                                .collect::<Vec<u8>>(),
                        )
                        .unwrap();
                    }
                    let cell = Cell::new(value).set_alignment(aligns[idx]);
                    cells.push(cell);
                }
            }

            table.add_row(cells);
        }
    }

    // render the bottom rows
    if bottom_rows != 0 {
        // first render the divider
        let mut cells: Vec<Cell> = Vec::new();
        let display_res_len = res_vec.len();
        for align in aligns.iter() {
            let cell = Cell::new("·").set_alignment(*align);
            cells.push(cell);
        }

        for _ in 0..3 {
            table.add_row(cells.clone());
        }
        if column_map.is_empty() {
            for values in res_vec.iter().skip(display_res_len - bottom_rows) {
                let mut cells = Vec::new();
                for (idx, align) in aligns.iter().enumerate() {
                    let cell = Cell::new(&values[idx]).set_alignment(*align);
                    cells.push(cell);
                }
                table.add_row(cells);
            }
        } else {
            for values in res_vec.iter().skip(display_res_len - bottom_rows) {
                let mut cells = Vec::new();
                for (idx, col_index) in column_map.iter().enumerate() {
                    if *col_index == -1 {
                        let cell = Cell::new("...").set_alignment(CellAlignment::Center);
                        cells.push(cell);
                    } else {
                        let mut value = values[*col_index as usize].clone();
                        if value.len() + 3 > widths[idx] {
                            let element_size = if widths[idx] >= 6 { widths[idx] - 6 } else { 0 };
                            value = String::from_utf8(
                                value
                                    .graphemes(true)
                                    .take(element_size)
                                    .flat_map(|g| g.as_bytes().iter())
                                    .copied() // copied converts &u8 into u8
                                    .chain(b"...".iter().copied())
                                    .collect::<Vec<u8>>(),
                            )
                            .unwrap();
                        }
                        let cell = Cell::new(value).set_alignment(aligns[idx]);
                        cells.push(cell);
                    }
                }
                table.add_row(cells);
            }
        }

        let row_count_str = format!("{} rows", row_count);
        let show_count_str = format!("({} shown)", top_rows + bottom_rows);
        table.add_row(vec![Cell::new(row_count_str).set_alignment(aligns[0])]);
        table.add_row(vec![Cell::new(show_count_str).set_alignment(aligns[0])]);
    }

    Ok(table)
}

fn render_head(
    schema: SchemaRef,
    widths: &mut [usize],
    column_map: &[i32],
    header: &mut Vec<Cell>,
    aligns: &mut Vec<CellAlignment>,
) {
    if column_map.is_empty() {
        for field in schema.fields() {
            let cell = Cell::new(format!("{}\n{}", field.name, field.data_type))
                .set_alignment(CellAlignment::Center);

            header.push(cell);

            if field.data_type.is_numeric() {
                aligns.push(CellAlignment::Right);
            } else {
                aligns.push(CellAlignment::Left);
            }
        }
    } else {
        let fields = schema.fields();
        for (idx, col_index) in column_map.iter().enumerate() {
            if *col_index == -1 {
                let cell = Cell::new("···").set_alignment(CellAlignment::Center);
                header.push(cell);
                aligns.push(CellAlignment::Center);
            } else {
                let field = &fields[*col_index as usize];
                let width = widths[idx];
                let mut field_name = field.name.to_string();
                let mut field_data_type = field.data_type.to_string();
                let element_size = if width >= 6 { width - 6 } else { 0 };

                if field_name.len() + 3 > width {
                    field_name = String::from_utf8(
                        field_name
                            .graphemes(true)
                            .take(element_size)
                            .flat_map(|g| g.as_bytes().iter())
                            .copied() // copied converts &u8 into u8
                            .chain(b"...".iter().copied())
                            .collect::<Vec<u8>>(),
                    )
                    .unwrap();
                }
                if field_data_type.len() + 3 > width {
                    field_data_type = String::from_utf8(
                        field_name
                            .graphemes(true)
                            .take(element_size)
                            .flat_map(|g| g.as_bytes().iter())
                            .copied() // copied converts &u8 into u8
                            .chain(b"...".iter().copied())
                            .collect::<Vec<u8>>(),
                    )
                    .unwrap();
                }

                let head_name = format!("{}\n{}", field_name, field_data_type);
                let cell = Cell::new(head_name).set_alignment(CellAlignment::Center);

                header.push(cell);

                if field.data_type.is_numeric() {
                    aligns.push(CellAlignment::Right);
                } else {
                    aligns.push(CellAlignment::Left);
                }
            }
        }
    }
}

fn print_expanded(schema: SchemaRef, results: &[Row]) -> Result<()> {
    let mut head_width = 0;
    for field in schema.fields() {
        if field.name.len() > head_width {
            head_width = field.name.len();
        }
    }
    for (row, result) in results.iter().enumerate() {
        println!("-[ RECORD {} ]-----------------------------------", row + 1);
        for (idx, field) in schema.fields().iter().enumerate() {
            println!("{: >head_width$}: {}", field.name, result.values()[idx]);
        }
    }
    println!();
    Ok(())
}

fn print_explain(results: &[Row]) -> Result<()> {
    println!("-[ EXPLAIN ]-----------------------------------");
    for result in results {
        println!("{}", result.values()[0]);
    }
    println!();
    Ok(())
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
