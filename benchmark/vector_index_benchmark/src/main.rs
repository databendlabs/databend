use databend_driver::new_connection;
use futures_util::StreamExt;
use prettytable::row;
use prettytable::Cell;
use prettytable::Row;
use prettytable::Table;
#[tokio::main]
async fn main() {
    let mut table = Table::new();
    table.add_row(row![
        "num_points",
        "dim",
        "k",
        "data size",
        "query time without index",
        "nlists",
        "build index time",
        "query time with ivf index",
        "recall"
    ]);
    table.add_row(bench(1_000_000, 128, 50, 1000).await);
    table.printstd();
}

async fn bench(num_points: usize, dim: usize, k: usize, nlists: usize) -> Row {
    let mut row = Row::empty();

    row.add_cell(Cell::new(&format_num(num_points)));
    row.add_cell(Cell::new(&dim.to_string()));
    row.add_cell(Cell::new(&k.to_string()));
    let bytes = num_points * dim * std::mem::size_of::<f32>();
    row.add_cell(Cell::new(&format_size(bytes)));

    let points = generate_points(num_points, dim);
    let target = generate_points(1, dim);

    let dsn = "databend://root:@localhost:8000/default?sslmode=disable";
    let conn = new_connection(dsn).unwrap();
    conn.exec("DROP TABLE IF EXISTS vector_index_benchmark_table")
        .await
        .unwrap();
    conn.exec("CREATE TABLE vector_index_benchmark_table (c array(float32))")
        .await
        .unwrap();

    conn.exec(&insert_array(&points, dim)).await.unwrap();

    let knn_sql = format!(
        "SELECT * FROM vector_index_benchmark_table ORDER BY cosine_distance(c,{:?}) LIMIT {}",
        target, k
    );
    let mut exact_result = Vec::with_capacity(k);
    let start = quanta::Instant::now();
    let mut stream = conn.query_iter(&knn_sql).await.unwrap();
    while let Some(block) = stream.next().await {
        exact_result.push(block.unwrap());
    }
    let elapsed = start.elapsed();
    row.add_cell(Cell::new(&format_time(elapsed.as_nanos() as usize)));
    row.add_cell(Cell::new(&nlists.to_string()));

    let start = quanta::Instant::now();
    conn.exec(" create index on t using ivfflat (c cosine) with (nlist=1);")
        .await
        .unwrap();
    let elapsed = start.elapsed();
    row.add_cell(Cell::new(&format_time(elapsed.as_nanos() as usize)));

    let mut index_result = Vec::with_capacity(k);
    let start = quanta::Instant::now();
    let mut stream = conn.query_iter(&knn_sql).await.unwrap();
    while let Some(block) = stream.next().await {
        index_result.push(block.unwrap());
    }
    let elapsed = start.elapsed();
    row.add_cell(Cell::new(&format_time(elapsed.as_nanos() as usize)));

    let mut recall = 0.0;
    let exact_result: Vec<String> = exact_result.iter().map(|x| format!("{:?}", x)).collect();
    let index_result: Vec<String> = index_result.iter().map(|x| format!("{:?}", x)).collect();
    for ref i in index_result {
        if exact_result.contains(i) {
            recall += 1.0;
        }
    }
    let recall = recall / k as f64;
    row.add_cell(Cell::new(&format!("{:.2}", recall)));
    row
}

fn insert_array(points: &[f32], dim: usize) -> String {
    let mut insert_sql = String::from("INSERT INTO vector_index_benchmark_table VALUES ");
    for i in 0..points.len() / dim {
        let start = i * dim;
        let end = start + dim;
        let point = &points[start..end];
        insert_sql.push_str(&format!("({:?})", point));
        if i != points.len() / dim - 1 {
            insert_sql.push(',');
        }
    }
    insert_sql
}

fn format_num(num: usize) -> String {
    let mut num = num.to_string();
    let mut i = num.len();
    while i > 3 {
        i -= 3;
        num.insert(i, '_');
    }
    num
}

fn format_size(num: usize) -> String {
    const UNITS: [&str; 7] = ["B", "KB", "MB", "GB", "TB", "PB", "EB"];
    let mut num = num as f64;
    let mut i = 0;
    while num >= 1024.0 {
        num /= 1024.0;
        i += 1;
    }
    format!("{:.2}{}", num, UNITS[i])
}

fn format_time(num: usize) -> String {
    const UNITS: [&str; 4] = ["ns", "us", "ms", "s"];
    let mut num = num as f64;
    let mut i = 0;
    while num >= 1000.0 {
        num /= 1000.0;
        i += 1;
    }
    format!("{:.2}{}", num, UNITS[i])
}

fn generate_points(n: usize, d: usize) -> Vec<f32> {
    let mut points = Vec::new();
    for _ in 0..n * d {
        points.push(rand::random::<f32>());
    }
    points
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test() {
        assert_eq!(format_num(1), "1");
        assert_eq!(format_num(10), "10");
        assert_eq!(format_num(100), "100");
        assert_eq!(format_num(1000), "1_000");
        assert_eq!(format_num(10000), "10_000");

        assert_eq!(format_size(1), "1.00B");
        assert_eq!(format_size(1024), "1.00KB");
        assert_eq!(format_size(1024 * 1024), "1.00MB");
        assert_eq!(format_size(1024 * 1024 * 1024), "1.00GB");
        assert_eq!(format_size(1024 * 1024 * 1024 * 1024), "1.00TB");

        assert_eq!(format_time(1), "1.00ns");
        assert_eq!(format_time(1000), "1.00us");
        assert_eq!(format_time(1000 * 1000), "1.00ms");
        assert_eq!(format_time(1000 * 1000 * 1000), "1.00s");

        assert_eq!(
            insert_array(&vec![1.0, 2.0, 3.0, 4.0], 2),
            "INSERT INTO vector_index_benchmark_table VALUES ([1.0, 2.0]),([3.0, 4.0])"
        );
    }
}
