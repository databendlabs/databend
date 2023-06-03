use databend_driver::new_connection;
use databend_driver::Connection;
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
    table.add_row(bench(10000, 128, 50, 100).await);
    table.printstd();
}

const TABLE_NAME: &str = "ttttttttttttttttttttttttttttttttttttttt";

async fn warmup(dim: usize, k: usize, conn: &dyn Connection) {
    let target = generate_points(1, dim);
    let knn_sql = format!(
        "SELECT * FROM {} ORDER BY cosine_distance(c,{:?}) LIMIT {}",
        TABLE_NAME, target, k
    );
    let _ = conn.exec(&knn_sql);
}

async fn bench(num_points: usize, dim: usize, k: usize, nlists: usize) -> Row {
    let mut row = Row::empty();

    row.add_cell(Cell::new(&format_num(num_points)));
    row.add_cell(Cell::new(&dim.to_string()));
    row.add_cell(Cell::new(&k.to_string()));
    let bytes = num_points * dim * std::mem::size_of::<f32>();
    row.add_cell(Cell::new(&format_size(bytes)));
    println!("generating {} {}d points randomly", num_points, dim);
    let points = generate_points(num_points, dim);
    let words = generate_string(num_points, 20);
    println!("generating points done");
    let target = generate_points(1, dim);

    let dsn = "databend://root:@localhost:8000/default?sslmode=disable";
    let conn = new_connection(dsn).unwrap();
    println!("creating table");
    conn.exec(&format!("DROP TABLE IF EXISTS {}", TABLE_NAME))
        .await
        .unwrap();
    conn.exec(&format!(
        "CREATE TABLE {} (word varchar,c array(float32))",
        TABLE_NAME
    ))
    .await
    .unwrap();
    println!("inserting points");
    conn.exec(&insert(&points, &words, dim, TABLE_NAME))
        .await
        .unwrap();
    println!("inserting points done");
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    // warmup(dim, k, conn.as_ref()).await;
    println!("querying without index");
    let knn_sql = format!(
        "SELECT word FROM {} ORDER BY cosine_distance(c,{:?}) LIMIT {}",
        TABLE_NAME, target, k
    );
    let mut exact_result = Vec::with_capacity(k);
    let start = quanta::Instant::now();
    let mut stream = conn.query_iter(&knn_sql).await.unwrap();
    while let Some(block) = stream.next().await {
        exact_result.push(block.unwrap());
    }
    let elapsed = start.elapsed();
    println!("querying without index done");
    row.add_cell(Cell::new(&format_time(elapsed.as_nanos() as usize)));
    row.add_cell(Cell::new(&nlists.to_string()));

    println!("building index");
    let start = quanta::Instant::now();
    conn.exec(&format!(
        "create index on {} using ivfflat (c cosine) with (nlist={});",
        TABLE_NAME, nlists
    ))
    .await
    .unwrap();
    let elapsed = start.elapsed();
    row.add_cell(Cell::new(&format_time(elapsed.as_nanos() as usize)));
    println!("building index done");
    //////////////////////////////////////////////////////////////////////////////////////////////////////
    // warmup(dim, k, conn.as_ref()).await;
    println!("querying with index");
    let mut index_result = Vec::with_capacity(k);
    let start = quanta::Instant::now();
    let mut stream = conn.query_iter(&knn_sql).await.unwrap();
    while let Some(block) = stream.next().await {
        index_result.push(block.unwrap());
    }
    let elapsed = start.elapsed();
    println!("querying with index done");
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

fn insert(points: &[f32], words: &[String], dim: usize, table_name: &str) -> String {
    let mut insert_sql = format!("INSERT INTO {} VALUES ", table_name);
    for i in 0..points.len() / dim {
        let start = i * dim;
        let end = start + dim;
        let point = &points[start..end];
        insert_sql.push_str(&format!("('{}',{:?})", words[i], points));
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

fn generate_string(n: usize, len: usize) -> Vec<String> {
    let mut strings = Vec::new();
    for _ in 0..n {
        let mut string = String::new();
        for _ in 0..len {
            string.push(rand::random::<char>());
        }
        strings.push(string);
    }
    strings
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
    }
}
