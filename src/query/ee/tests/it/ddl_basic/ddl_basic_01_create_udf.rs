use arrow_udf_wasm::build::*;
use databend_common_base::base::tokio;
use databend_common_compress::CompressAlgorithm;
use databend_common_compress::CompressCodec;
use databend_common_exception::Result;
use databend_common_storage::DataOperator;
use databend_enterprise_query::test_kits::context::EESetup;
use databend_query::test_kits::*;
use opendal::Operator;
use tokio::time::sleep;
use tokio::time::Duration;
use uuid::Uuid;

#[tokio::test(flavor = "multi_thread")]
async fn test_udf_js_gcd() -> Result<()> {
    let fixture = TestFixture::setup_with_custom(EESetup::new()).await?;

    // Create table
    fixture
        .execute_command_v2(r#"CREATE FUNCTION gcd (INT, INT) RETURNS BIGINT LANGUAGE javascript HANDLER = 'gcd_js' AS $$
                export function gcd_js(a, b) {
                    while (b != 0) {
                        let t = b;
                        b = a % b;
                        a = t;
                    }
                    return a;
                }
            $$
            "#
        ).await?;

    let res = fixture
        .execute_command_v2(
            r#"
            select 
                number, gcd(number * 3, number * 6) 
            from 
                numbers(5) 
            where number > 0 order by 1;        
            "#,
        )
        .await?;

    log::info!("Response {:#?}", res);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_udf_py_gcd() -> Result<()> {
    let fixture = TestFixture::setup_with_custom(EESetup::new()).await?;

    let py_gcd_function = r#"
def gcd_py(a, b):
    while b != 0:
        a, b = b, a % b
    return a
    "#;

    // Create table
    fixture
        .execute_command_v2(
            &format!(r#"CREATE FUNCTION gcd (INT, INT) RETURNS BIGINT LANGUAGE python HANDLER = 'gcd_py' AS $${}$$"#,
            py_gcd_function)
        ).await?;

    let res = fixture
        .execute_command_v2(
            r#"
            select 
                number, gcd(number * 3, number * 6) 
            from 
                numbers(5) 
            where number > 0 order by 1;        
            "#,
        )
        .await?;

    log::info!("Response {:#?}", res);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_udf_wasm_gcd() -> Result<()> {
    let fixture = TestFixture::setup_with_custom(EESetup::new()).await?;

    let manifest = r#"
[dependencies]
chrono = "0.4"
"#;
    let script = r#"
use arrow_udf::function;

#[function("wasm_gcd(int, int)->int")]
fn wasm_gcd(mut a: i32, mut b: i32) -> i32 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}
"#;

    log::info!("Building WASM blob");
    let code_blob = build(manifest, script).unwrap();
    let code_blob_size_mb = (code_blob.len() as f64) / (1024.0 * 1024.0);
    log::info!("Size of code_blob: {:.2} MB", code_blob_size_mb);
    log::info!(
        "MIME type: {}",
        infer::get(&code_blob)
            .map(|info| info.mime_type().to_string())
            .unwrap_or_else(|| "Unknown".to_string())
    );

    let code_blob = if code_blob_size_mb > 4.0 {
        let mut encoder = CompressCodec::from(CompressAlgorithm::Zstd);
        encoder.compress_all(&code_blob)?
    } else {
        code_blob
    };

    log::info!(
        "Compressed MIME type: {}",
        infer::get(&code_blob)
            .map(|info| info.mime_type().to_string())
            .unwrap_or_else(|| "Unknown".to_string())
    );

    let blocking_operator = DataOperator::instance().operator().blocking();
    let wasm_module_path = format!("test_dir/{}.wasm", Uuid::new_v4().to_string());
    blocking_operator
        .write_with(&wasm_module_path, code_blob)
        .content_type("application/wasm")
        .call()?;

    log::info!("WASM blob compression: {} - Done!", wasm_module_path);
    log::info!(
        "WASM blob stat: {:?}",
        blocking_operator.stat(&wasm_module_path)?
    );
    log::info!(
        "WASM blob stat: {:?}",
        blocking_operator.stat_with(&wasm_module_path).call()?
    );

    let command = format!(
        r#"CREATE FUNCTION wasm_gcd (INT, INT) RETURNS BIGINT LANGUAGE wasm HANDLER = 'wasm_gcd(int4,int4)->int4' AS $${wasm_module_path}$$"#
    );
    log::info!("Create UDF DDL command: {}", command);
    fixture.execute_command(&command).await?;
    log::info!("Create UDF DDL - Done!");

    log::info!("Executing SQL command");
    fixture
        .execute_command(
            r#"
SELECT number, wasm_gcd(number * 3, number * 6)
FROM numbers(5)
WHERE number > 0
ORDER BY 1;
"#,
        )
        .await?;
    log::info!("SQL command execution - Done!");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_udf_wasm_zstd_fibonacci() -> Result<()> {
    let fixture = TestFixture::setup_with_custom(EESetup::new()).await?;

    let manifest = r#"
[dependencies]
chrono = "0.4"
"#;
    let script = r#"
use arrow_udf::function;

#[used]
static GLOBAL_DUMMY_LUT: [u8; 10 * 1024 * 1024] = [0; 10 * 1024 * 1024];

#[function("wasm_gcd(int, int)->int")]
fn wasm_gcd(mut a: i32, mut b: i32) -> i32 {
    println!("Size of GLOBAL_DUMMY_LUT: {}", GLOBAL_DUMMY_LUT.len());
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}

// Least Common Multiple
#[function("wasm_lcm(int, int)->int")]
fn wasm_lcm(a: i32, b: i32) -> i32 {
    let gcd = wasm_gcd(a, b);
    (a * b) / gcd
}

// Factorial
#[function("wasm_factorial(int)->int")]
fn wasm_factorial(n: i32) -> i32 {
    if n <= 1 {
        1
    } else {
        n * wasm_factorial(n - 1)
    }
}

// Fibonacci
#[function("wasm_fibonacci(int)->int")]
fn wasm_fibonacci(n: i32) -> i32 {
    if n <= 1 {
        n
    } else {
        wasm_fibonacci(n - 1) + wasm_fibonacci(n - 2)
    }
}

// Prime Check
#[function("wasm_is_prime(int)->bool")]
fn wasm_is_prime(n: i32) -> bool {
    if n <= 1 {
        return false;
    }
    if n <= 3 {
        return true;
    }
    if n % 2 == 0 || n % 3 == 0 {
        return false;
    }
    let mut i = 5;
    while i * i <= n {
        if n % i == 0 || n % (i + 2) == 0 {
            return false;
        }
        i += 6;
    }
    true
}
"#;

    log::info!("Building WASM blob for fibonacci test");
    let code_blob = build(manifest, script).unwrap();
    let code_blob_size_mb = (code_blob.len() as f64) / (1024.0 * 1024.0);
    log::info!("Size of code_blob: {:.2} MB", code_blob_size_mb);
    log::info!(
        "MIME type: {}",
        infer::get(&code_blob)
            .map(|info| info.mime_type().to_string())
            .unwrap_or_else(|| "Unknown".to_string())
    );

    let code_blob = if code_blob_size_mb > 2.0 {
        let mut encoder = CompressCodec::from(CompressAlgorithm::Zstd);
        encoder.compress_all(&code_blob)?
    } else {
        code_blob
    };

    log::info!(
        "Compressed MIME type: {}",
        infer::get(&code_blob)
            .map(|info| info.mime_type().to_string())
            .unwrap_or_else(|| "Unknown".to_string())
    );

    let blocking_operator = DataOperator::instance().operator().blocking();
    let wasm_module_path = format!("test_dir/{}.wasm", Uuid::new_v4().to_string());
    blocking_operator
        .write_with(&wasm_module_path, code_blob)
        .content_type("application/wasm")
        .call()?;

    log::info!("WASM blob compression: {} - Done!", wasm_module_path);
    log::info!(
        "WASM blob stat: {:?}",
        blocking_operator.stat(&wasm_module_path)?
    );
    log::info!(
        "WASM blob stat: {:?}",
        blocking_operator.stat_with(&wasm_module_path).call()?
    );

    let command = format!(
        r#"CREATE FUNCTION wasm_fibonacci (INT) RETURNS BIGINT LANGUAGE wasm HANDLER = 'wasm_fibonacci(int4)->int4' AS $${wasm_module_path}$$"#
    );
    log::info!("Create UDF DDL command: {}", command);
    fixture.execute_command(&command).await?;
    log::info!("Create UDF DDL - Done!");

    log::info!("Executing SQL command");
    fixture
        .execute_command(
            r#"
SELECT number, wasm_fibonacci(number * 3)
FROM numbers(5)
WHERE number > 0
ORDER BY 1;
"#,
        )
        .await?;
    log::info!("SQL command execution - Done!");

    Ok(())
}
