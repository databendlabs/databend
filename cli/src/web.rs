use anyhow::Result;

use actix_web::{App, get, HttpResponse, HttpServer, Responder, web};
use actix_web::middleware::Logger;
use actix_files as fs;
use tokio::net::TcpListener;


struct AppState {
    result: String,
}

#[get("/api/message")]
async fn get_message(data: web::Data<AppState>) -> impl Responder {
    let response = serde_json::json!({
        "result": data.result,
    });
    HttpResponse::Ok().json(response)
}

pub async fn start_server_and_open_browser<'a>(explain_result: String) -> Result<()> {
    let port = find_available_port(8080).await;
    let server = tokio::spawn(async move {
        start_server(port, explain_result.to_string()).await;
    });

    // Open the browser in a separate task
    tokio::spawn(async move {
        if webbrowser::open(&format!("http://127.0.0.1:{}", port)).is_ok() {
            // eprintln!("Browser opened successfully at http://127.0.0.1:{}", port);
        } else {
            println!("Failed to open browser.");
        }
    });

    // Continue with the rest of the code
    server.await.expect("Server task failed");

    Ok(())
}

pub async fn start_server<'a>(port: u16, result: String) {
    let app_state = web::Data::new(AppState {
        result: result.clone(),
    });

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(app_state.clone())
            .service(get_message)
            .service(
                fs::Files::new("/", "./frontend/build").index_file("index.html")
            )
    })
        .bind(("127.0.0.1", port))
        .expect("Cannot bind to port")
        .run()
        .await
        .expect("Server run failed");
}



async fn find_available_port(start: u16) -> u16 {
    let mut port = start;
    loop {
        if TcpListener::bind(("127.0.0.1", port)).await.is_ok() {
            return port;
        }
        port += 1;
    }
}
