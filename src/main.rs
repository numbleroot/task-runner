use clap::Parser;
use tracing::{Level, event};
use tracing_subscriber::prelude::*;

mod api;
mod db;
mod worker;

#[derive(Debug)]
enum AppError {
    Tracing(tracing_subscriber::filter::ParseError),
    Db(crate::db::DbError),
    Io(std::io::Error),
}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        match self {
            AppError::Tracing(e) => write!(f, "{e}"),
            AppError::Db(e) => write!(f, "{e}"),
            AppError::Io(e) => write!(f, "{e}"),
        }
    }
}

impl From<tracing_subscriber::filter::ParseError> for AppError {
    fn from(err: tracing_subscriber::filter::ParseError) -> Self {
        Self::Tracing(err)
    }
}

impl From<crate::db::DbError> for AppError {
    fn from(err: crate::db::DbError) -> Self {
        Self::Db(err)
    }
}

impl From<std::io::Error> for AppError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

#[derive(Debug, Parser)]
#[command(about, author, version)]
struct Args {
    #[arg(long, env, default_value = "sqlite://tasks.db")]
    /// Connection URL to database used to store this scheduler's tasks.
    database_url: String,

    #[arg(long, env, default_value = "127.0.0.1")]
    /// IP address on which the task scheduler's HTTP handler listens.
    listen_ip: String,

    #[arg(long, env, default_value_t = 8080)]
    /// Port on which the task scheduler's HTTP handler listens.
    listen_port: u16,
}

// Properly handle the CTRL+C signal and shut everything down.
async fn shutdown_upon_signal(send_shutdown: tokio::sync::broadcast::Sender<()>) {
    let _ = tokio::signal::ctrl_c().await;
    event!(Level::INFO, "Received signal to shut down gracefully");
    drop(send_shutdown);
}

#[tokio::main]
async fn main() -> std::result::Result<(), AppError> {
    // Accept and parse CLI and ENV arguments.
    let args = Args::parse();

    // Set up tracing.
    let format_layer = tracing_subscriber::fmt::layer()
        .with_file(true)
        .with_line_number(true)
        .compact();
    let filter_layer = tracing_subscriber::EnvFilter::try_from_default_env()
        .or_else(|_| tracing_subscriber::EnvFilter::try_new("info"))?;
    tracing_subscriber::registry()
        .with(filter_layer)
        .with(format_layer)
        .init();
    event!(Level::INFO, "Launching tasker...");

    // Open and potentially initialize our SQLite database.
    let db_pool = db::init_open_db(&args.database_url).await?;

    // Prepare channel which upon dropping one half initiates shutdown.
    let (send_shutdown, _) = tokio::sync::broadcast::channel::<()>(1);

    // Prepare channel for inserting tasks into the DelayQueue we're using for
    // time-based task scheduling.
    let (send_task, recv_task) =
        tokio::sync::mpsc::channel::<(tokio::time::Duration, crate::api::Task)>(256);

    // Create background worker context and tokio task, in which the tasks stored in
    // the database will be handled.
    let worker_ctx = worker::WorkerCtx::new(db_pool.clone());
    let worker_shutdown = send_shutdown.subscribe();
    let worker_hdl = tokio::task::spawn(worker_ctx.run(worker_shutdown, recv_task));

    // Reinsert tasks from database into DelayQueue before making REST API to insert
    // new ones available to clients.
    db::reinsert_tasks(&db_pool, send_task.clone()).await?;

    // Prepare context struct that is passed to each Axum HTTP API handler below.
    let api_ctx = api::ApiCtx::new(db_pool.clone(), send_task);

    // Define all routes and assign the respective handler to each.
    let router = axum::Router::new()
        .without_v07_checks()
        .route(
            "/tasks/new",
            axum::routing::post(crate::api::post_tasks_new),
        )
        .route("/tasks/{id}", axum::routing::get(crate::api::get_task))
        .route(
            "/tasks/state/{state}",
            axum::routing::get(crate::api::get_tasks_by_state),
        )
        .route(
            "/tasks/type/{type}",
            axum::routing::get(crate::api::get_tasks_by_type),
        )
        .route(
            "/tasks/{id}",
            axum::routing::delete(crate::api::delete_task),
        )
        .with_state(api_ctx);

    // Open a TCP socket using tokio, on the configured IP and port.
    let api_sock_url = format!("{}:{}", args.listen_ip, args.listen_port);
    let api_sock = tokio::net::TcpListener::bind(&api_sock_url).await?;
    event!(
        Level::INFO,
        "HTTP API listening for requests on {api_sock_url}...",
    );

    // Respond to HTTP requests on the TCP socket using the defined Axum router.
    axum::serve(api_sock, router)
        .with_graceful_shutdown(shutdown_upon_signal(send_shutdown))
        .await?;

    let _ = worker_hdl.await;
    db_pool.close().await;

    Ok(())
}
