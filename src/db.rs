use std::str::FromStr;

#[derive(Debug)]
pub(crate) enum DbError {
    Sqlx(sqlx::Error),
    DateParse(chrono::ParseError),
    ChannelSend(Box<tokio::sync::mpsc::error::SendError<(std::time::Duration, crate::api::Task)>>),
}

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        match self {
            DbError::Sqlx(e) => write!(f, "{e}"),
            DbError::DateParse(e) => write!(f, "{e}"),
            DbError::ChannelSend(e) => write!(f, "{e}"),
        }
    }
}

impl From<sqlx::Error> for DbError {
    fn from(err: sqlx::Error) -> Self {
        Self::Sqlx(err)
    }
}

impl From<chrono::ParseError> for DbError {
    fn from(err: chrono::ParseError) -> Self {
        Self::DateParse(err)
    }
}

impl From<tokio::sync::mpsc::error::SendError<(std::time::Duration, crate::api::Task)>>
    for DbError
{
    fn from(
        err: tokio::sync::mpsc::error::SendError<(std::time::Duration, crate::api::Task)>,
    ) -> Self {
        Self::ChannelSend(Box::new(err))
    }
}

#[derive(Debug, Clone)]
struct DbWebhook {
    id: String,
    state: String,
    execution_time: String,
    url: String,
    body: String,
}

#[derive(Debug, Clone)]
struct DbHash {
    id: String,
    state: String,
    execution_time: String,
    secret: String,
}

/// Initializes a `SQLite` database at the supplied `db_url` location, if one
/// doesn't already exist. Opens up a connection pool to the database and
/// creates the tables required for this task scheduler, if they don't exist
/// already. Returns the connection pool for usage in the scheduler.
pub(crate) async fn init_open_db(
    db_url: &str,
) -> std::result::Result<sqlx::sqlite::SqlitePool, DbError> {
    // Create database if it doesn't exist already.
    let db_opts = sqlx::sqlite::SqliteConnectOptions::from_str(db_url)?.create_if_missing(true);

    // Open up connection pool to database.
    let db_pool = sqlx::sqlite::SqlitePoolOptions::new()
        .connect_with(db_opts)
        .await?;

    // Create table keeping track of webhook tasks, if it doesn't exist already.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS webhooks ( \
            id TEXT PRIMARY KEY NOT NULL, \
            state TEXT NOT NULL, \
            execution_time TEXT NOT NULL, \
            url TEXT NOT NULL, \
            body TEXT NOT NULL \
        ) STRICT;",
    )
    .execute(&db_pool)
    .await?;

    // Create index on `id` field of `webhooks` table.
    sqlx::query("CREATE UNIQUE INDEX IF NOT EXISTS webhooks_id ON webhooks ( id );")
        .execute(&db_pool)
        .await?;

    // Create composite index on fields `state` and `execution_time` in `webhooks`.
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS webhooks_state_time ON webhooks ( state, execution_time );",
    )
    .execute(&db_pool)
    .await?;

    // Create table keeping track of hash tasks, if it doesn't exist already.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS hashes ( \
            id TEXT PRIMARY KEY NOT NULL, \
            state TEXT NOT NULL, \
            execution_time TEXT NOT NULL, \
            secret TEXT NOT NULL \
        ) STRICT;",
    )
    .execute(&db_pool)
    .await?;

    // Create index on `id` field of `hashes` table.
    sqlx::query("CREATE UNIQUE INDEX IF NOT EXISTS hashes_id ON hashes ( id );")
        .execute(&db_pool)
        .await?;

    // Create composite index on fields `state` and `execution_time` in `hashes`.
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS hashes_state_time ON hashes ( state, execution_time );",
    )
    .execute(&db_pool)
    .await?;

    // Reset any `webhook` tasks in state `in_progress` to `todo`.
    sqlx::query!(
        "UPDATE webhooks \
        SET state = 'todo' \
        WHERE state = 'in_progress';",
    )
    .execute(&db_pool)
    .await?;

    // Reset any `hash` tasks in state `in_progress` to `todo`.
    sqlx::query!(
        "UPDATE hashes \
        SET state = 'todo' \
        WHERE state = 'in_progress';",
    )
    .execute(&db_pool)
    .await?;

    Ok(db_pool)
}

/// When the application restarts, the in-memory `DelayQueue` (yielding tasks
/// for handling once their deadline expired) is empty. This would prevent any
/// task from being handled whose execution time expired while the application
/// wasn't running. To remedy this, we populate the `DelayQueue` with all
/// `webhook` and `hash` tasks in state `todo` each time we start up again. Any
/// deadline which now lies in the past is set to 100 milliseconds as of time of
/// consideration.
pub(crate) async fn reinsert_tasks(
    db_pool: &sqlx::sqlite::SqlitePool,
    send_task: tokio::sync::mpsc::Sender<(tokio::time::Duration, crate::api::Task)>,
) -> std::result::Result<(), DbError> {
    // Retrieve all 'todo' webhook tasks.
    let webhooks = sqlx::query_as!(
        DbWebhook,
        "SELECT id, state, execution_time, url, body \
        FROM webhooks \
        WHERE state = 'todo' \
        ORDER BY execution_time ASC;",
    )
    .fetch_all(db_pool)
    .await?;

    for wh in webhooks {
        // Parse specified execution time from RFC 3339 format to chrono DateTime.
        let execution_time = chrono::DateTime::parse_from_rfc3339(&wh.execution_time)?;

        // Obtain number of milliseconds between now and the specified execution time,
        // if the latter lies in the future. If it doesn't, set a default execution time
        // for the task in 100 milliseconds.
        let dur_from_now_millis =
            u64::try_from((execution_time - chrono::Utc::now().fixed_offset()).num_milliseconds())
                .unwrap_or(100u64);

        // Send task with duration for which to wait until it will be yielded by the
        // DelayQueue via channel to worker task managing the DelayQueue for insertion.
        send_task
            .send((
                tokio::time::Duration::from_millis(dur_from_now_millis),
                crate::api::Task::Webhook(crate::api::ApiWebhook {
                    id: wh.id,
                    state: wh.state,
                    execution_time: wh.execution_time,
                    url: wh.url,
                    body: wh.body,
                }),
            ))
            .await?;
    }

    // Conduct the same steps for any `hash` task that is marked 'todo'.
    let hashes = sqlx::query_as!(
        DbHash,
        "SELECT id, state, execution_time, secret \
        FROM hashes \
        WHERE state = 'todo' \
        ORDER BY execution_time ASC;",
    )
    .fetch_all(db_pool)
    .await?;

    for h in hashes {
        let execution_time = chrono::DateTime::parse_from_rfc3339(&h.execution_time)?;

        let dur_from_now_millis =
            u64::try_from((execution_time - chrono::Utc::now().fixed_offset()).num_milliseconds())
                .unwrap_or(100u64);

        send_task
            .send((
                tokio::time::Duration::from_millis(dur_from_now_millis),
                crate::api::Task::Hash(crate::api::ApiHash {
                    id: h.id,
                    state: h.state,
                    execution_time: h.execution_time,
                    secret: h.secret,
                }),
            ))
            .await?;
    }

    Ok(())
}
