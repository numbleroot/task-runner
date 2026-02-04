use tracing::{Level, event};

#[derive(Debug, Clone)]
pub(crate) struct ApiCtx {
    db_pool: sqlx::sqlite::SqlitePool,
    send_task: tokio::sync::mpsc::Sender<(tokio::time::Duration, Task)>,
}

impl ApiCtx {
    pub(crate) fn new(
        db_pool: sqlx::sqlite::SqlitePool,
        send_task: tokio::sync::mpsc::Sender<(tokio::time::Duration, Task)>,
    ) -> Self {
        ApiCtx { db_pool, send_task }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct ApiWebhook {
    pub(crate) id: String,
    pub(crate) state: String,
    pub(crate) execution_time: String,
    pub(crate) url: String,
    pub(crate) body: String,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct ApiHash {
    pub(crate) id: String,
    pub(crate) state: String,
    pub(crate) execution_time: String,
    pub(crate) secret: String,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Task {
    Webhook(ApiWebhook),
    Hash(ApiHash),
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ReqPostTasksNew {
    #[serde(alias = "Webhook", alias = "WebHook")]
    Webhook {
        execution_time: String,
        url: String,
        body: String,
    },
    #[serde(alias = "Hash")]
    Hash {
        execution_time: String,
        secret: String,
    },
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(untagged, rename_all = "snake_case")]
pub(crate) enum RespPostTasksNew {
    Failure { msg: String },
    Success { id: String },
}

enum ApiTimeError {
    NotRfc3339(String),
    InPast(String),
}

impl std::fmt::Display for ApiTimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        match self {
            ApiTimeError::NotRfc3339(e) | ApiTimeError::InPast(e) => write!(f, "{e}"),
        }
    }
}

/// Parse the user-supplied datetime string according to RFC 3339 and, upon
/// success, verify that the obtained datetime lies in the future as of now.
fn validate_execution_time(
    execution_time: &str,
) -> std::result::Result<chrono::DateTime<chrono::FixedOffset>, ApiTimeError> {
    let Ok(execution_time) = chrono::DateTime::parse_from_rfc3339(execution_time) else {
        return Err(ApiTimeError::NotRfc3339(
            "field 'execution_time' must contain a valid RFC 3339 datetime, \
            including timezone, e.g.: '2026-01-30T15:30:00.123456789-06:00'"
                .to_string(),
        ));
    };

    // Determine how far into the future the earliest accepted execution time for
    // this task lies.
    if (execution_time - chrono::Utc::now().fixed_offset()).num_milliseconds() <= 0 {
        return Err(ApiTimeError::InPast(
            "field 'execution_time' must contain a datetime that lies in the future".to_string(),
        ));
    }

    Ok(execution_time)
}

#[allow(clippy::too_many_lines)]
/// Handles the case that the submitted task is a webhook task.
async fn post_tasks_new_webhook(
    api_ctx: ApiCtx,
    execution_time: String,
    url: String,
    body: String,
) -> (axum::http::StatusCode, axum::Json<RespPostTasksNew>) {
    // Parse field 'execution_time' from RFC 3339 format and validate it.
    let execution_time = match validate_execution_time(&execution_time) {
        Ok(t) => t,
        Err(e) => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                axum::Json(RespPostTasksNew::Failure {
                    msg: format!("Malformed 'webhook': {e}"),
                }),
            );
        }
    };
    let execution_time_str = execution_time.to_rfc3339();

    // Make sure field 'url' is not empty.
    if url.is_empty() {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            axum::Json(RespPostTasksNew::Failure {
                msg: "Malformed 'webhook': field 'url' must contain a URL".to_string(),
            }),
        );
    }

    // Prepend 'http://' to URL if it doesn't start with it already.
    let url = if url.starts_with("http://") || url.starts_with("https://") {
        url
    } else {
        format!("http://{url}")
    };

    // Make sure field 'body' is not empty.
    if body.is_empty() {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            axum::Json(RespPostTasksNew::Failure {
                msg: "Malformed 'webhook': field 'body' must contain a request body".to_string(),
            }),
        );
    }

    // Generate a new UUIDv7 for this task.
    let id = uuid::Uuid::now_v7();
    let id_str = id.to_string();

    // Insert new webhook task into database.
    match sqlx::query!(
        "INSERT INTO webhooks ( id, state, execution_time, url, body ) \
        VALUES ( $1, $2, $3, $4, $5 );",
        id_str,
        "todo",
        execution_time_str,
        url,
        body,
    )
    .execute(&api_ctx.db_pool)
    .await
    {
        Ok(_) => {}
        Err(e) => match e {
            sqlx::Error::Database(err_db) if err_db.is_unique_violation() => {
                event!(
                    Level::WARN,
                    "Uniqueness criterion for UUIDv7 violated: {} already in database",
                    id.to_string(),
                );
                return (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    axum::Json(RespPostTasksNew::Failure {
                        msg: "Task with generated ID already exists in database".to_string(),
                    }),
                );
            }
            _ => {
                event!(
                    Level::WARN,
                    "Inserting new webhook task into database failed: {e}"
                );
                return (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    axum::Json(RespPostTasksNew::Failure {
                        msg: "Inserting new webhook task into database failed".to_string(),
                    }),
                );
            }
        },
    }

    let Ok(dur_from_now_millis) =
        u64::try_from((execution_time - chrono::Utc::now().fixed_offset()).num_milliseconds())
    else {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            axum::Json(RespPostTasksNew::Failure {
                msg: "Malformed 'webhook': field 'execution_time' must contain a \
                datetime that lies in the future"
                    .to_string(),
            }),
        );
    };

    if api_ctx
        .send_task
        .send((
            tokio::time::Duration::from_millis(dur_from_now_millis),
            Task::Webhook(ApiWebhook {
                id: id_str,
                state: "todo".to_string(),
                execution_time: execution_time_str,
                url,
                body,
            }),
        ))
        .await
        .is_err()
    {
        event!(
            Level::WARN,
            "Sending new webhook task to delay queue failed"
        );
        return (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(RespPostTasksNew::Failure {
                msg: "Sending new webhook task to delay queue failed".to_string(),
            }),
        );
    }

    (
        axum::http::StatusCode::CREATED,
        axum::Json(RespPostTasksNew::Success { id: id.to_string() }),
    )
}

#[allow(clippy::too_many_lines)]
/// Handles the case that the submitted task is a hash task.
async fn post_tasks_new_hash(
    api_ctx: ApiCtx,
    execution_time: String,
    secret: String,
) -> (axum::http::StatusCode, axum::Json<RespPostTasksNew>) {
    // Parse field 'execution_time' from RFC 3339 format and validate it.
    let execution_time = match validate_execution_time(&execution_time) {
        Ok(t) => t,
        Err(e) => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                axum::Json(RespPostTasksNew::Failure {
                    msg: format!("Malformed 'hash': {e}"),
                }),
            );
        }
    };
    let execution_time_str = execution_time.to_rfc3339();

    // Make sure field 'secret' is not empty.
    if secret.is_empty() {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            axum::Json(RespPostTasksNew::Failure {
                msg: "Malformed 'hash': field 'secret' must contain a string".to_string(),
            }),
        );
    }

    // Generate a new UUIDv7 for this task.
    let id = uuid::Uuid::now_v7();
    let id_str = id.to_string();

    // Insert new hash task into database.
    match sqlx::query!(
        "INSERT INTO hashes ( id, state, execution_time, secret ) \
        VALUES ( $1, $2, $3, $4 );",
        id_str,
        "todo",
        execution_time_str,
        secret,
    )
    .execute(&api_ctx.db_pool)
    .await
    {
        Ok(_) => {}
        Err(e) => match e {
            sqlx::Error::Database(err_db) if err_db.is_unique_violation() => {
                event!(
                    Level::WARN,
                    "Uniqueness criterion for UUIDv7 violated: {} already in database",
                    id.to_string(),
                );
                return (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    axum::Json(RespPostTasksNew::Failure {
                        msg: "Task with generated ID already exists in database".to_string(),
                    }),
                );
            }
            _ => {
                event!(
                    Level::WARN,
                    "Inserting new hash task into database failed: {e}"
                );
                return (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    axum::Json(RespPostTasksNew::Failure {
                        msg: "Inserting new hash task into database failed".to_string(),
                    }),
                );
            }
        },
    }

    let Ok(dur_from_now_millis) =
        u64::try_from((execution_time - chrono::Utc::now().fixed_offset()).num_milliseconds())
    else {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            axum::Json(RespPostTasksNew::Failure {
                msg: "Malformed 'hash': field 'execution_time' must contain a \
                datetime that lies in the future"
                    .to_string(),
            }),
        );
    };

    if api_ctx
        .send_task
        .send((
            tokio::time::Duration::from_millis(dur_from_now_millis),
            Task::Hash(ApiHash {
                id: id_str,
                state: "todo".to_string(),
                execution_time: execution_time_str,
                secret,
            }),
        ))
        .await
        .is_err()
    {
        event!(Level::WARN, "Sending new hash task to delay queue failed");
        return (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(RespPostTasksNew::Failure {
                msg: "Sending new hash task to delay queue failed".to_string(),
            }),
        );
    }

    (
        axum::http::StatusCode::CREATED,
        axum::Json(RespPostTasksNew::Success { id: id.to_string() }),
    )
}

/// Inserts a new task (either webhook or hash) into the respective database
/// table after light validation. Also places a task for the worker task onto
/// the worker queue for handling at the specified execution time.
pub(crate) async fn post_tasks_new(
    axum::extract::State(api_ctx): axum::extract::State<ApiCtx>,
    axum::Json(payload): axum::Json<ReqPostTasksNew>,
) -> (axum::http::StatusCode, axum::Json<RespPostTasksNew>) {
    match payload {
        ReqPostTasksNew::Webhook {
            execution_time,
            url,
            body,
        } => post_tasks_new_webhook(api_ctx, execution_time, url, body).await,
        ReqPostTasksNew::Hash {
            execution_time,
            secret,
        } => post_tasks_new_hash(api_ctx, execution_time, secret).await,
    }
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RespGetTask {
    Failure { msg: String },
    Webhook(ApiWebhook),
    Hash(ApiHash),
}

/// Returns all details about the specified task (webhook or hash) from the
/// respective table. We rely on the property that collisions when generating
/// UUIDs are exceedingly unlikely, and can thus be ignored. If we thus find the
/// task in the `webhooks` table, we do not query the `hashes` table anymore. If
/// we also do not find the task in the `hashes` table, we report this fact to
/// the caller.
pub(crate) async fn get_task(
    axum::extract::State(api_ctx): axum::extract::State<ApiCtx>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> (axum::http::StatusCode, axum::Json<RespGetTask>) {
    match sqlx::query_as!(
        ApiWebhook,
        "SELECT id, state, execution_time, url, body \
        FROM webhooks \
        WHERE id = $1;",
        id,
    )
    .fetch_one(&api_ctx.db_pool)
    .await
    {
        Ok(webhook) => (
            axum::http::StatusCode::OK,
            axum::Json(RespGetTask::Webhook(webhook)),
        ),
        Err(e) => {
            if let sqlx::Error::RowNotFound = e {
                match sqlx::query_as!(
                    ApiHash,
                    "SELECT id, state, execution_time, secret \
                    FROM hashes \
                    WHERE id = $1;",
                    id,
                )
                .fetch_one(&api_ctx.db_pool)
                .await
                {
                    Ok(hash) => (
                        axum::http::StatusCode::OK,
                        axum::Json(RespGetTask::Hash(hash)),
                    ),
                    Err(e) => {
                        if let sqlx::Error::RowNotFound = e {
                            (
                                axum::http::StatusCode::NOT_FOUND,
                                axum::Json(RespGetTask::Failure {
                                    msg: format!("Task '{id}' does not exist"),
                                }),
                            )
                        } else {
                            event!(
                                Level::WARN,
                                "Fetching task '{id}' from 'hashes' table failed: {e}"
                            );
                            (
                                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                                axum::Json(RespGetTask::Failure {
                                    msg: format!("Fetching task '{id}' from 'hashes' table failed"),
                                }),
                            )
                        }
                    }
                }
            } else {
                event!(
                    Level::WARN,
                    "Fetching task '{id}' from 'webhooks' table failed: {e}"
                );
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    axum::Json(RespGetTask::Failure {
                        msg: format!("Fetching task '{id}' from 'webhooks' table failed"),
                    }),
                )
            }
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(untagged, rename_all = "snake_case")]
pub(crate) enum RespGetTasksByState {
    Failure { msg: String },
    Tasks(Vec<Task>),
}

/// Returns the list of tasks (containing potentially both webhook tasks and
/// hash tasks) in the specified state to the caller.
pub(crate) async fn get_tasks_by_state(
    axum::extract::State(api_ctx): axum::extract::State<ApiCtx>,
    axum::extract::Path(state): axum::extract::Path<String>,
) -> (axum::http::StatusCode, axum::Json<RespGetTasksByState>) {
    let state = state.to_lowercase();
    if (state != "todo") && (state != "in_progress") && (state != "failed") && (state != "done") {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            axum::Json(RespGetTasksByState::Failure {
                msg: "Field 'state' needs to be one of: 'todo', 'in_progress', 'failed', 'done'"
                    .to_string(),
            }),
        );
    }

    let webhooks = match sqlx::query_as!(
        ApiWebhook,
        "SELECT id, state, execution_time, url, body \
        FROM webhooks \
        WHERE state = $1 \
        ORDER BY execution_time ASC;",
        state,
    )
    .fetch_all(&api_ctx.db_pool)
    .await
    {
        Ok(t) => t,
        Err(e) => {
            event!(
                Level::WARN,
                "Failed to retrieve webhook tasks from database: {e}"
            );
            return (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(RespGetTasksByState::Failure {
                    msg: "Failed to retrieve webhook tasks from database".to_string(),
                }),
            );
        }
    };

    let hashes = match sqlx::query_as!(
        ApiHash,
        "SELECT id, state, execution_time, secret \
        FROM hashes \
        WHERE state = $1 \
        ORDER BY execution_time ASC;",
        state,
    )
    .fetch_all(&api_ctx.db_pool)
    .await
    {
        Ok(h) => h,
        Err(e) => {
            event!(
                Level::WARN,
                "Failed to retrieve hash tasks from database: {e}"
            );
            return (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(RespGetTasksByState::Failure {
                    msg: "Failed to retrieve hash tasks from database".to_string(),
                }),
            );
        }
    };

    // Create one tasks list by combining the webhooks and the hashes lists.
    let mut tasks = std::vec::Vec::<Task>::with_capacity(webhooks.len() + hashes.len());
    for webhook in webhooks {
        tasks.push(Task::Webhook(webhook));
    }
    for hash in hashes {
        tasks.push(Task::Hash(hash));
    }

    (
        axum::http::StatusCode::OK,
        axum::Json(RespGetTasksByState::Tasks(tasks)),
    )
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(untagged, rename_all = "snake_case")]
pub(crate) enum RespGetTasksByType {
    Failure { msg: String },
    Webhooks(Vec<ApiWebhook>),
    Hashes(Vec<ApiHash>),
}

/// Handles the case that the user requested all webhook tasks.
pub(crate) async fn get_webhooks(
    api_ctx: ApiCtx,
) -> (axum::http::StatusCode, axum::Json<RespGetTasksByType>) {
    let webhooks = match sqlx::query_as!(
        ApiWebhook,
        "SELECT id, state, execution_time, url, body \
        FROM webhooks \
        ORDER BY execution_time ASC;",
    )
    .fetch_all(&api_ctx.db_pool)
    .await
    {
        Ok(t) => t,
        Err(e) => {
            event!(
                Level::WARN,
                "Failed to retrieve webhook tasks from database: {e}"
            );
            return (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(RespGetTasksByType::Failure {
                    msg: "Failed to retrieve webhook tasks from database".to_string(),
                }),
            );
        }
    };

    (
        axum::http::StatusCode::OK,
        axum::Json(RespGetTasksByType::Webhooks(webhooks)),
    )
}

/// Handles the case that the user requested all hash tasks.
pub(crate) async fn get_hashes(
    api_ctx: ApiCtx,
) -> (axum::http::StatusCode, axum::Json<RespGetTasksByType>) {
    let hashes = match sqlx::query_as!(
        ApiHash,
        "SELECT id, state, execution_time, secret \
        FROM hashes \
        ORDER BY execution_time ASC;",
    )
    .fetch_all(&api_ctx.db_pool)
    .await
    {
        Ok(t) => t,
        Err(e) => {
            event!(
                Level::WARN,
                "Failed to retrieve hash tasks from database: {e}"
            );
            return (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(RespGetTasksByType::Failure {
                    msg: "Failed to retrieve hash tasks from database".to_string(),
                }),
            );
        }
    };

    (
        axum::http::StatusCode::OK,
        axum::Json(RespGetTasksByType::Hashes(hashes)),
    )
}

/// Returns all tasks to the user that are of the specified type (webhook or
/// hash). Tasks are ordered by their ID in ascending order, which should mean
/// chronological insertion order.
pub(crate) async fn get_tasks_by_type(
    axum::extract::State(api_ctx): axum::extract::State<ApiCtx>,
    axum::extract::Path(task_type): axum::extract::Path<String>,
) -> (axum::http::StatusCode, axum::Json<RespGetTasksByType>) {
    let task_type = task_type.to_lowercase();
    if task_type == "webhook" {
        get_webhooks(api_ctx).await
    } else if task_type == "hash" {
        get_hashes(api_ctx).await
    } else {
        (
            axum::http::StatusCode::BAD_REQUEST,
            axum::Json(RespGetTasksByType::Failure {
                msg: "Unsupported task type, use either 'webhook' or 'hash'".to_string(),
            }),
        )
    }
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(untagged, rename_all = "snake_case")]
pub(crate) enum RespDeleteTask {
    Failure { msg: String },
    Success {},
}

/// Deletes a task (webhook or hash) from the respective table. We rely on the
/// property that collisions when generating UUIDs are exceedingly unlikely, and
/// can thus be ignored. We thus attempt to delete the task from the webhooks
/// table first and if no row was affected, then from the hashes table. If
/// neither of the two queries succeeded, the task didn't exist and report that
/// back to the caller.
pub(crate) async fn delete_task(
    axum::extract::State(api_ctx): axum::extract::State<ApiCtx>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> (axum::http::StatusCode, axum::Json<RespDeleteTask>) {
    let num_del_webhooks = match sqlx::query!(
        "DELETE FROM webhooks \
        WHERE id = $1 AND state != $2;",
        id,
        "in_progress",
    )
    .execute(&api_ctx.db_pool)
    .await
    {
        Ok(t) => t.rows_affected(),
        Err(e) => {
            event!(
                Level::WARN,
                "Deleting task '{id}' from webhooks table failed with: {e}"
            );
            return (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(RespDeleteTask::Failure {
                    msg: format!("Deleting task '{id}' from webhooks table failed"),
                }),
            );
        }
    };

    if num_del_webhooks >= 1 {
        return (
            axum::http::StatusCode::NO_CONTENT,
            axum::Json(RespDeleteTask::Success {}),
        );
    }

    let num_del_hashes = match sqlx::query!(
        "DELETE FROM hashes \
        WHERE id = $1 AND state != $2;",
        id,
        "in_progress",
    )
    .execute(&api_ctx.db_pool)
    .await
    {
        Ok(t) => t.rows_affected(),
        Err(e) => {
            event!(
                Level::WARN,
                "Deleting task '{id}' from hashes table failed with: {e}"
            );
            return (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(RespDeleteTask::Failure {
                    msg: format!("Deleting task '{id}' from hashes table failed"),
                }),
            );
        }
    };

    if num_del_hashes >= 1 {
        return (
            axum::http::StatusCode::NO_CONTENT,
            axum::Json(RespDeleteTask::Success {}),
        );
    }

    // At this point, it is clear that the task ID doesn't exist. Report this.
    (
        axum::http::StatusCode::BAD_REQUEST,
        axum::Json(RespDeleteTask::Failure {
            msg: format!("Task '{id}' does not exist"),
        }),
    )
}
