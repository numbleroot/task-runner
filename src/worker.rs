use base64::prelude::*;
use futures_util::StreamExt;
use pbkdf2::password_hash::PasswordHasher;
use tracing::{Level, event};

#[derive(Debug, Clone)]
struct WorkerWebhook {
    id: String,
    execution_time: String,
    url: String,
    body: String,
}

#[derive(Debug, Clone)]
struct WorkerHash {
    id: String,
    execution_time: String,
    secret: String,
}

#[allow(clippy::too_many_lines)]
/// Handles a webhook task by ensuring it is time to execute it or otherwise
/// resetting its state to 'todo'. Upon successful POST of the task's body to
/// the task's URL, prints the obtained HTTP status code.
async fn handle_webhook(db_pool: sqlx::sqlite::SqlitePool, task: WorkerWebhook) {
    // Parse 'execution_time' field from webhooks database as RFC 3339 datetime.
    // This can't fail, as we're only ever inserting valid RFC 3339 datetimes
    // through the HTTP API.
    let Ok(execution_time) = chrono::DateTime::parse_from_rfc3339(&task.execution_time) else {
        event!(
            Level::WARN,
            "Failed to parse 'execution_time' for webhook as RFC 3339: {}",
            &task.execution_time,
        );

        // In case of failure, permanently mark this task's state as 'failed'.
        let task_id = task.id.clone();
        match sqlx::query!(
            "UPDATE webhooks \
            SET state = 'failed' \
            WHERE id = $1 AND state = 'todo';",
            task_id,
        )
        .execute(&db_pool)
        .await
        {
            Ok(_) => (),
            Err(e) => {
                event!(
                    Level::WARN,
                    "Worker failed to set 'state' for webhook task '{}' to 'failed': {e}",
                    &task.id,
                );
            }
        }
        return;
    };

    // If the time to handle this webhook task has not yet come, wait a bit.
    while chrono::Utc::now().fixed_offset() < execution_time {
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    // Immediately mark this task's state as `in_progress` as long as it is still in
    // state `todo`. Due to `SQLite`'s isolation features (serializing writes, i.e.,
    // parallel writers need to take turns), this means that no two tokio tasks
    // entering this handler at the same time will also both proceed beyond this
    // "barrier". Only one of them will while the other won't due to the now
    // incorrect `state = 'todo'` condition. This prevents the situation where the
    // same task is handled by more than one worker task concurrently.
    let task_id = task.id.clone();
    let res = match sqlx::query!(
        "UPDATE webhooks \
        SET state = 'in_progress' \
        WHERE id = $1 AND state = 'todo';",
        task_id,
    )
    .execute(&db_pool)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            event!(
                Level::WARN,
                "Worker failed to set 'state' for webhook task '{}' to 'in_progress': {e}",
                &task.id,
            );
            return;
        }
    };

    if res.rows_affected() != 1 {
        event!(
            Level::DEBUG,
            "Another task is already handling the POST request to '{}'...",
            &task.url,
        );
        return;
    }

    // The time to handle this webhook task has arrived, handle it.
    event!(Level::DEBUG, "Handling POST request to '{}'...", &task.url);

    let mut tries: usize = 1;
    let mut backoff_f: u64 = 1;
    let mut res = reqwest::Client::new()
        .post(&task.url)
        .body(task.body.clone())
        .send()
        .await;

    while res.is_err() && tries <= 5 {
        event!(
            Level::DEBUG,
            "Attempt {tries} / 5 to send POST to '{}' failed, backing off and retrying...",
            &task.url
        );
        let () = tokio::time::sleep(tokio::time::Duration::from_millis(100 * backoff_f)).await;
        res = reqwest::Client::new()
            .post(&task.url)
            .body(task.body.clone())
            .send()
            .await;
        tries += 1;
        backoff_f *= 2;
    }

    let res = match res {
        Ok(r) => r,
        Err(e) => {
            event!(
                Level::WARN,
                "Attempt {tries} / 5 to send POST to '{}' failed with (no further retries): {e}",
                &task.url
            );
            let task_id = task.id.clone();
            match sqlx::query!(
                "UPDATE webhooks \
                SET state = 'failed' \
                WHERE id = $1;",
                task_id,
            )
            .execute(&db_pool)
            .await
            {
                Ok(_) => {
                    event!(
                        Level::DEBUG,
                        "Worker set 'state' for webhook task '{}' to 'failed'",
                        &task.id,
                    );
                    return;
                }
                Err(e) => {
                    event!(
                        Level::WARN,
                        "Worker failed to set 'state' for webhook task '{}' to 'failed': {e}",
                        &task.id,
                    );
                    return;
                }
            }
        }
    };

    event!(
        Level::INFO,
        "POST request to '{}' yielded HTTP status code: {}",
        &task.url,
        res.status().as_str(),
    );

    // Request was successful, mark this task's state as 'done'.
    let task_id = task.id.clone();
    match sqlx::query!(
        "UPDATE webhooks \
        SET state = 'done' \
        WHERE id = $1;",
        task_id,
    )
    .execute(&db_pool)
    .await
    {
        Ok(_) => {
            event!(
                Level::DEBUG,
                "Worker set 'state' for webhook task '{}' to 'done'",
                &task.id,
            );
        }
        Err(e) => {
            event!(
                Level::WARN,
                "Worker failed to set 'state' for webhook task '{}' to 'done': {e}",
                &task.id,
            );
        }
    }
}

#[allow(clippy::too_many_lines)]
/// Handles a hash task by ensuring it is time to execute it or otherwise
/// resetting its state to 'todo'. Upon obtaining the desired hash of the secret
/// value, prints it in base64.
async fn handle_hash(db_pool: sqlx::sqlite::SqlitePool, task: WorkerHash) {
    // Parse 'execution_time' field from hashes database as RFC 3339 datetime.
    // This can't fail, as we're only ever inserting valid RFC 3339 datetimes
    // through the HTTP API.
    let Ok(execution_time) = chrono::DateTime::parse_from_rfc3339(&task.execution_time) else {
        event!(
            Level::WARN,
            "Failed to parse 'execution_time' for hash as RFC 3339: {}",
            &task.execution_time,
        );

        // In case of failure, permanently mark this task's state as 'failed'.
        let task_id = task.id.clone();
        match sqlx::query!(
            "UPDATE hashes \
            SET state = 'failed' \
            WHERE id = $1 AND state = 'todo';",
            task_id,
        )
        .execute(&db_pool)
        .await
        {
            Ok(_) => (),
            Err(e) => {
                event!(
                    Level::WARN,
                    "Worker failed to set 'state' for hash task '{}' to 'failed': {e}",
                    &task.id,
                );
            }
        }
        return;
    };

    // If the time to handle this hash task has not yet come, wait a bit.
    while chrono::Utc::now().fixed_offset() < execution_time {
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    // Immediately mark this task's state as `in_progress` as long as it is still in
    // state `todo`. Due to `SQLite`'s isolation features (serializing writes, i.e.,
    // parallel writers need to take turns), this means that no two tokio tasks
    // entering this handler at the same time will also both proceed beyond this
    // "barrier". Only one of them will while the other won't due to the now
    // incorrect `state = 'todo'` condition. This prevents the situation where the
    // same task is handled by more than one worker task concurrently.
    let task_id = task.id.clone();
    let res = match sqlx::query!(
        "UPDATE hashes \
        SET state = 'in_progress' \
        WHERE id = $1 AND state = 'todo';",
        task_id,
    )
    .execute(&db_pool)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            event!(
                Level::WARN,
                "Worker failed to set 'state' for hash task '{}' to 'in_progress': {e}",
                &task.id,
            );
            return;
        }
    };

    if res.rows_affected() != 1 {
        event!(
            Level::DEBUG,
            "Another task is already handling the hash task for '{}'...",
            &task.secret,
        );
        return;
    }

    // Time to handle this hash task has arrived, handle it.
    event!(Level::DEBUG, "Handling hash task for '{}'...", &task.secret);

    let secret = task.secret.as_bytes().to_vec();
    let hash = match tokio::task::spawn_blocking(move || {
        let salt = pbkdf2::password_hash::SaltString::generate(&mut rand::rngs::OsRng);
        match pbkdf2::Pbkdf2.hash_password_customized(
            &secret,
            None,
            None,
            pbkdf2::Params {
                rounds: 600_000,
                output_length: 32,
            },
            &salt,
        ) {
            Ok(h) => h.to_string(),
            Err(e) => e.to_string(),
        }
    })
    .await
    {
        Ok(h) => h,
        Err(e) => {
            event!(
                Level::WARN,
                "Computing the PBKDF2 hash value for secret '{}' failed: {e}",
                &task.secret,
            );

            // Finalize this task's state to 'failed'.
            let task_id = task.id.clone();
            match sqlx::query!(
                "UPDATE hashes \
                SET state = 'failed' \
                WHERE id = $1;",
                task_id,
            )
            .execute(&db_pool)
            .await
            {
                Ok(_) => {
                    event!(
                        Level::DEBUG,
                        "Worker set 'state' for hash task '{}' to 'failed'",
                        &task.id,
                    );
                    return;
                }
                Err(e) => {
                    event!(
                        Level::WARN,
                        "Worker failed to set 'state' for hash task '{}' to 'failed': {e}",
                        &task.id,
                    );
                    return;
                }
            }
        }
    };

    let hash_base64 = BASE64_STANDARD.encode(hash);
    event!(
        Level::INFO,
        "Base64-encoded hash of secret '{}' obtained with PBKDF2: '{}'",
        &task.secret,
        hash_base64,
    );

    // Request was successful, mark this task's state as 'done'.
    let task_id = task.id.clone();
    match sqlx::query!(
        "UPDATE hashes \
        SET state = 'done' \
        WHERE id = $1;",
        task_id,
    )
    .execute(&db_pool)
    .await
    {
        Ok(_) => {
            event!(
                Level::DEBUG,
                "Worker set 'state' for hash task '{}' to 'done'",
                &task.id,
            );
        }
        Err(e) => {
            event!(
                Level::WARN,
                "Worker failed to set 'state' for hash task '{}' to 'done': {e}",
                &task.id,
            );
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct WorkerCtx {
    db_pool: sqlx::sqlite::SqlitePool,
}

impl WorkerCtx {
    pub(crate) fn new(db_pool: sqlx::sqlite::SqlitePool) -> Self {
        WorkerCtx { db_pool }
    }

    pub(crate) async fn run(
        self,
        mut recv_shutdown: tokio::sync::broadcast::Receiver<()>,
        mut recv_task: tokio::sync::mpsc::Receiver<(tokio::time::Duration, crate::api::Task)>,
    ) {
        let mut delay_queue = tokio_util::time::DelayQueue::<crate::api::Task>::new();
        loop {
            tokio::select! {
                Some((at_time, task)) = recv_task.recv() => {
                    event!(Level::DEBUG, "Inserting task into delay queue...");
                    delay_queue.insert(task, at_time);
                }
                Some(ready) = delay_queue.next() => {
                    match ready.get_ref() {
                        crate::api::Task::Webhook(wh) => {
                            event!(Level::DEBUG, "A webhook task is ready now!");
                            tokio::task::spawn(handle_webhook(self.db_pool.clone(), WorkerWebhook{
                                id: wh.id.clone(),
                                execution_time: wh.execution_time.clone(),
                                url: wh.url.clone(),
                                body: wh.body.clone(),
                            }));
                        }
                        crate::api::Task::Hash(h) => {
                            event!(Level::DEBUG, "A hash task is ready now!");
                            tokio::task::spawn(handle_hash(self.db_pool.clone(), WorkerHash{
                                id: h.id.clone(),
                                execution_time: h.execution_time.clone(),
                                secret: h.secret.clone(),
                            }));
                        }
                    }
                }
                _ = recv_shutdown.recv() => {
                    event!(Level::DEBUG, "Worker shutting down...");
                    return;
                }
            }
        }
    }
}
