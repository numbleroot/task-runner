# Task Runner, Using tokio's `DelayQueue`

Implements a task runner to experiment with tokio's [`DelayQueue`](https://docs.rs/tokio-util/latest/tokio_util/time/delay_queue/struct.DelayQueue.html).
Tasks of type `webhook` or `hash` can be added to the SQLite database via a JSON REST API.
A `webhook` task sends an HTTP POST with the specified body to the specified URL, while a `hash` task computes the PBKDF2 hash of the specified secret.
Tasks will be handled by the task runner no earlier than their specified `execution_time`.
This is realized using a `DelayQueue` from the `tokio_util` crate, and this project serves as an opportunity to experiment with that concept.


## Compilation and Running

After cloning the repository, compile the `tasker` binary inside the repository folder:
```bash
cargo build --release
```

Afterwards, `./target/release/tasker` is available with the following options configurable either via CLI or ENV arguments:
```bash
Usage: tasker [OPTIONS]

Options:
      --database-url <DATABASE_URL>  Connection URL to database used to store this scheduler's tasks [env: DATABASE_URL=] [default: sqlite:tasks.db]
      --listen-ip <LISTEN_IP>        IP address on which the task scheduler's HTTP handler listens [env: LISTEN_IP=] [default: 127.0.0.1]
      --listen-port <LISTEN_PORT>    Port on which the task scheduler's HTTP handler listens [env: LISTEN_PORT=] [default: 8080]
  -h, --help                         Print help
  -V, --version                      Print version
```

Or, compile and run in unoptimized mode in a single step via:
```bash
cargo run
```

Additionally, `tasker` can be run at different `LOG_LEVEL`s: `TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR`.

In order to run `tasker` at log level `DEBUG` and with a non-default database URL and HTTP API listener port, execute:
```bash
RUST_LOG="debug" DATABASE_URL="sqlite://staging_tasks.db" LISTEN_PORT=8081 ./target/release/tasker
```


## Available HTTP Endpoints

When `tasker` is running, the following HTTP endpoints are available at `http://LISTEN_IP:LISTEN_PORT`:

1. `POST /tasks/new` with below `JSON` payload for a `Webhook` task:
```json
{
    "webhook": {
        "execution_time": "2026-02-10T16:30:00.0+01:00",   // Must be an RFC 3339 datetime in the future
        "url": "https://...",                              // URL to which to send the POST request, must be non-empty
        "body": "{ \"key\": \"value\" }"                   // Body to include in the POST request, must be non-empty
    }
}
```
or a `Hash` task:
```json
{
    "hash": {
        "execution_time": "2026-02-10T16:30:00.0+01:00",   // Must be an RFC 3339 datetime in the future
        "secret": "correct-horse-battery-staple"           // Must be non-empty
    }
}
```
Upon successful task creation, the generated UUIDv7 is returned, e.g.:
```json
{
    "id": "019bbade-01c6-ed11-821f-bc1538901f12"
}
```

2. `GET /tasks/019bbade-01c6-ed11-821f-bc1538901f12` with a UUID string as the ID of the task as part of the URL. If a task with the supplied ID exists, it is returned, e.g.:
```json
{
    "webhook": {
        "id": "019bbade-01c6-ed11-821f-bc1538901f12",
        "state": "todo",
        "execution_time": "2026-02-10T16:30:00.0+01:00",
        "url": "https://...",
        "body": "{ \"key\": \"value\" }"
    }
}
```
or:
```json
{
    "hash": {
        "id": "019bbade-01c6-ed11-821f-bc1538901f12",
        "state": "todo",
        "execution_time": "2026-02-10T16:30:00.0+01:00",
        "secret": "correct-horse-battery-staple"
    }
}
```

3. `GET /tasks/type/webhook` or `GET /tasks/type/hash`, e.g. for `/webhook`:
```json
[
    {
        "id": "019bbade-01c6-ed11-821f-bc1538901f12",
        "state": "todo",
        "execution_time": "2026-02-10T16:30:00.0+01:00",
        "url": "https://...",
        "body": "{ \"key\": \"value\" }"
    },
    // ...
]
```

4. `GET /tasks/state/STATE` with `STATE` one of `todo`, `in_progress`, `failed`, or `done`:
```json
[
    {
        "webhook": {
            "id": "019bbade-01c6-ed11-821f-bc1538901f12",
            "state": "done",
            "execution_time": "2026-02-10T16:30:00.0+01:00",
            "url": "https://...",
            "body": "{ \"key\": \"value\" }"
        }
    },
    {
        "hash": {
            "id": "019bbade-01c6-ed11-821f-bc1538901f13",
            "state": "done",
            "execution_time": "2026-02-10T16:30:00.0+01:00",
            "secret": "correct-horse-battery-staple"
        }
    },
    // ...
]
```

5. `DELETE /tasks/019bbade-01c6-ed11-821f-bc1538901f12` with a UUID string as the ID of the task as part of the URL. If a task with the supplied ID exists, it is deleted, with no content returned.
