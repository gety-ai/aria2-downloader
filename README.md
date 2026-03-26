# aria2-downloader

A small async Rust wrapper around `aria2c` that manages process lifecycle for you.

It starts `aria2c` lazily on the first download, polls per-task progress through aria2 RPC, and stops the process automatically after an idle timeout.

## Features

- Lazy process startup
- Automatic idle shutdown
- Per-download progress watching with `tokio::sync::watch`
- Single URL and multi-mirror downloads
- Force-cancel support through aria2 RPC
- Simple builder for RPC port, poll interval, idle timeout, and extra aria2 args

## Requirements

- Rust toolchain
- `aria2c` installed and available in `PATH`, or an explicit path passed to `Aria2Downloader::new` / `Aria2Downloader::builder`

## Installation

Add the crate to your `Cargo.toml`:

```toml
[dependencies]
aria2-downloader = { git = "https://github.com/gety-ai/aria2-downloader.git" }
tokio = { version = "1", features = ["macros", "rt"] }
```

## Quick Start

```rust
use std::time::Duration;

use aria2_downloader::Aria2Downloader;

#[tokio::main(flavor = "current_thread")]
async fn main() -> aria2_downloader::Result<()> {
    let downloader = Aria2Downloader::new("aria2c", Duration::from_secs(60));

    let mut handle = downloader
        .download("https://example.com/file.zip", None)
        .await?;

    loop {
        let progress = handle.changed().await?;
        println!(
            "gid={} status={:?} {:.1}% speed={} B/s",
            progress.gid,
            progress.status,
            progress.percent(),
            progress.download_speed
        );

        if progress.is_finished() {
            break;
        }
    }

    Ok(())
}
```

## Configure Output And Mirrors

```rust
use std::time::Duration;

use aria2_downloader::{Aria2Downloader, DownloadOptions};

#[tokio::main(flavor = "current_thread")]
async fn main() -> aria2_downloader::Result<()> {
    let downloader = Aria2Downloader::builder("aria2c")
        .idle_timeout(Duration::from_secs(120))
        .rpc_port(16800)
        .poll_interval(Duration::from_millis(500))
        .extra_args(vec!["--check-certificate=false".into()])
        .build();

    let options = DownloadOptions {
        dir: Some("./downloads".into()),
        out: Some("archive.zip".into()),
        headers: vec!["Referer: https://example.com".into()],
        split: Some(4),
        max_connection_per_server: Some(4),
        proxy: None,
    };

    let handle = downloader
        .download_uris(
            vec![
                "https://mirror-a.example.com/archive.zip".into(),
                "https://mirror-b.example.com/archive.zip".into(),
            ],
            Some(options),
        )
        .await?;

    let final_progress = handle.wait().await?;
    println!("done: {:?}", final_progress.status);

    downloader.shutdown().await?;
    Ok(())
}
```

## Public API

### `Aria2Downloader`

- `new(aria2_path, idle_timeout)`: shorthand constructor
- `builder(aria2_path)`: configure the downloader in detail
- `download(url, opts)`: submit one URL
- `download_uris(urls, opts)`: submit multiple mirror URLs
- `shutdown()`: stop the managed `aria2c` process and fail any in-flight tasks
- `is_running()`: check whether the managed process is currently alive

### `Aria2DownloaderBuilder`

Defaults:

- `idle_timeout = 300s`
- `rpc_port = 6800`
- `poll_interval = 1s`
- `extra_args = []`

Builder methods:

- `idle_timeout(Duration)`
- `rpc_port(u16)`
- `poll_interval(Duration)`
- `extra_args(Vec<String>)`
- `build()`

### `DownloadHandle`

- `gid()`: aria2 task id
- `progress()`: current snapshot
- `changed()`: await next progress update
- `wait()`: await task completion
- `cancel()`: force-remove the task through aria2 RPC

### `DownloadProgress`

- `status`: one of `Pending`, `Active`, `Waiting`, `Paused`, `Complete`, `Error`, `Removed`
- `percent()`: clamped to `0.0..=100.0`
- `is_finished()`: true for `Complete`, `Error`, and `Removed`

## Error Semantics

- `ProcessStart`: failed to spawn `aria2c`
- `ConnectTimeout`: `aria2c` started but RPC was not reachable before timeout
- `InvalidInput`: empty URI list or blank URI
- `Rpc`: aria2 RPC request failed
- `NotRunning`: process-dependent operation was attempted while no aria2 process was active
- `DownloadFailed`: aria2 reported task failure
- `ChannelClosed`: the progress watch channel closed unexpectedly

`wait()` returns `DownloadFailed` for downloads that end in aria2 error state. A cancelled task resolves with `DownloadStatus::Removed`.

## Operational Notes

- The downloader owns one managed `aria2c` child process.
- The process is started on first download submission, not at construction time.
- If the process exits unexpectedly, existing tracked tasks are failed and the next submission restarts aria2.
- Idle shutdown only happens when there are no tracked tasks.
- On Windows, the spawned `aria2c` process is created without opening a console window.
- If you run multiple downloader instances in parallel on the same machine, assign different RPC ports.

## Testing

The test suite uses a local HTTP server plus a real `aria2c` binary. It covers:

- successful download flow
- idle shutdown and restart
- mirror fallback
- HTTP failure propagation
- cancel behavior
- shutdown behavior for in-flight work
- input validation and progress edge cases

Run tests with:

```powershell
cargo test
```

If `aria2c` is not in `PATH`, point tests at a binary explicitly:

```powershell
$env:ARIA2C_BIN = "C:\path\to\aria2c.exe"
cargo test
```
