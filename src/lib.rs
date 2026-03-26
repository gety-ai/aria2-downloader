//! Aria2 downloader with automatic process lifecycle management.
//!
//! - **Lazy start**: aria2c is spawned on the first download request.
//! - **Auto idle shutdown**: process is killed after a configurable idle duration.
//! - **Per-task progress**: each [`DownloadHandle`] exposes a `watch` channel.
//!
//! # Example
//! ```no_run
//! # use std::time::Duration;
//! # use aria2_downloader::Aria2Downloader;
//! # #[tokio::main(flavor = "current_thread")] async fn main() -> aria2_downloader::Result<()> {
//! let dl = Aria2Downloader::new("aria2c", Duration::from_secs(60));
//!
//! let mut handle = dl.download("https://example.com/file.zip", None).await?;
//! loop {
//!     let p = handle.changed().await?;
//!     println!("{:.1}%  {} B/s", p.percent(), p.download_speed);
//!     if p.is_finished() { break; }
//! }
//! # Ok(()) }
//! ```

use std::borrow::Cow;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{
    Arc, Mutex as StdMutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::Duration;

use aria2_rs::options::TaskOptions;
use aria2_rs::status::{Status, StatusKey, TaskStatus as Aria2Status};
use aria2_rs::{Client, ConnectionMeta, SmallVec};

use tokio::process::Child;
use tokio::sync::{Mutex, Notify, watch};
use tokio::time::Instant;

// ── Error ────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to start aria2c: {0}")]
    ProcessStart(#[source] std::io::Error),

    #[error("failed to connect to aria2c after {0:?}")]
    ConnectTimeout(Duration),

    #[error("aria2 rpc: {0}")]
    Rpc(#[from] aria2_rs::Error),

    #[error("process not running")]
    NotRunning,

    #[error("download failed (gid={gid}): {message}")]
    DownloadFailed { gid: String, message: String },

    #[error("progress channel closed")]
    ChannelClosed,
}

pub type Result<T> = std::result::Result<T, Error>;

// ── Progress types ───────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DownloadStatus {
    /// Submitted but not yet reported by aria2.
    Pending,
    Active,
    Waiting,
    Paused,
    Complete,
    Error,
    Removed,
}

#[derive(Debug, Clone)]
pub struct DownloadProgress {
    pub gid: String,
    pub status: DownloadStatus,
    /// Total size in bytes (0 if unknown).
    pub total_length: u64,
    /// Bytes downloaded so far.
    pub completed_length: u64,
    /// Current download speed in bytes/s.
    pub download_speed: u64,
    /// Current upload speed in bytes/s.
    pub upload_speed: u64,
    /// Error description, if any.
    pub error_message: Option<String>,
}

impl DownloadProgress {
    /// Percentage in `0.0 ..= 100.0`.
    pub fn percent(&self) -> f64 {
        if self.total_length == 0 {
            0.0
        } else {
            self.completed_length as f64 / self.total_length as f64 * 100.0
        }
    }

    pub fn is_finished(&self) -> bool {
        matches!(
            self.status,
            DownloadStatus::Complete | DownloadStatus::Error | DownloadStatus::Removed
        )
    }
}

impl Default for DownloadProgress {
    fn default() -> Self {
        Self {
            gid: String::new(),
            status: DownloadStatus::Pending,
            total_length: 0,
            completed_length: 0,
            download_speed: 0,
            upload_speed: 0,
            error_message: None,
        }
    }
}

// ── Download options ─────────────────────────────────────────

#[derive(Debug, Clone, Default)]
pub struct DownloadOptions {
    /// Download directory.
    pub dir: Option<String>,
    /// Output file name.
    pub out: Option<String>,
    /// HTTP headers (e.g. `"Referer: https://…"`).
    pub headers: Vec<String>,
    /// Number of connections per download.
    pub split: Option<i32>,
    /// Max connections per server.
    pub max_connection_per_server: Option<i32>,
    /// Proxy URL.
    pub proxy: Option<String>,
}

// ── Download handle ──────────────────────────────────────────

/// Handle to a single download task. Provides progress and cancellation.
pub struct DownloadHandle {
    gid: String,
    rx: watch::Receiver<DownloadProgress>,
    shared: Arc<Shared>,
}

impl DownloadHandle {
    /// The aria2 GID for this download.
    pub fn gid(&self) -> &str {
        &self.gid
    }

    /// Snapshot of current progress.
    pub fn progress(&self) -> DownloadProgress {
        self.rx.borrow().clone()
    }

    /// Wait until the next progress update arrives.
    pub async fn changed(&mut self) -> Result<DownloadProgress> {
        self.rx.changed().await.map_err(|_| Error::ChannelClosed)?;
        Ok(self.rx.borrow_and_update().clone())
    }

    /// Block until the download finishes.
    ///
    /// Returns [`Error::DownloadFailed`] if aria2 reports an error.
    pub async fn wait(mut self) -> Result<DownloadProgress> {
        loop {
            {
                let p = self.rx.borrow_and_update().clone();
                if p.is_finished() {
                    return finish_result(p);
                }
            }
            if self.rx.changed().await.is_err() {
                // Sender dropped — return last known state.
                return finish_result(self.rx.borrow().clone());
            }
        }
    }

    /// Cancel (force-remove) this download.
    pub async fn cancel(&self) -> Result<()> {
        let client = self.shared.client().await?;
        client
            .call(&aria2_rs::call::ForceRemoveCall {
                gid: Cow::Borrowed(&self.gid),
            })
            .await?;
        Ok(())
    }
}

fn finish_result(p: DownloadProgress) -> Result<DownloadProgress> {
    if p.status == DownloadStatus::Error {
        Err(Error::DownloadFailed {
            gid: p.gid.clone(),
            message: p.error_message.clone().unwrap_or_default(),
        })
    } else {
        Ok(p)
    }
}

// ── Internal shared state ────────────────────────────────────

struct Shared {
    aria2_path: PathBuf,
    idle_timeout: Duration,
    rpc_port: u16,
    poll_interval: Duration,
    extra_args: Vec<String>,

    /// Guards process lifecycle (start / stop).
    process: Mutex<Option<ProcessCtx>>,
    /// Per-GID progress senders.  Held very briefly (no await).
    tasks: StdMutex<HashMap<String, watch::Sender<DownloadProgress>>>,
    /// Poked whenever a new download is submitted – resets the idle timer.
    activity: Notify,
    /// Cooperative shutdown flag for background tasks.
    shutdown: AtomicBool,
    shutdown_notify: Notify,
    /// Monotonic counter used to assign unique generations to spawned processes.
    next_generation: AtomicU64,
    /// Generation of the currently running aria2c process, or `0` if stopped.
    active_generation: AtomicU64,
}

struct ProcessCtx {
    child: Child,
    client: Arc<Client>,
}

impl Shared {
    async fn client(&self) -> Result<Arc<Client>> {
        self.process
            .lock()
            .await
            .as_ref()
            .map(|c| c.client.clone())
            .ok_or(Error::NotRunning)
    }
}

// ── Builder ──────────────────────────────────────────────────

pub struct Aria2DownloaderBuilder {
    aria2_path: PathBuf,
    idle_timeout: Duration,
    rpc_port: u16,
    poll_interval: Duration,
    extra_args: Vec<String>,
}

impl Aria2DownloaderBuilder {
    pub fn idle_timeout(mut self, d: Duration) -> Self {
        self.idle_timeout = d;
        self
    }

    pub fn rpc_port(mut self, port: u16) -> Self {
        self.rpc_port = port;
        self
    }

    pub fn poll_interval(mut self, d: Duration) -> Self {
        self.poll_interval = d;
        self
    }

    /// Extra CLI arguments passed to aria2c on startup.
    pub fn extra_args(mut self, args: Vec<String>) -> Self {
        self.extra_args = args;
        self
    }

    pub fn build(self) -> Aria2Downloader {
        Aria2Downloader {
            shared: Arc::new(Shared {
                aria2_path: self.aria2_path,
                idle_timeout: self.idle_timeout,
                rpc_port: self.rpc_port,
                poll_interval: self.poll_interval,
                extra_args: self.extra_args,
                process: Mutex::new(None),
                tasks: StdMutex::new(HashMap::new()),
                activity: Notify::new(),
                shutdown: AtomicBool::new(false),
                shutdown_notify: Notify::new(),
                next_generation: AtomicU64::new(1),
                active_generation: AtomicU64::new(0),
            }),
        }
    }
}

// ── Aria2Downloader ──────────────────────────────────────────

/// Downloads files through a managed aria2c process.
///
/// The process is started lazily on the first [`download`](Self::download)
/// call and stopped automatically when idle for longer than the configured
/// timeout.
pub struct Aria2Downloader {
    shared: Arc<Shared>,
}

impl Aria2Downloader {
    /// Shorthand: `aria2_path` + `idle_timeout`, rest defaults.
    pub fn new(aria2_path: impl Into<PathBuf>, idle_timeout: Duration) -> Self {
        Self::builder(aria2_path).idle_timeout(idle_timeout).build()
    }

    pub fn builder(aria2_path: impl Into<PathBuf>) -> Aria2DownloaderBuilder {
        Aria2DownloaderBuilder {
            aria2_path: aria2_path.into(),
            idle_timeout: Duration::from_secs(300),
            rpc_port: 6800,
            poll_interval: Duration::from_secs(1),
            extra_args: Vec::new(),
        }
    }

    /// Submit a single-URL download.
    pub async fn download(
        &self,
        url: &str,
        opts: Option<DownloadOptions>,
    ) -> Result<DownloadHandle> {
        self.download_uris(vec![url.to_string()], opts).await
    }

    /// Submit a download with multiple mirror URIs.
    pub async fn download_uris(
        &self,
        urls: Vec<String>,
        opts: Option<DownloadOptions>,
    ) -> Result<DownloadHandle> {
        self.ensure_running().await?;
        self.shared.activity.notify_one();

        let client = self.shared.client().await?;
        let reply = client
            .call(&aria2_rs::call::AddUriCall {
                uris: SmallVec::from_iter(urls),
                options: opts.map(into_task_options),
            })
            .await?;
        let gid = reply.0.to_string();

        let init = DownloadProgress {
            gid: gid.clone(),
            ..Default::default()
        };
        let (tx, rx) = watch::channel(init);
        self.shared.tasks.lock().unwrap().insert(gid.clone(), tx);

        Ok(DownloadHandle {
            gid,
            rx,
            shared: self.shared.clone(),
        })
    }

    /// Gracefully shut down the aria2c process.
    pub async fn shutdown(&self) -> Result<()> {
        self.shared.shutdown.store(true, Ordering::Relaxed);
        self.shared.shutdown_notify.notify_waiters();
        stop_process(&self.shared).await;
        Ok(())
    }

    /// Returns `true` if the aria2c process is currently running.
    pub async fn is_running(&self) -> bool {
        self.shared.process.lock().await.is_some()
    }

    // ── internal ─────────────────────────────────────────────

    async fn ensure_running(&self) -> Result<()> {
        let mut guard = self.shared.process.lock().await;
        if let Some(ctx) = guard.as_mut() {
            match ctx.child.try_wait() {
                Ok(None) => return Ok(()),
                Ok(Some(status)) => {
                    tracing::warn!(?status, "aria2c exited unexpectedly, restarting");
                    self.shared.active_generation.store(0, Ordering::Release);
                    *guard = None;
                    fail_all_tasks(&self.shared, "aria2c process exited unexpectedly");
                }
                Err(error) => {
                    tracing::warn!(%error, "failed to inspect aria2c process state, restarting");
                    self.shared.active_generation.store(0, Ordering::Release);
                    *guard = None;
                    fail_all_tasks(&self.shared, "failed to inspect aria2c process state");
                }
            }
        }

        // Reset shutdown flag (may have been set by a previous idle shutdown).
        self.shared.shutdown.store(false, Ordering::Relaxed);
        let generation = self.shared.next_generation.fetch_add(1, Ordering::Relaxed);

        let secret = format!(
            "{:x}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        let mut cmd = tokio::process::Command::new(&self.shared.aria2_path);
        cmd.args([
            "--enable-rpc",
            &format!("--rpc-listen-port={}", self.shared.rpc_port),
            &format!("--rpc-secret={}", secret),
            "--rpc-listen-all=false",
            "--quiet",
        ]);
        for arg in &self.shared.extra_args {
            cmd.arg(arg);
        }
        cmd.kill_on_drop(true);

        #[cfg(windows)]
        {
            const CREATE_NO_WINDOW: u32 = 0x0800_0000;
            cmd.creation_flags(CREATE_NO_WINDOW);
        }

        let child = cmd.spawn().map_err(Error::ProcessStart)?;
        let client =
            connect_with_retry(self.shared.rpc_port, &secret, Duration::from_secs(10)).await?;
        let client = Arc::new(client);
        self.shared
            .active_generation
            .store(generation, Ordering::Release);

        let s1 = self.shared.clone();
        tokio::spawn(run_poller(s1, generation));

        let s2 = self.shared.clone();
        tokio::spawn(run_idle_watcher(s2, generation));

        *guard = Some(ProcessCtx { child, client });

        tracing::info!(port = self.shared.rpc_port, "aria2c started");
        Ok(())
    }
}

impl Drop for Aria2Downloader {
    fn drop(&mut self) {
        self.shared.shutdown.store(true, Ordering::Relaxed);
        self.shared.shutdown_notify.notify_waiters();
        // `kill_on_drop(true)` on the Child ensures aria2c is killed.
    }
}

// ── Background: progress poller ──────────────────────────────

async fn run_poller(shared: Arc<Shared>, generation: u64) {
    let mut interval = tokio::time::interval(shared.poll_interval);

    loop {
        interval.tick().await;

        if shared.shutdown.load(Ordering::Relaxed) || !is_generation_active(&shared, generation) {
            return;
        }

        let gids: Vec<String> = shared.tasks.lock().unwrap().keys().cloned().collect();
        if gids.is_empty() {
            continue;
        }

        let client = match shared.client().await {
            Ok(c) => c,
            Err(_) => return,
        };

        let mut finished = Vec::new();

        for gid in &gids {
            let res = client
                .call(&aria2_rs::call::TellStatusCall {
                    gid: Cow::Borrowed(gid),
                    keys: SmallVec::from_iter([
                        StatusKey::Status,
                        StatusKey::TotalLength,
                        StatusKey::CompletedLength,
                        StatusKey::DownloadSpeed,
                        StatusKey::UploadSpeed,
                        StatusKey::ErrorMessage,
                    ]),
                })
                .await;

            match res {
                Ok(st) => {
                    let prog = status_to_progress(gid, &st);
                    let done = prog.is_finished();
                    if let Some(tx) = shared.tasks.lock().unwrap().get(gid) {
                        let _ = tx.send(prog);
                    }
                    if done {
                        finished.push(gid.clone());
                    }
                }
                Err(e) => {
                    tracing::warn!(gid, %e, "tellStatus failed");
                    let err = DownloadProgress {
                        gid: gid.clone(),
                        status: DownloadStatus::Error,
                        error_message: Some(format!("lost contact with aria2c: {e}")),
                        ..Default::default()
                    };
                    if let Some(tx) = shared.tasks.lock().unwrap().get(gid) {
                        let _ = tx.send(err);
                    }
                    finished.push(gid.clone());
                }
            }
        }

        if !finished.is_empty() {
            let mut tasks = shared.tasks.lock().unwrap();
            for gid in &finished {
                tasks.remove(gid);
            }
            drop(tasks);
            shared.activity.notify_one();
        }
    }
}

// ── Background: idle watcher ─────────────────────────────────

async fn run_idle_watcher(shared: Arc<Shared>, generation: u64) {
    loop {
        if shared.shutdown.load(Ordering::Relaxed) || !is_generation_active(&shared, generation) {
            return;
        }

        if !shared.tasks.lock().unwrap().is_empty() {
            tokio::select! {
                () = shared.activity.notified() => continue,
                () = shared.shutdown_notify.notified() => return,
            };
        }

        // Start idle countdown, reset on new activity.
        let timed_out = tokio::select! {
            () = tokio::time::sleep(shared.idle_timeout) => true,
            () = shared.activity.notified() => false,
            () = shared.shutdown_notify.notified() => return,
        };

        if timed_out {
            if !is_generation_active(&shared, generation)
                || !shared.tasks.lock().unwrap().is_empty()
            {
                continue;
            }
            tracing::info!("idle timeout reached, stopping aria2c");
            stop_process(&shared).await;
            return;
        }
    }
}

// ── Helpers ──────────────────────────────────────────────────

async fn connect_with_retry(port: u16, secret: &str, timeout: Duration) -> Result<Client> {
    let deadline = Instant::now() + timeout;
    loop {
        let meta = ConnectionMeta {
            url: format!("ws://127.0.0.1:{port}/jsonrpc"),
            token: Some(format!("token:{secret}")),
        };
        match Client::connect(meta, 32).await {
            Ok(c) => return Ok(c),
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(_) => return Err(Error::ConnectTimeout(timeout)),
        }
    }
}

async fn stop_process(shared: &Shared) {
    let mut guard = shared.process.lock().await;
    if let Some(mut ctx) = guard.take() {
        shared.active_generation.store(0, Ordering::Release);
        fail_all_tasks(shared, "aria2c process stopped");
        let _ = ctx.child.kill().await;
        tracing::info!("aria2c stopped");
    }
}

fn is_generation_active(shared: &Shared, generation: u64) -> bool {
    shared.active_generation.load(Ordering::Acquire) == generation
}

fn fail_all_tasks(shared: &Shared, message: &str) {
    let mut tasks = shared.tasks.lock().unwrap();
    if tasks.is_empty() {
        return;
    }

    for (gid, tx) in tasks.drain() {
        let _ = tx.send(DownloadProgress {
            gid,
            status: DownloadStatus::Error,
            error_message: Some(message.to_string()),
            ..Default::default()
        });
    }

    shared.activity.notify_one();
}

fn status_to_progress(gid: &str, st: &Status) -> DownloadProgress {
    DownloadProgress {
        gid: gid.to_string(),
        status: match st.status {
            Some(Aria2Status::Active) => DownloadStatus::Active,
            Some(Aria2Status::Waiting) => DownloadStatus::Waiting,
            Some(Aria2Status::Paused) => DownloadStatus::Paused,
            Some(Aria2Status::Complete) => DownloadStatus::Complete,
            Some(Aria2Status::Error) => DownloadStatus::Error,
            Some(Aria2Status::Removed) => DownloadStatus::Removed,
            None => DownloadStatus::Pending,
        },
        total_length: st.total_length.unwrap_or(0),
        completed_length: st.completed_length.unwrap_or(0),
        download_speed: st.download_speed.unwrap_or(0),
        upload_speed: st.upload_speed.unwrap_or(0),
        error_message: st.error_message.clone(),
    }
}

fn into_task_options(opts: DownloadOptions) -> TaskOptions {
    TaskOptions {
        dir: opts.dir.map(Into::into),
        out: opts.out.map(Into::into),
        header: if opts.headers.is_empty() {
            None
        } else {
            Some(SmallVec::from_iter(
                opts.headers.into_iter().map(Into::into),
            ))
        },
        split: opts.split,
        max_connection_per_server: opts.max_connection_per_server,
        all_proxy: opts.proxy.map(Into::into),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_to_progress_maps_expected_fields() {
        let status = Status {
            gid: Some("gid-1".into()),
            status: Some(Aria2Status::Active),
            total_length: Some(100),
            completed_length: Some(25),
            upload_length: None,
            bitfield: None,
            download_speed: Some(4),
            upload_speed: Some(2),
            info_hash: None,
            num_seeders: None,
            seeder: None,
            piece_length: None,
            num_pieces: None,
            connections: None,
            error_code: None,
            error_message: None,
            followed_by: None,
            following: None,
            belongs_to: None,
            dir: None,
            files: None,
            bittorrent: None,
        };

        let progress = status_to_progress("gid-1", &status);
        assert_eq!(progress.gid, "gid-1");
        assert_eq!(progress.status, DownloadStatus::Active);
        assert_eq!(progress.total_length, 100);
        assert_eq!(progress.completed_length, 25);
        assert_eq!(progress.download_speed, 4);
        assert_eq!(progress.upload_speed, 2);
        assert_eq!(progress.percent(), 25.0);
    }

    #[test]
    fn finish_result_returns_download_error() {
        let error = finish_result(DownloadProgress {
            gid: "gid-2".into(),
            status: DownloadStatus::Error,
            error_message: Some("boom".into()),
            ..Default::default()
        })
        .unwrap_err();

        match error {
            Error::DownloadFailed { gid, message } => {
                assert_eq!(gid, "gid-2");
                assert_eq!(message, "boom");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
