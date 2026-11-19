//! Two cameras, one registry, one RTSP server, under churn.
//!
//! The production `GstPipelineRegistry` serves two cameras while, for
//! 20 s, live sessions start and end, RTSP clients join and leave (the
//! last one often exactly while a detach runs), half-open clients prepare
//! a media they never play, and the idle caption and thumbnail are
//! refreshed. Every registry call has a deadline; a watchdog thread ends
//! the process when nothing completes for 30 s, so a wedged runtime cannot
//! hang CI. A wedge shows up as `<call>(<camera>) did not return within
//! 10 s` plus the last operation per camera.
//!
//! Skipped when a GStreamer element is missing, unless
//! `STREAMER_REQUIRE_GST_IT=1` (set in CI).

#[allow(dead_code, reason = "this binary uses part of the shared fixtures")]
mod support;

use std::collections::HashMap;
use std::future::Future;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, PoisonError};
use std::thread::JoinHandle;
use std::time::Duration;

use bytes::Bytes;
use gstreamer as gst;
use gstreamer::prelude::*;
use gstreamer_app as gst_app;

use streamer_domain::camera::CameraId;
use streamer_domain::config::WebrtcConfig;
use streamer_infra_media::{GstPipelineRegistry, LiveSinks, MediaError, PipelineRegistry};
use support::probe::{RtspProbe, eventually};
use support::{Stack, gstreamer_ready};

/// How long the churn lasts.
const CHURN: Duration = Duration::from_secs(20);
/// Longest any registry call may take.
const CALL_LIMIT: Duration = Duration::from_secs(10);
/// No completed call for this long ends the process.
const WATCHDOG_LIMIT: Duration = Duration::from_secs(30);

/// A tiny xorshift generator: the schedule needs variety, not quality.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed | 1)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    /// A duration between `lo` and `hi` milliseconds, inclusive.
    fn millis(&mut self, lo: u64, hi: u64) -> Duration {
        Duration::from_millis(lo + self.next_u64() % (hi - lo + 1))
    }
}

/// What every loop shares.
struct Ctx {
    registry: Arc<GstPipelineRegistry>,
    /// The sinks of the sessions in progress; the RTP source feeds them.
    sinks: Arc<StdMutex<HashMap<CameraId, LiveSinks>>>,
    stop: AtomicBool,
    done: AtomicBool,
    progress: AtomicU64,
    last_op: StdMutex<HashMap<String, String>>,
    failures: StdMutex<Vec<String>>,
}

impl Ctx {
    fn fail(&self, message: String) {
        eprintln!("STRESS FAILURE: {message}");
        self.failures
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(message);
    }

    fn note(&self, camera: &CameraId, what: String) {
        self.last_op
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(camera.to_string(), what);
    }

    fn should_stop(&self) -> bool {
        let failed = !self
            .failures
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_empty();
        failed || self.stop.load(Ordering::SeqCst)
    }

    /// Run one registry call under its deadline; a failure or a timeout is
    /// recorded and answered with `None`.
    async fn call<T>(
        &self,
        op: &str,
        camera: &CameraId,
        fut: impl Future<Output = Result<T, MediaError>>,
    ) -> Option<T> {
        self.note(camera, format!("{op} (running)"));
        let outcome = tokio::time::timeout(CALL_LIMIT, fut).await;
        match outcome {
            Ok(Ok(value)) => {
                self.progress.fetch_add(1, Ordering::SeqCst);
                self.note(camera, format!("{op} (done)"));
                Some(value)
            }
            Ok(Err(e)) => {
                self.progress.fetch_add(1, Ordering::SeqCst);
                self.fail(format!("{op}({camera}) failed: {e}"));
                None
            }
            Err(_) => {
                self.fail(format!(
                    "{op}({camera}) did not return within {} s",
                    CALL_LIMIT.as_secs()
                ));
                None
            }
        }
    }
}

/// Push every RTP packet of `buffer` to the sinks of the sessions in progress.
fn feed(sinks: &StdMutex<HashMap<CameraId, LiveSinks>>, buffer: &gst::BufferRef) {
    let Ok(map) = buffer.map_readable() else {
        return;
    };
    let packet = Bytes::copy_from_slice(map.as_slice());
    let sinks = sinks.lock().unwrap_or_else(PoisonError::into_inner);
    for session in sinks.values() {
        let _ = session.video.push(packet.clone());
    }
}

/// One helper pipeline making H.264 RTP for every live session.
fn start_rtp_source(sinks: Arc<StdMutex<HashMap<CameraId, LiveSinks>>>) -> gst::Pipeline {
    let pipeline = gst::parse::launch(
        "videotestsrc is-live=true ! video/x-raw,width=320,height=240,framerate=15/1 \
         ! x264enc tune=zerolatency speed-preset=ultrafast key-int-max=15 \
         ! rtph264pay pt=96 config-interval=1 ! appsink name=out sync=false",
    )
    .expect("rtp source launch")
    .downcast::<gst::Pipeline>()
    .expect("rtp source pipeline");
    let appsink = pipeline
        .by_name("out")
        .expect("rtp appsink")
        .downcast::<gst_app::AppSink>()
        .expect("appsink type");
    appsink.set_callbacks(
        gst_app::AppSinkCallbacks::builder()
            .new_sample(move |sink| {
                let sample = sink.pull_sample().map_err(|_| gst::FlowError::Eos)?;
                if let Some(list) = sample.buffer_list() {
                    for buffer in list.iter() {
                        feed(&sinks, buffer);
                    }
                } else if let Some(buffer) = sample.buffer() {
                    feed(&sinks, buffer);
                }
                Ok(gst::FlowSuccess::Ok)
            })
            .build(),
    );
    pipeline
        .set_state(gst::State::Playing)
        .expect("rtp source -> Playing");
    pipeline
}

/// A solid 64x48 JPEG, encoded by GStreamer.
fn small_jpeg() -> Vec<u8> {
    let pipeline = gst::parse::launch(
        "videotestsrc pattern=solid-color num-buffers=1 \
         ! video/x-raw,width=64,height=48 ! jpegenc ! appsink name=out sync=false",
    )
    .expect("jpeg launch")
    .downcast::<gst::Pipeline>()
    .expect("jpeg pipeline");
    let sink = pipeline
        .by_name("out")
        .expect("jpeg appsink")
        .downcast::<gst_app::AppSink>()
        .expect("appsink type");
    pipeline
        .set_state(gst::State::Playing)
        .expect("jpeg -> Playing");
    let sample = sink
        .try_pull_sample(gst::ClockTime::from_seconds(5))
        .expect("a jpeg sample");
    let _ = pipeline.set_state(gst::State::Null);
    let map = sample
        .buffer()
        .expect("jpeg buffer")
        .map_readable()
        .expect("jpeg map");
    map.as_slice().to_vec()
}

/// Live sessions of one camera: attach, feed for a while, detach.
async fn session_loop(ctx: &Ctx, camera: &CameraId, seed: u64) {
    let mut rng = Rng::new(seed);
    while !ctx.should_stop() {
        let attached = ctx
            .call(
                "attach_live_sink",
                camera,
                ctx.registry.attach_live_sink(camera),
            )
            .await;
        let Some(sinks) = attached else {
            return;
        };
        ctx.sinks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(camera.clone(), sinks);
        tokio::time::sleep(rng.millis(150, 600)).await;
        let removed = ctx
            .sinks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(camera);
        drop(removed);
        if ctx
            .call(
                "detach_live_sink",
                camera,
                ctx.registry.detach_live_sink(camera),
            )
            .await
            .is_none()
        {
            return;
        }
        tokio::time::sleep(rng.millis(50, 200)).await;
    }
}

/// Refresh the caption and the thumbnail of every camera, over and over.
async fn refresh_loop(ctx: &Ctx, cameras: &[CameraId], jpeg: &[u8]) {
    let mut round = 0_u32;
    while !ctx.should_stop() {
        round += 1;
        for camera in cameras {
            let caption = format!("stress {round}");
            let set = ctx
                .call(
                    "set_idle_caption",
                    camera,
                    ctx.registry.set_idle_caption(camera, caption),
                )
                .await;
            let shown = ctx
                .call(
                    "refresh_thumbnail",
                    camera,
                    ctx.registry
                        .refresh_thumbnail(camera, Bytes::copy_from_slice(jpeg)),
                )
                .await;
            if set.is_none() || shown.is_none() {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// A client that plays for a while and leaves.
fn client_loop(url: &str, ctx: &Ctx, seed: u64) {
    let mut rng = Rng::new(seed);
    while !ctx.should_stop() {
        let probe = RtspProbe::connect(url);
        std::thread::sleep(rng.millis(100, 700));
        drop(probe);
    }
}

/// `host:port` of an `rtsp://host:port/path` URL.
fn host_port(url: &str) -> String {
    let rest = url.strip_prefix("rtsp://").unwrap_or(url);
    rest.split('/').next().unwrap_or(rest).to_string()
}

/// A client that asks for the description (the server prepares a media) and
/// never plays.
fn half_open_loop(url: &str, ctx: &Ctx, seed: u64) {
    let mut rng = Rng::new(seed);
    let request = format!("DESCRIBE {url} RTSP/1.0\r\nCSeq: 1\r\nAccept: application/sdp\r\n\r\n");
    while !ctx.should_stop() {
        if let Ok(mut stream) = TcpStream::connect(host_port(url)) {
            let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
            let _ = stream.write_all(request.as_bytes());
            let mut buf = [0_u8; 2048];
            let _ = stream.read(&mut buf);
            std::thread::sleep(rng.millis(100, 400));
        }
        std::thread::sleep(rng.millis(50, 150));
    }
}

/// Ends the process when no registry call completes for [`WATCHDOG_LIMIT`].
fn start_watchdog(ctx: Arc<Ctx>) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let mut seen = ctx.progress.load(Ordering::SeqCst);
        let mut since = std::time::Instant::now();
        while !ctx.done.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(500));
            let now = ctx.progress.load(Ordering::SeqCst);
            if now != seen {
                seen = now;
                since = std::time::Instant::now();
            } else if since.elapsed() > WATCHDOG_LIMIT {
                let last = ctx
                    .last_op
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .clone();
                eprintln!("STRESS WATCHDOG: last op per camera = {last:?}");
                std::process::exit(101);
            }
        }
    })
}

/// The work of the churn phase, until [`CHURN`] has passed.
async fn churn(ctx: &Arc<Ctx>, stack: &Stack, jpeg: &Arc<Vec<u8>>) {
    let mut tasks = Vec::new();
    let mut threads: Vec<JoinHandle<()>> = Vec::new();
    for (index, camera) in (1_u64..).zip(stack.cameras.iter()) {
        let (session_ctx, session_camera) = (Arc::clone(ctx), camera.clone());
        tasks.push(tokio::spawn(async move {
            session_loop(&session_ctx, &session_camera, 0x9e37_79b9 * index).await;
        }));
    }
    let (refresh_ctx, refresh_cameras, refresh_jpeg) =
        (Arc::clone(ctx), stack.cameras.clone(), Arc::clone(jpeg));
    tasks.push(tokio::spawn(async move {
        refresh_loop(&refresh_ctx, &refresh_cameras, &refresh_jpeg).await;
    }));
    for (index, stream) in (1_u64..).zip([support::STREAM, support::STREAM2]) {
        let url = stack.url_of(stream);
        for client in 0_u64..2 {
            let (thread_ctx, thread_url) = (Arc::clone(ctx), url.clone());
            let seed = 0x1234_5679 * (index * 10 + client);
            threads.push(std::thread::spawn(move || {
                client_loop(&thread_url, &thread_ctx, seed);
            }));
        }
        let (thread_ctx, thread_url) = (Arc::clone(ctx), url);
        threads.push(std::thread::spawn(move || {
            half_open_loop(&thread_url, &thread_ctx, 0x7777_7777 * index);
        }));
    }

    let deadline = tokio::time::Instant::now() + CHURN;
    while tokio::time::Instant::now() < deadline && !ctx.should_stop() {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    ctx.stop.store(true, Ordering::SeqCst);
    for task in tasks {
        let _ = task.await;
    }
    let joined = tokio::task::spawn_blocking(move || {
        for thread in threads {
            let _ = thread.join();
        }
    });
    let _ = joined.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_cameras_survive_session_and_client_churn() {
    if !gstreamer_ready() {
        return;
    }
    let stack = Stack::pair(WebrtcConfig::default());
    for camera in &stack.cameras {
        stack.media.register(camera).await.expect("register");
    }
    let sinks: Arc<StdMutex<HashMap<CameraId, LiveSinks>>> = Arc::default();
    let ctx = Arc::new(Ctx {
        registry: Arc::clone(&stack.registry),
        sinks: Arc::clone(&sinks),
        stop: AtomicBool::new(false),
        done: AtomicBool::new(false),
        progress: AtomicU64::new(0),
        last_op: StdMutex::default(),
        failures: StdMutex::default(),
    });
    let watchdog = start_watchdog(Arc::clone(&ctx));
    let source = start_rtp_source(sinks);
    let jpeg = Arc::new(small_jpeg());

    churn(&ctx, &stack, &jpeg).await;

    let failures = ctx
        .failures
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    let last = ctx
        .last_op
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    assert!(
        failures.is_empty(),
        "registry stalled or failed during the churn: {failures:?}; last op per camera = {last:?}"
    );

    epilogue(&ctx, &stack).await;

    ctx.done.store(true, Ordering::SeqCst);
    let _ = watchdog.join();
    let _ = source.set_state(gst::State::Null);
}

/// After the churn both cameras still go idle -> live -> idle for a client.
async fn epilogue(ctx: &Ctx, stack: &Stack) {
    let mut sessions = Vec::new();
    for camera in &stack.cameras {
        let attached = ctx
            .call(
                "attach_live_sink",
                camera,
                ctx.registry.attach_live_sink(camera),
            )
            .await;
        let sinks = attached.expect("epilogue attach_live_sink");
        ctx.sinks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(camera.clone(), sinks);
        sessions.push(camera.clone());
    }
    let probes: Vec<RtspProbe> = [support::STREAM, support::STREAM2]
        .iter()
        .map(|stream| RtspProbe::connect(&stack.url_of(stream)))
        .collect();
    for (index, probe) in probes.iter().enumerate() {
        assert!(
            eventually(Duration::from_secs(15), || probe.frames() > 0).await,
            "camera {index} delivered no frame after the churn"
        );
    }
    for camera in &sessions {
        let removed = ctx
            .sinks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(camera);
        drop(removed);
        let detached = ctx
            .call(
                "detach_live_sink",
                camera,
                ctx.registry.detach_live_sink(camera),
            )
            .await;
        assert!(detached.is_some(), "epilogue detach_live_sink failed");
    }
    for (index, probe) in probes.iter().enumerate() {
        let before = probe.frames();
        assert!(
            eventually(Duration::from_secs(15), || probe.frames() > before + 5).await,
            "camera {index} stopped delivering frames after the detach"
        );
    }
}
