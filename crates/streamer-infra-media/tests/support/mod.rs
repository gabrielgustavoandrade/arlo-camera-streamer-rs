//! Shared fixtures for the GStreamer integration tests.

pub mod gateway;
pub mod probe;

use std::sync::Arc;

use gstreamer as gst;

use streamer_domain::camera::{CameraId, StreamName};
use streamer_domain::config::{
    CameraConfig, CooldownConfig, HlsOutput, OutputConfig, RtspOutput, VideoEncoder, WebrtcConfig,
};
use streamer_infra_media::{GstMediaMultiplexer, GstPipelineRegistry, RtspServer};

/// Set to `1` where the plugins are guaranteed (CI): a missing element
/// then fails the test instead of skipping it.
const REQUIRE_ENV: &str = "STREAMER_REQUIRE_GST_IT";

/// Every element the idle pipeline, the WebRTC leg, the fake gateway
/// and the probe instantiate.
const ELEMENTS: &[&str] = &[
    "webrtcbin",
    "nicesrc",
    "dtlssrtpenc",
    "srtpdec",
    "x264enc",
    "avdec_h264",
    "opusenc",
    "opusdec",
    "avenc_aac",
    "audiomixer",
    "input-selector",
    "textoverlay",
    "gdkpixbufoverlay",
    "imagefreeze",
    "jpegenc",
    "rtspsrc",
    "valve",
];

pub const CAMERA: &str = "IT_CAM";
pub const STREAM: &str = "it_cam";
/// The second camera of [`Stack::pair`].
#[allow(dead_code)]
pub const CAMERA2: &str = "IT_CAM2";
#[allow(dead_code)]
pub const STREAM2: &str = "it_cam2";

/// Initialise GStreamer and report whether every element the tests
/// need is installed. Skips (returns `false`) when one is missing,
/// unless [`REQUIRE_ENV`] asks for a hard failure.
pub fn gstreamer_ready() -> bool {
    gst::init().expect("gst::init");
    let missing: Vec<&str> = ELEMENTS
        .iter()
        .copied()
        .filter(|e| gst::ElementFactory::find(e).is_none())
        .collect();
    if missing.is_empty() {
        return true;
    }
    assert!(
        std::env::var(REQUIRE_ENV).as_deref() != Ok("1"),
        "GStreamer elements missing: {missing:?}"
    );
    eprintln!("skipping: GStreamer elements missing: {missing:?}");
    false
}

/// The production media stack for one camera, on an ephemeral RTSP port.
pub struct Stack {
    pub media: GstMediaMultiplexer<GstPipelineRegistry>,
    /// The registry behind `media`, for tests that call it directly.
    #[allow(dead_code)]
    pub registry: Arc<GstPipelineRegistry>,
    pub camera: CameraId,
    /// Every camera of the stack (`camera` is the first).
    #[allow(dead_code)]
    pub cameras: Vec<CameraId>,
    server: Arc<RtspServer>,
    /// This stack's own: a thumbnail one test stores must not become the
    /// next test's idle picture (a media loads the stored one when built).
    thumbnails: std::path::PathBuf,
}

impl Drop for Stack {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.thumbnails);
    }
}

/// Numbers the stacks of one test process.
static STACKS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

impl Stack {
    pub fn new(webrtc: WebrtcConfig) -> Self {
        Self::with_outputs(webrtc, None)
    }

    /// The stack with HLS written under `hls.dir`.
    pub fn with_hls(webrtc: WebrtcConfig, hls: HlsOutput) -> Self {
        Self::with_outputs(webrtc, Some(hls))
    }

    /// ONE server, ONE registry and ONE multiplexer with two cameras
    /// (`IT_CAM` / `it_cam` and `IT_CAM2` / `it_cam2`).
    #[allow(dead_code)]
    pub fn pair(webrtc: WebrtcConfig) -> Self {
        Self::build(webrtc, None, &[(CAMERA, STREAM), (CAMERA2, STREAM2)])
    }

    fn with_outputs(webrtc: WebrtcConfig, hls: Option<HlsOutput>) -> Self {
        Self::build(webrtc, hls, &[(CAMERA, STREAM)])
    }

    fn build(webrtc: WebrtcConfig, hls: Option<HlsOutput>, specs: &[(&str, &str)]) -> Self {
        let server = RtspServer::start("127.0.0.1:0").expect("rtsp server");
        let stack = STACKS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let thumbnails =
            std::env::temp_dir().join(format!("streamer-it-thumbs-{}-{stack}", std::process::id()));
        streamer_infra_media::prepare_thumbnail_dir(&thumbnails).expect("thumbnail dir");
        let registry = Arc::new(GstPipelineRegistry::new(
            server.clone(),
            streamer_infra_media::encoder::EncoderPlan::uniform(
                streamer_infra_media::encoder::VideoEncoding::at_default_bitrate(
                    streamer_infra_media::encoder::EncoderBackend::X264,
                ),
            ),
            thumbnails.clone(),
        ));
        let camera = CameraId::new(specs[0].0);
        let camera_ids: Vec<CameraId> = specs.iter().map(|(id, _)| CameraId::new(*id)).collect();
        let cameras: Vec<CameraConfig> = specs
            .iter()
            .map(|(id, stream)| CameraConfig {
                arlo_device_id: CameraId::new(*id),
                stream_name: StreamName::parse(stream).expect("stream name"),
                codec_hint: None,
                cooldown: CooldownConfig::default(),
                video_encoder: None,
                bitrate_kbps: None,
            })
            .collect();
        let output = OutputConfig {
            rtsp: RtspOutput {
                bind: "127.0.0.1:0".to_string(),
            },
            hls,
            dash: None,
            video_encoder: VideoEncoder::X264,
            bitrate_kbps: streamer_domain::config::DEFAULT_VIDEO_BITRATE_KBPS,
            metrics_bind: "127.0.0.1:0".to_string(),
            admin_bind: "127.0.0.1:0".to_string(),
        };
        let media = GstMediaMultiplexer::new(
            Arc::clone(&registry),
            output,
            webrtc,
            &cameras,
            // The harness relays from its own plaintext RTSP server.
            streamer_infra_media::RelayTls::from_config(None)
                .expect("system roots")
                .allowing_plaintext_to_loopback(),
        );
        Self {
            media,
            registry,
            camera,
            cameras: camera_ids,
            server,
            thumbnails,
        }
    }

    /// Serve `launch` at `mount` on the test server, standing in for the
    /// app's watch-along stream; returns its URL.
    pub fn install_source(&self, mount: &str, launch: &str) -> String {
        self.server
            .install_factory(mount, launch)
            .expect("install source factory");
        let port = self.server.bound_port().expect("rtsp server bound");
        format!("rtsp://127.0.0.1:{port}{mount}")
    }

    /// The camera's RTSP URL on the bound port.
    pub fn url(&self) -> String {
        self.url_of(STREAM)
    }

    /// The RTSP URL of the stream `stream` on the bound port.
    pub fn url_of(&self, stream: &str) -> String {
        let port = self.server.bound_port().expect("rtsp server bound");
        format!("rtsp://127.0.0.1:{port}/{stream}")
    }
}

/// Mean brightness (0–255) of the last video frame of an MPEG-TS
/// segment, or `None` when it holds no decodable frame.
pub fn segment_mean_luma(path: &std::path::Path) -> Option<u64> {
    use gstreamer::prelude::*;
    let launch = format!(
        "filesrc location=\"{}\" ! tsdemux ! h264parse ! avdec_h264 ! videoconvert \
         ! video/x-raw,format=GRAY8 ! appsink name=frames sync=false",
        path.display()
    );
    let pipeline = gst::parse::launch(&launch)
        .ok()?
        .downcast::<gst::Pipeline>()
        .ok()?;
    let sink = pipeline
        .by_name("frames")?
        .downcast::<gstreamer_app::AppSink>()
        .ok()?;
    pipeline.set_state(gst::State::Playing).ok()?;
    let mut last = None;
    while let Some(sample) = sink.try_pull_sample(gst::ClockTime::from_seconds(5)) {
        let buffer = sample.buffer()?;
        let map = buffer.map_readable().ok()?;
        let frame = map.as_slice();
        if !frame.is_empty() {
            let sum: u64 = frame.iter().map(|&b| u64::from(b)).sum();
            last = Some(sum / frame.len() as u64);
        }
    }
    let _ = pipeline.set_state(gst::State::Null);
    last
}

/// The WebRTC settings with the stall timeout at its floor, so the stall
/// test finishes quickly.
pub fn fast_stall() -> WebrtcConfig {
    WebrtcConfig {
        live_stall_timeout_secs: 4,
        ..WebrtcConfig::default()
    }
}
