use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
};

use ffmpeg_next as ffmpeg;
use gpui::RenderImage;
use image::{Delay, Frame, RgbaImage};

const MAX_VIDEO_FRAME_COUNT: usize = 300;
const MAX_VIDEO_WIDTH: u32 = 480;
const MIN_FRAME_DELAY_MS: u32 = 16;
const MAX_FRAME_DELAY_MS: u32 = 500;
const DEFAULT_FRAME_DELAY_MS: u32 = 67;

static FFMPEG_INIT_OK: OnceLock<bool> = OnceLock::new();
static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

pub fn decode_video_to_render_image(video_bytes: &[u8]) -> Option<Arc<RenderImage>> {
    if video_bytes.is_empty() {
        return None;
    }
    if !*FFMPEG_INIT_OK.get_or_init(|| ffmpeg::init().is_ok()) {
        return None;
    }

    let temp_path = next_temp_video_path();
    fs::write(&temp_path, video_bytes).ok()?;

    let decoded = decode_video_file_to_render_image(&temp_path);
    let _ = fs::remove_file(&temp_path);
    decoded
}

fn decode_video_file_to_render_image(path: &PathBuf) -> Option<Arc<RenderImage>> {
    let mut input_ctx = ffmpeg::format::input(path).ok()?;
    let input_stream = input_ctx.streams().best(ffmpeg::media::Type::Video)?;
    let stream_index = input_stream.index();
    let stream_time_base = f64::from(input_stream.time_base());
    let default_delay_ms = frame_delay_ms_from_rate(input_stream.rate());

    let context_decoder =
        ffmpeg::codec::context::Context::from_parameters(input_stream.parameters()).ok()?;
    let mut decoder = context_decoder.decoder().video().ok()?;
    let source_width = decoder.width();
    let source_height = decoder.height();
    if source_width == 0 || source_height == 0 {
        return None;
    }

    let (target_width, target_height) = target_video_size(source_width, source_height);
    let mut scaler = ffmpeg::software::scaling::Context::get(
        decoder.format(),
        source_width,
        source_height,
        ffmpeg::format::Pixel::BGRA,
        target_width,
        target_height,
        ffmpeg::software::scaling::Flags::BILINEAR,
    )
    .ok()?;

    let mut frames = Vec::new();
    let mut decoded = ffmpeg::util::frame::Video::empty();
    let mut converted = ffmpeg::util::frame::Video::empty();
    let mut previous_pts = None::<i64>;

    for (stream, packet) in input_ctx.packets() {
        if stream.index() != stream_index {
            continue;
        }
        if decoder.send_packet(&packet).is_err() {
            continue;
        }
        while decoder.receive_frame(&mut decoded).is_ok() {
            if frames.len() >= MAX_VIDEO_FRAME_COUNT {
                break;
            }
            if scaler.run(&decoded, &mut converted).is_err() {
                continue;
            }
            let frame_delay_ms =
                frame_delay_ms_from_pts(previous_pts, decoded.timestamp(), stream_time_base)
                    .unwrap_or(default_delay_ms);
            previous_pts = decoded.timestamp().or(previous_pts);
            if let Some(image_frame) = frame_to_image_frame(&converted, frame_delay_ms) {
                frames.push(image_frame);
            }
        }
        if frames.len() >= MAX_VIDEO_FRAME_COUNT {
            break;
        }
    }

    let _ = decoder.send_eof();
    while frames.len() < MAX_VIDEO_FRAME_COUNT && decoder.receive_frame(&mut decoded).is_ok() {
        if scaler.run(&decoded, &mut converted).is_err() {
            continue;
        }
        let frame_delay_ms =
            frame_delay_ms_from_pts(previous_pts, decoded.timestamp(), stream_time_base)
                .unwrap_or(default_delay_ms);
        previous_pts = decoded.timestamp().or(previous_pts);
        if let Some(image_frame) = frame_to_image_frame(&converted, frame_delay_ms) {
            frames.push(image_frame);
        }
    }

    if frames.is_empty() {
        return None;
    }

    Some(Arc::new(RenderImage::new(frames)))
}

fn frame_to_image_frame(frame: &ffmpeg::util::frame::Video, delay_ms: u32) -> Option<Frame> {
    let width = usize::try_from(frame.width()).ok()?;
    let height = usize::try_from(frame.height()).ok()?;
    if width == 0 || height == 0 {
        return None;
    }

    let stride = frame.stride(0);
    let row_bytes = width.checked_mul(4)?;
    if stride < row_bytes {
        return None;
    }
    let src_plane = frame.data(0);
    if src_plane.len() < stride.checked_mul(height)? {
        return None;
    }

    let mut packed = vec![0u8; row_bytes.checked_mul(height)?];
    for row in 0..height {
        let src_start = row.checked_mul(stride)?;
        let src_end = src_start.checked_add(row_bytes)?;
        let dst_start = row.checked_mul(row_bytes)?;
        let dst_end = dst_start.checked_add(row_bytes)?;
        packed[dst_start..dst_end].copy_from_slice(&src_plane[src_start..src_end]);
    }

    let width_u32 = u32::try_from(width).ok()?;
    let height_u32 = u32::try_from(height).ok()?;
    let image = RgbaImage::from_vec(width_u32, height_u32, packed)?;
    let delay = Delay::from_numer_denom_ms(delay_ms.max(1), 1);
    Some(Frame::from_parts(image, 0, 0, delay))
}

fn frame_delay_ms_from_rate(rate: ffmpeg::Rational) -> u32 {
    let fps = f64::from(rate);
    if !fps.is_finite() || fps <= 0.0 {
        return DEFAULT_FRAME_DELAY_MS;
    }
    let ms = (1000.0 / fps).round();
    ms.clamp(f64::from(MIN_FRAME_DELAY_MS), f64::from(MAX_FRAME_DELAY_MS)) as u32
}

fn frame_delay_ms_from_pts(
    previous_pts: Option<i64>,
    next_pts: Option<i64>,
    time_base: f64,
) -> Option<u32> {
    let prev = previous_pts?;
    let next = next_pts?;
    if !time_base.is_finite() || time_base <= 0.0 {
        return None;
    }
    let delta = next.saturating_sub(prev);
    if delta <= 0 {
        return None;
    }
    let millis = ((delta as f64) * time_base * 1000.0).round();
    if !millis.is_finite() {
        return None;
    }
    Some(millis.clamp(f64::from(MIN_FRAME_DELAY_MS), f64::from(MAX_FRAME_DELAY_MS)) as u32)
}

fn target_video_size(width: u32, height: u32) -> (u32, u32) {
    if width <= MAX_VIDEO_WIDTH {
        return (width.max(1), height.max(1));
    }
    let ratio = f64::from(MAX_VIDEO_WIDTH) / f64::from(width);
    let scaled_height = (f64::from(height) * ratio).round() as u32;
    (MAX_VIDEO_WIDTH, scaled_height.max(1))
}

/// Extracts the first video frame as a PNG thumbnail.
/// Returns `Some((width, height))` on success.
pub fn extract_video_thumbnail_to_file(
    video_path: &Path,
    output_png_path: &Path,
) -> Option<(u32, u32)> {
    if !*FFMPEG_INIT_OK.get_or_init(|| ffmpeg::init().is_ok()) {
        return None;
    }

    let mut input_ctx = ffmpeg::format::input(video_path).ok()?;
    let input_stream = input_ctx.streams().best(ffmpeg::media::Type::Video)?;
    let stream_index = input_stream.index();

    let context_decoder =
        ffmpeg::codec::context::Context::from_parameters(input_stream.parameters()).ok()?;
    let mut decoder = context_decoder.decoder().video().ok()?;
    let source_width = decoder.width();
    let source_height = decoder.height();
    if source_width == 0 || source_height == 0 {
        return None;
    }

    let (target_width, target_height) = target_video_size(source_width, source_height);
    let mut scaler = ffmpeg::software::scaling::Context::get(
        decoder.format(),
        source_width,
        source_height,
        ffmpeg::format::Pixel::RGBA,
        target_width,
        target_height,
        ffmpeg::software::scaling::Flags::BILINEAR,
    )
    .ok()?;

    let mut decoded_frame = ffmpeg::util::frame::Video::empty();
    let mut converted = ffmpeg::util::frame::Video::empty();

    for (stream, packet) in input_ctx.packets() {
        if stream.index() != stream_index {
            continue;
        }
        if decoder.send_packet(&packet).is_err() {
            continue;
        }
        if decoder.receive_frame(&mut decoded_frame).is_ok() {
            if scaler.run(&decoded_frame, &mut converted).is_ok() {
                return write_frame_as_png(
                    &converted,
                    target_width,
                    target_height,
                    output_png_path,
                );
            }
        }
    }

    let _ = decoder.send_eof();
    if decoder.receive_frame(&mut decoded_frame).is_ok()
        && scaler.run(&decoded_frame, &mut converted).is_ok()
    {
        return write_frame_as_png(&converted, target_width, target_height, output_png_path);
    }

    None
}

fn write_frame_as_png(
    frame: &ffmpeg::util::frame::Video,
    width: u32,
    height: u32,
    output_path: &Path,
) -> Option<(u32, u32)> {
    let w = usize::try_from(width).ok()?;
    let h = usize::try_from(height).ok()?;
    let stride = frame.stride(0);
    let row_bytes = w.checked_mul(4)?;
    if stride < row_bytes {
        return None;
    }
    let src_plane = frame.data(0);
    if src_plane.len() < stride.checked_mul(h)? {
        return None;
    }

    let mut packed = vec![0u8; row_bytes.checked_mul(h)?];
    for row in 0..h {
        let src_start = row.checked_mul(stride)?;
        let src_end = src_start.checked_add(row_bytes)?;
        let dst_start = row.checked_mul(row_bytes)?;
        let dst_end = dst_start.checked_add(row_bytes)?;
        packed[dst_start..dst_end].copy_from_slice(&src_plane[src_start..src_end]);
    }

    let img = RgbaImage::from_vec(width, height, packed)?;
    img.save(output_path).ok()?;
    Some((width, height))
}

fn next_temp_video_path() -> PathBuf {
    let nonce = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "zbase-link-preview-video-{}-{nonce}.bin",
        std::process::id()
    ))
}
