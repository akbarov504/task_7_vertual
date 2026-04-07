import subprocess
import os
import signal
import sys
import time

OUT_VIDEO_DEVICE = "/dev/video29"
OUT_AUDIO_DEVICE = "hw:4,0"

IN_VIDEO_DEVICE = "/dev/video25"
IN_AUDIO_DEVICE = "hw:3,0"

OUT_VIRTUAL_VIDEO_DEVICE = "/dev/video40"
IN_VIRTUAL_VIDEO_DEVICE = "/dev/video41"

OUTPUT_DIR = "records"
SEGMENT_TIME = 10

WIDTH = 1920
HEIGHT = 1080
FPS = 30

VIRTUAL_WIDTH = 1280
VIRTUAL_HEIGHT = 720
VIRTUAL_FPS = 15

os.makedirs(OUTPUT_DIR, exist_ok=True)
processes = []

def build_ffmpeg_command(
    video_device,
    audio_device,
    channels,
    sample_rate,
    prefix,
    virtual_video_device=None
):
    timestamp_pattern = os.path.join(
        OUTPUT_DIR,
        f"{prefix}_%Y-%m-%d_%H-%M-%S.mp4"
    )

    cmd = [
        "ffmpeg",
        "-nostdin",
        "-hide_banner",
        "-loglevel", "warning",
        
        # --- PHASE 1: INPUT OPTIMIZATION ---
        "-fflags", "nobuffer+genpts", # Prevent input buffering
        "-flags", "low_delay",        # Tell FFmpeg to prioritize speed
        "-probesize", "32",           # Analyze less data at start to reduce startup lag
        "-analyzeduration", "0",
        
        "-thread_queue_size", "1024",
        "-f", "v4l2",
        "-input_format", "mjpeg",
        "-framerate", str(FPS),
        "-video_size", f"{WIDTH}x{HEIGHT}",
        "-i", video_device,

        "-thread_queue_size", "1024",
        "-f", "alsa",
        "-channels", channels,
        "-sample_rate", sample_rate,
        "-i", audio_device,
    ]

    # --- PHASE 2: FILE RECORDING (Hardware) ---
    cmd += [
        "-map", "0:v:0",
        "-map", "1:a:0",
        "-c:v", "h264_rkmpp",
        "-b:v", "1800k",
        "-g", str(FPS),               # Reduced GOP to 1 second for faster recovery
        "-maxrate", "1800k",
        "-bufsize", "1800k",          # Tight buffer for low latency
        "-rc_mode", "vbr",
        
        "-c:a", "aac",
        "-b:a", "64k",
        "-af", "aresample=async=1",

        "-f", "segment",
        "-segment_time", str(SEGMENT_TIME),
        "-segment_format", "mp4",
        "-reset_timestamps", "1",
        "-strftime", "1",
        timestamp_pattern,
    ]

    # --- PHASE 3: VIRTUAL PORT (The Delay Fixer) ---
    if virtual_video_device:
        cmd += [
            "-map", "0:v:0",
            "-an",
            # Optimization: Scale first, then drop FPS to 15 immediately to save CPU
            "-vf", f"fps={VIRTUAL_FPS},scale={VIRTUAL_WIDTH}:{VIRTUAL_HEIGHT}:flags=lanczos,format=yuv420p",
            "-c:v", "rawvideo",
            "-pix_fmt", "yuv420p",
            "-f", "v4l2",
            # This allows the virtual port to "drop frames" if it can't keep up
            # preventing the 5-second delay from building up.
            "-timestamp", "now",
            virtual_video_device,
        ]

    return cmd

def stop_ffmpeg(signum=None, frame=None):
    global processes
    print("\n[INFO] Hamma yozuv jarayonlari to'xtatilmoqda...")

    for p in processes:
        if p and p.poll() is None:
            p.terminate()

    time.sleep(2)

    for p in processes:
        if p and p.poll() is None:
            p.kill()

    print("[INFO] Hamma FFmpeg jarayonlari to'xtatildi.")
    sys.exit(0)

def check_device_exists(device_path):
    if not os.path.exists(device_path):
        print(f"[ERROR] Device topilmadi: {device_path}")
        return False
    return True

def main():
    global processes

    required_devices = [
        OUT_VIDEO_DEVICE,
        IN_VIDEO_DEVICE,
        OUT_VIRTUAL_VIDEO_DEVICE,
        IN_VIRTUAL_VIDEO_DEVICE,
    ]

    for device in required_devices:
        if device.startswith("/dev/video"):
            if not check_device_exists(device):
                sys.exit(1)

    cmd_out = build_ffmpeg_command(
        video_device=OUT_VIDEO_DEVICE,
        audio_device=OUT_AUDIO_DEVICE,
        channels="2",
        sample_rate="48000",
        prefix="OUT",
        virtual_video_device=OUT_VIRTUAL_VIDEO_DEVICE
    )

    cmd_in = build_ffmpeg_command(
        video_device=IN_VIDEO_DEVICE,
        audio_device=IN_AUDIO_DEVICE,
        channels="2",
        sample_rate="48000",
        prefix="IN",
        virtual_video_device=IN_VIRTUAL_VIDEO_DEVICE
    )

    print("[INFO] Live recording boshlandi (2 kamera + virtual camera)")
    print(f"[INFO] 1-Kamera (OUT): {OUT_VIDEO_DEVICE} | Mic: {OUT_AUDIO_DEVICE} | Virtual: {OUT_VIRTUAL_VIDEO_DEVICE}")
    print(f"[INFO] 2-Kamera (IN) : {IN_VIDEO_DEVICE} | Mic: {IN_AUDIO_DEVICE} | Virtual: {IN_VIRTUAL_VIDEO_DEVICE}")
    print(f"[INFO] Papka: {OUTPUT_DIR}")
    print(f"[INFO] Segment: {SEGMENT_TIME} sekund")
    print(f"[INFO] Virtual stream: {VIRTUAL_WIDTH}x{VIRTUAL_HEIGHT} @ {VIRTUAL_FPS} fps")
    print("[INFO] To'xtatish uchun CTRL+C bosing\n")

    signal.signal(signal.SIGINT, stop_ffmpeg)
    signal.signal(signal.SIGTERM, stop_ffmpeg)

    process_out = subprocess.Popen(cmd_out)
    process_in = subprocess.Popen(cmd_in)

    processes.append(process_out)
    processes.append(process_in)

    process_out.wait()
    process_in.wait()

if __name__ == "__main__":
    main()
