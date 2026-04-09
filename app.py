import subprocess
import os
import signal
import sys
import time
import threading

OUT_VIDEO_DEVICE = "/dev/v4l/by-path/platform-xhci-hcd.0.auto-usb-0:1.3:1.0-video-index0"
OUT_AUDIO_DEVICE = "hw:Camera_1,0"

IN_VIDEO_DEVICE = "/dev/v4l/by-path/platform-xhci-hcd.10.auto-usb-0:1:1.0-video-index0"
IN_AUDIO_DEVICE = "hw:Camera,0"

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

RECONNECT_DELAY = 3

os.makedirs(OUTPUT_DIR, exist_ok=True)

stop_event = threading.Event()
processes = {}
process_lock = threading.Lock()

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
        "-fflags", "nobuffer+genpts",
        "-flags", "low_delay",
        "-probesize", "32",
        "-analyzeduration", "0",

        "-thread_queue_size", "2048",
        "-f", "v4l2",
        "-input_format", "mjpeg",
        "-framerate", str(FPS),
        "-video_size", f"{WIDTH}x{HEIGHT}",
        "-i", video_device,

        "-thread_queue_size", "2048",
        "-f", "alsa",
        "-channels", channels,
        "-sample_rate", sample_rate,
        "-i", audio_device,

        "-max_muxing_queue_size", "1024",
    ]

    cmd += [
        "-map", "0:v:0",
        "-map", "1:a:0",

        "-c:v", "h264_rkmpp",
        "-b:v", "1800k",
        "-g", str(FPS * 2),
        "-maxrate", "1800k",
        "-bufsize", "3600k",

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

    if virtual_video_device:
        cmd += [
            "-map", "0:v:0",
            "-an",
            "-vf", f"fps={VIRTUAL_FPS},scale={VIRTUAL_WIDTH}:{VIRTUAL_HEIGHT}:flags=lanczos,format=yuyv422",
            "-c:v", "rawvideo",
            "-pix_fmt", "yuyv422",
            "-f", "v4l2",
            virtual_video_device,
        ]

    return cmd

def check_video_device_exists(device_path):
    return os.path.exists(device_path)

def check_virtual_device_exists(device_path):
    return os.path.exists(device_path)

def check_audio_device_exists(card_name):
    try:
        result = subprocess.run(
            ["arecord", "-L"],
            capture_output=True,
            text=True,
            timeout=5
        )
        return card_name in result.stdout
    except Exception:
        return False

def terminate_process(proc, name):
    if not proc:
        return

    if proc.poll() is None:
        print(f"[INFO] {name}: ffmpeg to'xtatilmoqda...")
        proc.terminate()
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            print(f"[WARN] {name}: ffmpeg kill qilinmoqda...")
            proc.kill()
            try:
                proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                pass

def camera_worker(name, video_device, audio_device, virtual_video_device):
    global processes

    while not stop_event.is_set():
        video_ok = check_video_device_exists(video_device)
        audio_ok = check_audio_device_exists(audio_device)
        virtual_ok = check_virtual_device_exists(virtual_video_device)

        if not video_ok:
            print(f"[WARN] {name}: video device yo'q -> {video_device}")
            time.sleep(RECONNECT_DELAY)
            continue

        if not audio_ok:
            print(f"[WARN] {name}: audio device yo'q -> {audio_device}")
            time.sleep(RECONNECT_DELAY)
            continue

        if not virtual_ok:
            print(f"[WARN] {name}: virtual device yo'q -> {virtual_video_device}")
            time.sleep(RECONNECT_DELAY)
            continue

        cmd = build_ffmpeg_command(
            video_device=video_device,
            audio_device=audio_device,
            channels="2",
            sample_rate="48000",
            prefix=name,
            virtual_video_device=virtual_video_device
        )

        print(f"[INFO] {name}: ffmpeg ishga tushirildi")
        print(f"[INFO] {name}: VIDEO={video_device}")
        print(f"[INFO] {name}: AUDIO={audio_device}")
        print(f"[INFO] {name}: VIRTUAL={virtual_video_device}")

        proc = subprocess.Popen(cmd)

        with process_lock:
            processes[name] = proc

        while not stop_event.is_set():
            ret = proc.poll()
            if ret is not None:
                print(f"[WARN] {name}: ffmpeg to'xtab qoldi (code={ret}). Qayta ulanish kutilmoqda...")
                break
            time.sleep(1)

        terminate_process(proc, name)

        with process_lock:
            processes[name] = None

        if not stop_event.is_set():
            time.sleep(RECONNECT_DELAY)

def stop_all(signum=None, frame=None):
    print("\n[INFO] Dastur to'xtatilmoqda...")
    stop_event.set()

    with process_lock:
        for name, proc in processes.items():
            terminate_process(proc, name)

    print("[INFO] Hamma jarayonlar to'xtatildi.")
    sys.exit(0)

def main():
    signal.signal(signal.SIGINT, stop_all)
    signal.signal(signal.SIGTERM, stop_all)

    if not check_virtual_device_exists(OUT_VIRTUAL_VIDEO_DEVICE):
        print(f"[ERROR] Virtual device topilmadi: {OUT_VIRTUAL_VIDEO_DEVICE}")
        sys.exit(1)

    if not check_virtual_device_exists(IN_VIRTUAL_VIDEO_DEVICE):
        print(f"[ERROR] Virtual device topilmadi: {IN_VIRTUAL_VIDEO_DEVICE}")
        sys.exit(1)

    print("[INFO] Auto-reconnect recording system boshlandi")
    print(f"[INFO] Papka: {OUTPUT_DIR}")
    print(f"[INFO] Segment: {SEGMENT_TIME} sekund")
    print(f"[INFO] Virtual stream: {VIRTUAL_WIDTH}x{VIRTUAL_HEIGHT} @ {VIRTUAL_FPS} fps")
    print("[INFO] Kamera sug'urilsa, dastur kutadi va qayta tiqilganda avtomatik ishga tushadi")
    print("[INFO] To'xtatish uchun CTRL+C bosing\n")

    out_thread = threading.Thread(
        target=camera_worker,
        args=("OUT", OUT_VIDEO_DEVICE, OUT_AUDIO_DEVICE, OUT_VIRTUAL_VIDEO_DEVICE),
        daemon=True
    )

    in_thread = threading.Thread(
        target=camera_worker,
        args=("IN", IN_VIDEO_DEVICE, IN_AUDIO_DEVICE, IN_VIRTUAL_VIDEO_DEVICE),
        daemon=True
    )

    out_thread.start()
    in_thread.start()

    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()
