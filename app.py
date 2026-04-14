import subprocess
import os
import signal
import sys
import time
import threading
from datetime import datetime, timedelta

from config import LOCAL_PATH, VIDEO_SEGMENT_LEN
from db import init_db, insert_video, video_exists


OUT_VIDEO_DEVICE = "/dev/v4l/by-path/platform-xhci-hcd.0.auto-usb-0:1.3:1.0-video-index0"
OUT_AUDIO_DEVICE = "hw:Camera_1,0"

IN_VIDEO_DEVICE = "/dev/v4l/by-path/platform-xhci-hcd.10.auto-usb-0:1:1.0-video-index0"
IN_AUDIO_DEVICE = "hw:Camera,0"

OUT_VIRTUAL_VIDEO_DEVICE = "/dev/video40"
IN_VIRTUAL_VIDEO_DEVICE = "/dev/video41"

OUTPUT_DIR = LOCAL_PATH
SEGMENT_TIME = VIDEO_SEGMENT_LEN

# Recording input
WIDTH = 1920
HEIGHT = 1080
FPS = 20

# Virtual stream output
VIRTUAL_WIDTH = 1280
VIRTUAL_HEIGHT = 720
VIRTUAL_FPS = 15

RECONNECT_DELAY = 3
DB_SCAN_INTERVAL = 2
FILE_STABLE_SECONDS = 2

GLOBAL_VIDEO_PREFIX = "ADAS-VID"

os.makedirs(OUTPUT_DIR, exist_ok=True)

stop_event = threading.Event()
processes = {}
process_lock = threading.Lock()


def wait_until_next_segment_boundary():
    now = time.time()
    wait_seconds = SEGMENT_TIME - (int(now) % SEGMENT_TIME)
    if wait_seconds == SEGMENT_TIME:
        wait_seconds = 0

    if wait_seconds > 0:
        print(f"[INFO] Keyingi {SEGMENT_TIME}s boundary kutilmoqda: {wait_seconds} sec")
        time.sleep(wait_seconds)


def build_recording_ffmpeg_command(
    video_device,
    audio_device,
    channels,
    sample_rate,
    prefix,
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

        "-fflags", "genpts",
        "-thread_queue_size", "256",

        # VIDEO INPUT
        "-f", "v4l2",
        "-input_format", "mjpeg",
        "-framerate", str(FPS),
        "-video_size", f"{WIDTH}x{HEIGHT}",
        "-i", video_device,

        # AUDIO INPUT
        "-thread_queue_size", "1024",
        "-f", "alsa",
        "-channels", channels,
        "-sample_rate", sample_rate,
        "-i", audio_device,

        "-max_muxing_queue_size", "512",

        "-map", "0:v:0",
        "-map", "1:a:0",

        # VIDEO ENCODE
        "-c:v", "h264_rkmpp",
        "-b:v", "1800k",
        "-g", str(FPS * SEGMENT_TIME),
        "-keyint_min", str(FPS * SEGMENT_TIME),
        "-maxrate", "1800k",
        "-bufsize", "1800k",
        "-force_key_frames", f"expr:gte(t,n_forced*{SEGMENT_TIME})",

        # AUDIO ENCODE
        "-c:a", "aac",
        "-b:a", "96k",
        "-ar", "44100",
        "-af", "aresample=async=1000:min_hard_comp=0.100:first_pts=0",

        # SEGMENT OUTPUT
        "-f", "segment",
        "-segment_time", str(SEGMENT_TIME),
        "-segment_atclocktime", "1",
        "-segment_format", "mp4",
        "-reset_timestamps", "1",
        "-strftime", "1",
        timestamp_pattern,
    ]

    return cmd


def build_virtual_ffmpeg_command(
    video_device,
    virtual_video_device,
):
    cmd = [
        "ffmpeg",
        "-nostdin",
        "-hide_banner",
        "-loglevel", "warning",

        # Low latency
        "-fflags", "nobuffer+genpts+discardcorrupt",
        "-flags", "low_delay",
        "-avioflags", "direct",
        "-probesize", "32",
        "-analyzeduration", "0",
        "-flush_packets", "1",

        # VIDEO INPUT
        "-thread_queue_size", "64",
        "-f", "v4l2",
        "-input_format", "mjpeg",
        "-framerate", str(FPS),
        "-video_size", f"{WIDTH}x{HEIGHT}",
        "-i", video_device,

        "-map", "0:v:0",
        "-an",

        "-vf", (
            f"fps={VIRTUAL_FPS},"
            f"scale={VIRTUAL_WIDTH}:{VIRTUAL_HEIGHT}:flags=fast_bilinear,"
            f"format=yuv420p"
        ),
        "-pix_fmt", "yuv420p",
        "-f", "v4l2",
        virtual_video_device,
    ]

    return cmd


def check_video_device_exists(device_path):
    return os.path.exists(device_path)


def check_virtual_device_exists(device_path):
    return os.path.exists(device_path)


def terminate_process(proc, name):
    if not proc:
        return

    if proc.poll() is None:
        print(f"[INFO] {name}: process to'xtatilmoqda...")
        proc.terminate()
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            print(f"[WARN] {name}: process kill qilinmoqda...")
            proc.kill()
            try:
                proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                pass


def parse_segment_times_from_filename(file_name: str):
    base_name = os.path.basename(file_name)
    name_without_ext = os.path.splitext(base_name)[0]

    parts = name_without_ext.split("_", 1)
    if len(parts) != 2:
        return None, None, None, None

    camera_type, dt_part = parts

    try:
        start_dt = datetime.strptime(dt_part, "%Y-%m-%d_%H-%M-%S")
        end_dt = start_dt + timedelta(seconds=SEGMENT_TIME)
        segment_key = dt_part
        return (
            camera_type,
            start_dt.isoformat(),
            end_dt.isoformat(),
            segment_key
        )
    except ValueError:
        return None, None, None, None


def make_global_video_id(segment_key: str) -> str:
    dt = datetime.strptime(segment_key, "%Y-%m-%d_%H-%M-%S")
    return f"{GLOBAL_VIDEO_PREFIX}-{dt.strftime('%Y%m%d-%H%M%S')}"


def is_file_stable(file_path: str, stable_seconds: int = FILE_STABLE_SECONDS) -> bool:
    if not os.path.exists(file_path):
        return False

    size1 = os.path.getsize(file_path)
    time.sleep(stable_seconds)

    if not os.path.exists(file_path):
        return False

    size2 = os.path.getsize(file_path)
    return size1 == size2 and size2 > 0


def scan_and_insert_segments():
    print("[INFO] Segment DB watcher ishga tushdi")

    while not stop_event.is_set():
        try:
            files = sorted(
                f for f in os.listdir(OUTPUT_DIR)
                if f.lower().endswith(".mp4")
            )

            for file_name in files:
                file_path = os.path.join(OUTPUT_DIR, file_name)

                if video_exists(file_path):
                    continue

                if not is_file_stable(file_path):
                    continue

                camera_type, start_time, end_time, segment_key = parse_segment_times_from_filename(file_name)
                if not camera_type or not start_time or not end_time or not segment_key:
                    print(f"[WARN] DB watcher: filename parse bo'lmadi -> {file_name}")
                    continue

                global_video_id = make_global_video_id(segment_key)

                insert_video(
                    file_path=file_path,
                    camera_type=camera_type,
                    start_time=start_time,
                    end_time=end_time,
                    globalVideoId=global_video_id
                )

                print(
                    f"[DB] Video saqlandi: "
                    f"camera_type={camera_type}, "
                    f"file_path={file_path}, "
                    f"globalVideoId={global_video_id}"
                )

        except Exception as e:
            print(f"[DB WATCHER ERROR] {e}")

        time.sleep(DB_SCAN_INTERVAL)


def camera_worker(name, video_device, audio_device, virtual_video_device):
    global processes

    while not stop_event.is_set():
        video_ok = check_video_device_exists(video_device)
        virtual_ok = check_virtual_device_exists(virtual_video_device)

        if not video_ok:
            print(f"[WARN] {name}: video device yo'q -> {video_device}")
            time.sleep(RECONNECT_DELAY)
            continue

        if not virtual_ok:
            print(f"[WARN] {name}: virtual device yo'q -> {virtual_video_device}")
            time.sleep(RECONNECT_DELAY)
            continue

        record_cmd = build_recording_ffmpeg_command(
            video_device=video_device,
            audio_device=audio_device,
            channels="2",
            sample_rate="48000",
            prefix=name,
        )

        virtual_cmd = build_virtual_ffmpeg_command(
            video_device=video_device,
            virtual_video_device=virtual_video_device,
        )

        print(f"[INFO] {name}: recording va virtual process ishga tushiriladi")
        print(f"[INFO] {name}: VIDEO={video_device}")
        print(f"[INFO] {name}: AUDIO={audio_device}")
        print(f"[INFO] {name}: VIRTUAL={virtual_video_device}")

        wait_until_next_segment_boundary()

        record_proc = subprocess.Popen(record_cmd)
        virtual_proc = subprocess.Popen(virtual_cmd)

        with process_lock:
            processes[f"{name}_record"] = record_proc
            processes[f"{name}_virtual"] = virtual_proc

        while not stop_event.is_set():
            record_ret = record_proc.poll()
            virtual_ret = virtual_proc.poll()

            if record_ret is not None:
                print(f"[WARN] {name}: recording process to'xtab qoldi (code={record_ret})")
                break

            if virtual_ret is not None:
                print(f"[WARN] {name}: virtual process to'xtab qoldi (code={virtual_ret})")
                break

            time.sleep(1)

        terminate_process(record_proc, f"{name}_record")
        terminate_process(virtual_proc, f"{name}_virtual")

        with process_lock:
            processes[f"{name}_record"] = None
            processes[f"{name}_virtual"] = None

        if not stop_event.is_set():
            print(f"[INFO] {name}: qayta ulanishga urinish...")
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
    init_db()

    signal.signal(signal.SIGINT, stop_all)
    signal.signal(signal.SIGTERM, stop_all)

    if not check_virtual_device_exists(OUT_VIRTUAL_VIDEO_DEVICE):
        print(f"[ERROR] Virtual device topilmadi: {OUT_VIRTUAL_VIDEO_DEVICE}")
        sys.exit(1)

    if not check_virtual_device_exists(IN_VIRTUAL_VIDEO_DEVICE):
        print(f"[ERROR] Virtual device topilmadi: {IN_VIRTUAL_VIDEO_DEVICE}")
        sys.exit(1)

    print("[INFO] Split-process recording system boshlandi")
    print(f"[INFO] Papka: {OUTPUT_DIR}")
    print(f"[INFO] Segment: {SEGMENT_TIME} sekund")
    print(f"[INFO] Virtual stream: {VIRTUAL_WIDTH}x{VIRTUAL_HEIGHT} @ {VIRTUAL_FPS} fps")
    print("[INFO] Recording va virtual stream alohida processlarda ishlaydi")
    print("[INFO] OUT va IN bir vaqtdagi segmentlar bir xil globalVideoId oladi")
    print("[INFO] To'xtatish uchun CTRL+C bosing\n")

    db_thread = threading.Thread(target=scan_and_insert_segments, daemon=True)

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

    db_thread.start()
    out_thread.start()
    in_thread.start()

    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()