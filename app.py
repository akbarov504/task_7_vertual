import subprocess
import os
import signal
import sys
import time
import threading
import math
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

WIDTH = 1920
HEIGHT = 1080
FPS = 20

VIRTUAL_WIDTH = 640
VIRTUAL_HEIGHT = 640
VIRTUAL_FPS = 20

RECONNECT_DELAY = 3
DB_SCAN_INTERVAL = 2
FILE_STABLE_SECONDS = 2

VIDEO_ID_NAMESPACE = "TRUCK_VIN"

os.makedirs(OUTPUT_DIR, exist_ok=True)

stop_event = threading.Event()
main_process = None
process_lock = threading.Lock()


def now_ts() -> float:
    return time.time()


def format_ts(ts: float) -> str:
    dt = datetime.fromtimestamp(ts)
    return dt.strftime("%Y-%m-%d %H:%M:%S.") + f"{dt.microsecond:06d}"


def get_next_aligned_time(segment_seconds: int, after_ts: float = None) -> float:
    if after_ts is None:
        after_ts = now_ts()
    return math.ceil(after_ts / segment_seconds) * segment_seconds


def wait_until_boundary():
    target_ts = get_next_aligned_time(SEGMENT_TIME)
    print(f"[SYNC] Keyingi aligned start: {format_ts(target_ts)}")

    while not stop_event.is_set():
        remain = target_ts - now_ts()
        if remain <= 0:
            break

        if remain > 1:
            print(f"[SYNC] Startgacha {remain:.2f}s qoldi")
            time.sleep(0.5)
        elif remain > 0.01:
            time.sleep(0.001)
        else:
            while not stop_event.is_set() and now_ts() < target_ts:
                pass
            break

    actual = now_ts()
    diff_us = int((actual - target_ts) * 1_000_000)
    print(f"[SYNC] Real start point: {format_ts(actual)} diff_us={diff_us}")


def check_video_device_exists(device_path):
    return os.path.exists(device_path)


def check_virtual_device_exists(device_path):
    return os.path.exists(device_path)


def terminate_process(proc, name="FFMPEG"):
    if not proc:
        return

    if proc.poll() is None:
        print(f"[INFO] {name}: ffmpeg to'xtatilmoqda...")
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            print(f"[WARN] {name}: ffmpeg kill qilinmoqda...")
            proc.kill()
            try:
                proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                pass


def parse_segment_times_from_filename(file_name: str):
    """
    OUT_2026-04-09_13-10-00.mp4
    IN_2026-04-09_13-10-00.mp4
    """
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
        return camera_type, start_dt.isoformat(), end_dt.isoformat(), segment_key
    except ValueError:
        return None, None, None, None


def make_global_video_id(segment_key: str) -> str:
    return str(VIDEO_ID_NAMESPACE + "_" + segment_key)


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


def build_single_ffmpeg_command():
    out_timestamp_pattern = os.path.join(OUTPUT_DIR, "OUT_%Y-%m-%d_%H-%M-%S.mp4")
    in_timestamp_pattern = os.path.join(OUTPUT_DIR, "IN_%Y-%m-%d_%H-%M-%S.mp4")

    cmd = [
        "ffmpeg",
        "-nostdin",
        "-hide_banner",
        "-loglevel", "warning",

        # -------- INPUT 0: OUT VIDEO --------
        "-thread_queue_size", "256",
        "-f", "v4l2",
        "-input_format", "mjpeg",
        "-framerate", str(FPS),
        "-video_size", f"{WIDTH}x{HEIGHT}",
        "-use_wallclock_as_timestamps", "1",
        "-i", OUT_VIDEO_DEVICE,

        # -------- INPUT 1: OUT AUDIO --------
        "-thread_queue_size", "256",
        "-f", "alsa",
        "-channels", "2",
        "-sample_rate", "48000",
        "-i", OUT_AUDIO_DEVICE,

        # -------- INPUT 2: IN VIDEO --------
        "-thread_queue_size", "256",
        "-f", "v4l2",
        "-input_format", "mjpeg",
        "-framerate", str(FPS),
        "-video_size", f"{WIDTH}x{HEIGHT}",
        "-use_wallclock_as_timestamps", "1",
        "-i", IN_VIDEO_DEVICE,

        # -------- INPUT 3: IN AUDIO --------
        "-thread_queue_size", "256",
        "-f", "alsa",
        "-channels", "2",
        "-sample_rate", "48000",
        "-i", IN_AUDIO_DEVICE,

        "-max_muxing_queue_size", "1024",
    ]

    # ---------------- OUT RECORDING ----------------
    cmd += [
        "-map", "0:v:0",
        "-map", "1:a:0",
        "-c:v", "h264_rkmpp",
        "-b:v", "1800k",
        "-g", str(FPS * SEGMENT_TIME),
        "-keyint_min", str(FPS * SEGMENT_TIME),
        "-maxrate", "1800k",
        "-bufsize", "1800k",
        "-force_key_frames", f"expr:gte(t,n_forced*{SEGMENT_TIME})",
        "-c:a", "aac",
        "-b:a", "64k",
        "-af", "aresample=async=1:first_pts=0",
        "-f", "segment",
        "-segment_time", str(SEGMENT_TIME),
        "-segment_atclocktime", "1",
        "-segment_clocktime_offset", "0",
        "-strftime", "1",
        "-reset_timestamps", "1",
        "-segment_format", "mp4",
        out_timestamp_pattern,
    ]

    # ---------------- OUT VIRTUAL ----------------
    cmd += [
        "-map", "0:v:0",
        "-an",
        "-vf", (
            f"fps={VIRTUAL_FPS},"
            f"scale={VIRTUAL_WIDTH}:{VIRTUAL_HEIGHT}:flags=fast_bilinear,"
            f"format=yuv420p"
        ),
        "-pix_fmt", "yuv420p",
        "-f", "v4l2",
        OUT_VIRTUAL_VIDEO_DEVICE,
    ]

    # ---------------- IN RECORDING ----------------
    cmd += [
        "-map", "2:v:0",
        "-map", "3:a:0",
        "-c:v", "h264_rkmpp",
        "-b:v", "1800k",
        "-g", str(FPS * SEGMENT_TIME),
        "-keyint_min", str(FPS * SEGMENT_TIME),
        "-maxrate", "1800k",
        "-bufsize", "1800k",
        "-force_key_frames", f"expr:gte(t,n_forced*{SEGMENT_TIME})",
        "-c:a", "aac",
        "-b:a", "64k",
        "-af", "aresample=async=1:first_pts=0",
        "-f", "segment",
        "-segment_time", str(SEGMENT_TIME),
        "-segment_atclocktime", "1",
        "-segment_clocktime_offset", "0",
        "-strftime", "1",
        "-reset_timestamps", "1",
        "-segment_format", "mp4",
        in_timestamp_pattern,
    ]

    # ---------------- IN VIRTUAL ----------------
    cmd += [
        "-map", "2:v:0",
        "-an",
        "-vf", (
            f"fps={VIRTUAL_FPS},"
            f"scale={VIRTUAL_WIDTH}:{VIRTUAL_HEIGHT}:flags=fast_bilinear,"
            f"format=yuv420p"
        ),
        "-pix_fmt", "yuv420p",
        "-f", "v4l2",
        IN_VIRTUAL_VIDEO_DEVICE,
    ]

    return cmd


def devices_ready():
    checks = [
        ("OUT video", OUT_VIDEO_DEVICE),
        ("OUT audio", OUT_AUDIO_DEVICE),
        ("IN video", IN_VIDEO_DEVICE),
        ("IN audio", IN_AUDIO_DEVICE),
        ("OUT virtual", OUT_VIRTUAL_VIDEO_DEVICE),
        ("IN virtual", IN_VIRTUAL_VIDEO_DEVICE),
    ]

    for label, path in checks:
        if "audio" in label.lower():
            # alsa path ni os.path.exists bilan tekshirmaymiz
            continue

        if not os.path.exists(path):
            print(f"[WARN] {label} device yo'q -> {path}")
            return False

    return True


def ffmpeg_supervisor():
    global main_process

    while not stop_event.is_set():
        if not devices_ready():
            time.sleep(RECONNECT_DELAY)
            continue

        wait_until_boundary()

        if stop_event.is_set():
            break

        cmd = build_single_ffmpeg_command()

        print("[INFO] SINGLE FFMPEG ishga tushirilmoqda...")
        print(f"[INFO] OUT_VIDEO={OUT_VIDEO_DEVICE}")
        print(f"[INFO] OUT_AUDIO={OUT_AUDIO_DEVICE}")
        print(f"[INFO] IN_VIDEO={IN_VIDEO_DEVICE}")
        print(f"[INFO] IN_AUDIO={IN_AUDIO_DEVICE}")
        print(f"[INFO] OUT_VIRTUAL={OUT_VIRTUAL_VIDEO_DEVICE}")
        print(f"[INFO] IN_VIRTUAL={IN_VIRTUAL_VIDEO_DEVICE}")

        proc = subprocess.Popen(cmd)

        with process_lock:
            main_process = proc

        print(f"[INFO] SINGLE FFMPEG PID={proc.pid} start={format_ts(now_ts())}")

        while not stop_event.is_set():
            ret = proc.poll()
            if ret is not None:
                print(f"[WARN] SINGLE FFMPEG to'xtab qoldi (code={ret}). Qayta ishga tushiriladi...")
                break
            time.sleep(1)

        terminate_process(proc, "SINGLE_FFMPEG")

        with process_lock:
            main_process = None

        if not stop_event.is_set():
            time.sleep(RECONNECT_DELAY)


def stop_all(signum=None, frame=None):
    global main_process

    print("\n[INFO] Dastur to'xtatilmoqda...")
    stop_event.set()

    with process_lock:
        terminate_process(main_process, "SINGLE_FFMPEG")

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

    print("[INFO] Single-process sync recording system boshlandi")
    print(f"[INFO] Papka: {OUTPUT_DIR}")
    print(f"[INFO] Segment: {SEGMENT_TIME} sekund")
    print(f"[INFO] Virtual stream: {VIRTUAL_WIDTH}x{VIRTUAL_HEIGHT} @ {VIRTUAL_FPS} fps")
    print("[INFO] OUT va IN endi bitta ffmpeg ichida yoziladi")
    print("[INFO] Bu variant alohida 2 process variantdan ancha aniq sync beradi")
    print("[INFO] To'xtatish uchun CTRL+C bosing\n")

    db_thread = threading.Thread(target=scan_and_insert_segments, daemon=True)
    ffmpeg_thread = threading.Thread(target=ffmpeg_supervisor, daemon=True)

    db_thread.start()
    ffmpeg_thread.start()

    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()