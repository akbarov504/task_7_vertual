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

IN_VIDEO_DEVICE  = "/dev/v4l/by-path/platform-xhci-hcd.10.auto-usb-0:1:1.0-video-index0"
IN_AUDIO_DEVICE  = "hw:Camera,0"

OUT_VIRTUAL_VIDEO_DEVICE = "/dev/video40"
IN_VIRTUAL_VIDEO_DEVICE  = "/dev/video41"

OUTPUT_DIR   = LOCAL_PATH
SEGMENT_TIME = VIDEO_SEGMENT_LEN

WIDTH  = 1920
HEIGHT = 1080
FPS    = 20

VIRTUAL_WIDTH  = 640
VIRTUAL_HEIGHT = 640
VIRTUAL_FPS    = 20

RECONNECT_DELAY     = 3
DB_SCAN_INTERVAL    = 2
FILE_STABLE_SECONDS = 2

VIDEO_ID_NAMESPACE = "TRUCK_VIN"

# Master clock tuning
PREPARE_AHEAD_SECONDS  = 1.5    # boundary dan oldin uyg'onish
COARSE_SLEEP_THRESHOLD = 0.050
FINE_SLEEP_THRESHOLD   = 0.002

os.makedirs(OUTPUT_DIR, exist_ok=True)

stop_event   = threading.Event()
processes    = {}
process_lock = threading.Lock()

# ── Master Clock state ─────────────────────────────────────────────────────────
_tick_lock      = threading.Lock()
_tick_event     = threading.Event()
_tick_target_ts = 0.0


def now_ts() -> float:
    return time.time()


def format_ts(ts: float) -> str:
    dt = datetime.fromtimestamp(ts)
    return dt.strftime("%Y-%m-%d %H:%M:%S.") + f"{dt.microsecond:06d}"


def get_next_aligned_time(segment_seconds: int, after_ts: float = None) -> float:
    if after_ts is None:
        after_ts = now_ts()
    eps = 1e-9
    return math.floor(
        (after_ts + segment_seconds - eps) / segment_seconds
    ) * segment_seconds


def wait_until_precise(target_ts: float, name: str):
    """Busy-wait bilan target vaqtga aniq yetish."""
    while not stop_event.is_set():
        remaining = target_ts - now_ts()
        if remaining <= 0:
            break
        if remaining > COARSE_SLEEP_THRESHOLD:
            time.sleep(min(remaining * 0.8, 0.020))
        elif remaining > FINE_SLEEP_THRESHOLD:
            time.sleep(0.001)
        else:
            while not stop_event.is_set() and now_ts() < target_ts:
                pass
            break

    diff_us = int((now_ts() - target_ts) * 1_000_000)
    print(f"[CLOCK] {name}: diff_us={diff_us}")


def master_clock():
    """
    Global segment boundary clock.

    Har SEGMENT_TIME da bir marta tick beradi.
    Barcha camera_worker lar shu tick ni kutib Popen qiladi.

    Natija:
    - Birinchi start da: ikkalasi bir xil tick da boshlanadi
    - Reconnect da: uzilgan worker keyingi tick ni kutadi,
      ishlayotgan worker o'z ticki da davom etadi —
      lekin keyingi tick da ular yana aligned bo'ladi
    """
    global _tick_event, _tick_target_ts

    print("[CLOCK] Master clock ishga tushdi")

    while not stop_event.is_set():
        target_ts = get_next_aligned_time(SEGMENT_TIME, now_ts())
        print(f"[CLOCK] Keyingi tick -> {format_ts(target_ts)}")

        # PREPARE_AHEAD_SECONDS oldin uyg'onamiz
        sleep_until = target_ts - PREPARE_AHEAD_SECONDS
        remaining   = sleep_until - now_ts()
        if remaining > 0:
            time.sleep(remaining)

        if stop_event.is_set():
            break

        # Yangi event yaratamiz — workerlar buni kutayotgan bo'lishi mumkin
        new_event = threading.Event()
        with _tick_lock:
            _tick_target_ts = target_ts
            _tick_event     = new_event

        # Aynan boundary da fire
        wait_until_precise(target_ts, "MASTER")

        if stop_event.is_set():
            break

        new_event.set()
        print(f"[CLOCK] >>> TICK <<< {format_ts(target_ts)}")

        # O'sha boundary ni qayta hisoblamamaslik uchun yarim sekund ilgarilash
        time.sleep(0.5)

    print("[CLOCK] Master clock to'xtatildi")


def wait_for_next_tick() -> float:
    """
    Keyingi master tick ni kutadi va target_ts ni qaytaradi.
    Worker har Popen oldidan shu funksiyani chaqiradi.
    """
    # Joriy event va ts ni olamiz
    with _tick_lock:
        ev = _tick_event
        ts = _tick_target_ts

    # Event fire bo'lguncha kutamiz (100ms polling)
    while not stop_event.is_set():
        if ev.wait(timeout=0.1):
            return ts
        # Master yangi event yaratgan bo'lishi mumkin — tekshiramiz
        with _tick_lock:
            if _tick_event is not ev:
                ev = _tick_event
                ts = _tick_target_ts

    return 0.0


# ── ffmpeg command builder ─────────────────────────────────────────────────────

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

    channel_layout = "stereo" if channels == "2" else "mono"

    cmd = [
        "ffmpeg",
        "-nostdin",
        "-hide_banner",
        "-loglevel", "warning",

        # Low latency
        "-fflags",          "nobuffer+genpts",
        "-flags",           "low_delay",
        "-avioflags",       "direct",
        "-probesize",       "32",
        "-analyzeduration", "0",

        # VIDEO INPUT
        "-thread_queue_size", "4096",
        "-f",            "v4l2",
        "-input_format", "mjpeg",
        "-framerate",    str(FPS),
        "-video_size",   f"{WIDTH}x{HEIGHT}",
        "-i",            video_device,

        # AUDIO INPUT — channel_layout explicit (warning 1 yo'qoladi)
        "-thread_queue_size", "4096",
        "-f",              "alsa",
        "-channels",       channels,
        "-sample_rate",    sample_rate,
        "-channel_layout", channel_layout,
        "-i",              audio_device,

        "-max_muxing_queue_size", "4096",
    ]

    # RECORDING OUTPUT
    cmd += [
        "-map", "0:v:0",
        "-map", "1:a:0",

        "-c:v",        "h264_rkmpp",
        "-b:v",        "1800k",
        "-g",          str(FPS * SEGMENT_TIME),
        "-keyint_min", str(FPS * SEGMENT_TIME),
        "-maxrate",    "1800k",
        "-bufsize",    "1800k",
        "-force_key_frames", f"expr:gte(t,n_forced*{SEGMENT_TIME})",

        "-c:a", "aac",
        "-b:a", "64k",
        "-af",  "aresample=async=1:first_pts=0",

        "-f",                        "segment",
        "-segment_time",             str(SEGMENT_TIME),
        "-segment_atclocktime",      "1",
        "-segment_clocktime_offset", "0",
        "-segment_format",           "mp4",
        "-reset_timestamps",         "1",
        "-strftime",                 "1",
        timestamp_pattern,
    ]

    # VIRTUAL CAMERA OUTPUT — color_range explicit (warning 2 yo'qoladi)
    if virtual_video_device:
        cmd += [
            "-map", "0:v:0",
            "-an",
            "-vf", (
                f"fps={VIRTUAL_FPS},"
                f"scale={VIRTUAL_WIDTH}:{VIRTUAL_HEIGHT}:flags=fast_bilinear,"
                f"format=yuv420p"
            ),
            "-pix_fmt",     "yuv420p",
            "-color_range", "tv",
            "-f",           "v4l2",
            virtual_video_device,
        ]

    return cmd


# ── Helpers ────────────────────────────────────────────────────────────────────

def check_device_exists(device_path):
    return os.path.exists(device_path)


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


def parse_segment_times_from_filename(file_name: str):
    base_name        = os.path.basename(file_name)
    name_without_ext = os.path.splitext(base_name)[0]
    parts            = name_without_ext.split("_", 1)

    if len(parts) != 2:
        return None, None, None, None

    camera_type, dt_part = parts

    try:
        start_dt    = datetime.strptime(dt_part, "%Y-%m-%d_%H-%M-%S")
        end_dt      = start_dt + timedelta(seconds=SEGMENT_TIME)
        segment_key = dt_part
        return (
            camera_type,
            start_dt.isoformat(),
            end_dt.isoformat(),
            segment_key,
        )
    except ValueError:
        return None, None, None, None


def make_global_video_id(segment_key: str) -> str:
    return f"{VIDEO_ID_NAMESPACE}_{segment_key}"


def is_file_stable(file_path: str, stable_seconds: int = FILE_STABLE_SECONDS) -> bool:
    if not os.path.exists(file_path):
        return False
    size1 = os.path.getsize(file_path)
    time.sleep(stable_seconds)
    if not os.path.exists(file_path):
        return False
    size2 = os.path.getsize(file_path)
    return size1 == size2 and size2 > 0


# ── DB watcher ─────────────────────────────────────────────────────────────────

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

                camera_type, start_time, end_time, segment_key = \
                    parse_segment_times_from_filename(file_name)

                if not all([camera_type, start_time, end_time, segment_key]):
                    print(f"[WARN] DB watcher: parse bo'lmadi -> {file_name}")
                    continue

                global_video_id = make_global_video_id(segment_key)

                insert_video(
                    file_path=file_path,
                    camera_type=camera_type,
                    start_time=start_time,
                    end_time=end_time,
                    globalVideoId=global_video_id,
                )

                print(
                    f"[DB] Saqlandi: camera={camera_type} "
                    f"id={global_video_id} path={file_path}"
                )

        except Exception as e:
            print(f"[DB WATCHER ERROR] {e}")

        time.sleep(DB_SCAN_INTERVAL)


# ── Camera worker ──────────────────────────────────────────────────────────────

def camera_worker(name, video_device, audio_device, virtual_video_device):
    """
    Har safar:
      1) Device larni tekshiradi
      2) Master clock tick ni kutadi  ← sync shu yerda
      3) Tick kelganda Popen qiladi
      4) ffmpeg o'lsa — 1-ga qaytadi, keyingi tick da restart

    Reconnect scenariysi:
      OUT 10:00:23 da uzilib, 10:00:41 da qayta ulandi
      OUT keyingi tick = 10:00:50 ni kutadi
      IN  10:00:50 da o'z segmentini yopib yangi ochadi
      => 10:00:50 dan ikkalasi yana aligned
    """
    global processes

    print(f"[WORKER] {name}: ishga tushdi")

    while not stop_event.is_set():

        # 1) Device tekshiruv
        if not check_device_exists(video_device):
            print(f"[WARN] {name}: video device yo'q -> {video_device}")
            time.sleep(RECONNECT_DELAY)
            continue

        if not check_device_exists(virtual_video_device):
            print(f"[WARN] {name}: virtual device yo'q -> {virtual_video_device}")
            time.sleep(RECONNECT_DELAY)
            continue

        # 2) CMD tayyor
        cmd = build_ffmpeg_command(
            video_device=video_device,
            audio_device=audio_device,
            channels="2",
            sample_rate="48000",
            prefix=name,
            virtual_video_device=virtual_video_device,
        )

        # 3) Master tick ni kut
        print(f"[WORKER] {name}: keyingi tick kutmoqda...")
        target_ts = wait_for_next_tick()

        if stop_event.is_set():
            break

        # 4) Popen
        delta_us = int((now_ts() - target_ts) * 1_000_000)
        proc     = subprocess.Popen(cmd)
        print(f"[WORKER] {name}: Popen delta_us={delta_us} PID={proc.pid}")

        with process_lock:
            processes[name] = proc

        # 5) ffmpeg kuzatuvi
        while not stop_event.is_set():
            ret = proc.poll()
            if ret is not None:
                print(f"[WARN] {name}: ffmpeg to'xtadi (code={ret}). Qayta ulanish...")
                break
            time.sleep(1)

        terminate_process(proc, name)

        with process_lock:
            processes[name] = None

        if not stop_event.is_set():
            time.sleep(RECONNECT_DELAY)

    print(f"[WORKER] {name}: to'xtatildi")


# ── Signal handler ─────────────────────────────────────────────────────────────

def stop_all(signum=None, frame=None):
    print("\n[INFO] Dastur to'xtatilmoqda...")
    stop_event.set()

    with process_lock:
        for name, proc in processes.items():
            terminate_process(proc, name)

    print("[INFO] Hamma jarayonlar to'xtatildi.")
    sys.exit(0)


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    init_db()

    signal.signal(signal.SIGINT,  stop_all)
    signal.signal(signal.SIGTERM, stop_all)

    if not check_device_exists(OUT_VIRTUAL_VIDEO_DEVICE):
        print(f"[ERROR] Virtual device topilmadi: {OUT_VIRTUAL_VIDEO_DEVICE}")
        sys.exit(1)

    if not check_device_exists(IN_VIRTUAL_VIDEO_DEVICE):
        print(f"[ERROR] Virtual device topilmadi: {IN_VIRTUAL_VIDEO_DEVICE}")
        sys.exit(1)

    print("[INFO] Recording system boshlandi")
    print(f"[INFO] Papka    : {OUTPUT_DIR}")
    print(f"[INFO] Segment  : {SEGMENT_TIME} sekund")
    print(f"[INFO] Virtual  : {VIRTUAL_WIDTH}x{VIRTUAL_HEIGHT} @ {VIRTUAL_FPS} fps")
    print("[INFO] Kamera sug'urilsa — keyingi tick da avtomatik boshlanadi")
    print("[INFO] OUT va IN bir tick da boshlangan segmentlar bir xil globalVideoId oladi")
    print("[INFO] To'xtatish uchun CTRL+C\n")

    clock_thread = threading.Thread(
        target=master_clock, daemon=True, name="MasterClock"
    )
    db_thread = threading.Thread(
        target=scan_and_insert_segments, daemon=True, name="DBWatcher"
    )
    out_thread = threading.Thread(
        target=camera_worker,
        args=("OUT", OUT_VIDEO_DEVICE, OUT_AUDIO_DEVICE, OUT_VIRTUAL_VIDEO_DEVICE),
        daemon=True,
        name="Worker-OUT",
    )
    in_thread = threading.Thread(
        target=camera_worker,
        args=("IN", IN_VIDEO_DEVICE, IN_AUDIO_DEVICE, IN_VIRTUAL_VIDEO_DEVICE),
        daemon=True,
        name="Worker-IN",
    )

    clock_thread.start()
    db_thread.start()
    out_thread.start()
    in_thread.start()

    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()