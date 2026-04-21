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

# Sync tuning
PREPARE_AHEAD_SECONDS  = 2.0    # keyingi boundary dan oldin tayyor turish
COARSE_SLEEP_THRESHOLD = 0.050  # 50 ms dan katta bo'lsa sleep
FINE_SLEEP_THRESHOLD   = 0.005  # 5 ms dan katta bo'lsa short sleep
BARRIER_TIMEOUT        = 10     # sekund

os.makedirs(OUTPUT_DIR, exist_ok=True)

stop_event   = threading.Event()
processes    = {}
process_lock = threading.Lock()

# Sync state — ikkala camera_worker bir xil slot ishlatishi uchun
sync_state_lock  = threading.Lock()
sync_target_ts   = None   # hozirgi maqsad vaqt
sync_barrier_pre = None   # 1-chi: ikkalasi tayyor bo'lgach precise-wait boshlash
sync_barrier_go  = None   # 2-chi: ikkalasi precise-wait tugagach Popen qilish


def now_ts() -> float:
    return time.time()


def format_ts(ts: float) -> str:
    dt = datetime.fromtimestamp(ts)
    return dt.strftime("%Y-%m-%d %H:%M:%S.") + f"{dt.microsecond:06d}"


def get_next_aligned_time(segment_seconds: int, after_ts: float = None) -> float:
    """
    Keyingi segment boundary.
    10s uchun:
      10:29:26.1 -> 10:29:30
      10:29:30.0 -> 10:29:40
    """
    if after_ts is None:
        after_ts = now_ts()

    eps = 1e-9
    return math.floor((after_ts + segment_seconds - eps) / segment_seconds) * segment_seconds


def get_or_create_sync_slot():
    """
    Har ikkala worker bir xil target_ts, barrier_pre va barrier_go oladi.

    Yangi slot faqat hozirgi target o'tib ketganda yaratiladi —
    shunda bir worker reconnect qilganda ham har ikkalasi
    KEYINGI bir xil boundary ga tushadi.
    """
    global sync_target_ts, sync_barrier_pre, sync_barrier_go

    with sync_state_lock:
        current = now_ts()

        # Slot yo'q yoki o'tib ketgan bo'lsa — yangisini yaratamiz
        if sync_target_ts is None or current >= sync_target_ts - 0.001:
            sync_target_ts   = get_next_aligned_time(SEGMENT_TIME, current + PREPARE_AHEAD_SECONDS)
            sync_barrier_pre = threading.Barrier(2)   # precise-wait ga kirish eshigi
            sync_barrier_go  = threading.Barrier(2)   # Popen ga kirish eshigi
            print(f"[SYNC] Yangi sync slot -> {format_ts(sync_target_ts)}")

        return sync_target_ts, sync_barrier_pre, sync_barrier_go


def wait_until_precise(target_ts: float, name: str):
    """
    Target vaqtga imkon qadar aniq yetib borish:
    - uzoq bo'lsa sleep
    - yaqinlashsa short sleep
    - ohirida busy-wait
    """
    while not stop_event.is_set():
        remaining = target_ts - now_ts()

        if remaining <= 0:
            break

        if remaining > COARSE_SLEEP_THRESHOLD:
            time.sleep(min(remaining / 2.0, 0.020))
        elif remaining > FINE_SLEEP_THRESHOLD:
            time.sleep(0.001)
        else:
            # very fine busy wait
            while not stop_event.is_set() and now_ts() < target_ts:
                pass
            break

    actual = now_ts()
    diff_us = int((actual - target_ts) * 1_000_000)
    print(
        f"[SYNC] {name}: target={format_ts(target_ts)} "
        f"actual={format_ts(actual)} diff_us={diff_us}"
    )


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

        # Low latency flags
        "-fflags", "nobuffer+genpts",
        "-flags", "low_delay",
        "-avioflags", "direct",
        "-probesize", "32",
        "-analyzeduration", "0",

        # VIDEO INPUT
        "-thread_queue_size", "1024",
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

        "-max_muxing_queue_size", "1024",
    ]

    # RECORDING OUTPUT
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
        "-segment_format", "mp4",
        "-reset_timestamps", "1",
        "-strftime", "1",
        timestamp_pattern,
    ]

    # VIRTUAL CAMERA OUTPUT
    if virtual_video_device:
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
    """
    Format:
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
        return (
            camera_type,
            start_dt.isoformat(),
            end_dt.isoformat(),
            segment_key
        )
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


def synchronized_start(name, cmd):
    """
    Ikki bosqichli sync:
      1) barrier_pre  — ikkalasi ham tayyor bo'lganda precise-wait boshlanadi
      2) wait_until_precise — GIL tufayli biri oldin tugasa ham ketmaydi
      3) barrier_go   — IKKALASI precise-wait ni tugatgach BIRGALIKDA Popen qiladi

    Natijada Popen lar orasidagi farq faqat OS scheduling latency (~0-200 us).
    """
    target_ts, barrier_pre, barrier_go = get_or_create_sync_slot()
    print(f"[SYNC] {name}: slot={format_ts(target_ts)} | barrier_pre kutmoqda...")

    # --- 1-chi eshik: ikkalasi tayyor ---
    try:
        barrier_pre.wait(timeout=BARRIER_TIMEOUT)
    except threading.BrokenBarrierError:
        print(f"[WARN] {name}: barrier_pre buzildi, qayta uriniladi")
        return None

    # --- Precise wait ---
    wait_until_precise(target_ts, name)

    if stop_event.is_set():
        return None

    # --- 2-chi eshik: ikkalasi precise-wait ni tugatdi ---
    try:
        barrier_go.wait(timeout=1.0)   # 1s timeout — biri kech qolsa kutmasin
    except threading.BrokenBarrierError:
        print(f"[WARN] {name}: barrier_go buzildi, yolg'iz davom etadi")

    # --- Popen ---
    popen_before = now_ts()
    proc = subprocess.Popen(cmd)
    popen_after  = now_ts()

    print(
        f"[SYNC] {name}: popen={format_ts(popen_before)} "
        f"delta_us={int((popen_before - target_ts) * 1_000_000)}"
    )

    return proc


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

        cmd = build_ffmpeg_command(
            video_device=video_device,
            audio_device=audio_device,
            channels="2",
            sample_rate="48000",
            prefix=name,
            virtual_video_device=virtual_video_device
        )

        print(f"[INFO] {name}: ffmpeg tayyorlandi")
        print(f"[INFO] {name}: VIDEO={video_device}")
        print(f"[INFO] {name}: AUDIO={audio_device}")
        print(f"[INFO] {name}: VIRTUAL={virtual_video_device}")

        proc = synchronized_start(name, cmd)

        if proc is None:
            if not stop_event.is_set():
                time.sleep(0.2)
            continue

        with process_lock:
            processes[name] = proc

        print(f"[INFO] {name}: ffmpeg ishga tushdi PID={proc.pid}")

        while not stop_event.is_set():
            ret = proc.poll()
            if ret is not None:
                print(f"[WARN] {name}: ffmpeg to'xtab qoldi (code={ret}). Qayta ulanish...")
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
    init_db()

    signal.signal(signal.SIGINT, stop_all)
    signal.signal(signal.SIGTERM, stop_all)

    if not check_virtual_device_exists(OUT_VIRTUAL_VIDEO_DEVICE):
        print(f"[ERROR] Virtual device topilmadi: {OUT_VIRTUAL_VIDEO_DEVICE}")
        sys.exit(1)

    if not check_virtual_device_exists(IN_VIRTUAL_VIDEO_DEVICE):
        print(f"[ERROR] Virtual device topilmadi: {IN_VIRTUAL_VIDEO_DEVICE}")
        sys.exit(1)

    first_slot = get_next_aligned_time(SEGMENT_TIME, now_ts() + PREPARE_AHEAD_SECONDS)

    print("[INFO] Auto-reconnect recording system boshlandi")
    print(f"[INFO] Papka: {OUTPUT_DIR}")
    print(f"[INFO] Segment: {SEGMENT_TIME} sekund")
    print(f"[INFO] Virtual stream: {VIRTUAL_WIDTH}x{VIRTUAL_HEIGHT} @ {VIRTUAL_FPS} fps")
    print("[INFO] Kamera sug'urilsa, dastur kutadi va qayta tiqilganda avtomatik ishga tushadi")
    print("[INFO] Har yozilgan video DB ga saqlanadi")
    print("[INFO] OUT va IN bir vaqtdagi segmentlar bir xil globalVideoId oladi")
    print(f"[INFO] Birinchi sync slot: {format_ts(first_slot)}")
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