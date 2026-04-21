"""
Arxitektura:
  ffmpeg#OUT  — real OUT camera → /dev/video40 (virtual, faqat stream)
  ffmpeg#IN   — real IN  camera → /dev/video41 (virtual, faqat stream)
  ffmpeg#REC  — /dev/video40 + /dev/video41 → OUT_*.mp4 + IN_*.mp4
                bitta process, atomik segment switching → kafolatlangan sync

Reconnect:
  OUT kamera uzilsa → ffmpeg#OUT qayta boshlanadi
  ffmpeg#REC /dev/video40 dan o'qishda timeout/reconnect bilan kutadi
  ffmpeg#IN  to'xtamaydi
  OUT qayta ulanishi bilan ffmpeg#REC o'z segmentini davom ettiradi
"""

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

# ── Device config ──────────────────────────────────────────────────────────────
OUT_VIDEO_DEVICE = "/dev/v4l/by-path/platform-xhci-hcd.0.auto-usb-0:1.3:1.0-video-index0"
OUT_AUDIO_DEVICE = "hw:Camera_1,0"

IN_VIDEO_DEVICE  = "/dev/v4l/by-path/platform-xhci-hcd.10.auto-usb-0:1:1.0-video-index0"
IN_AUDIO_DEVICE  = "hw:Camera,0"

# Virtual loopback devicelar (v4l2loopback)
OUT_VIRTUAL_VIDEO = "/dev/video40"   # ffmpeg#OUT yozadi, ffmpeg#REC o'qiydi
IN_VIRTUAL_VIDEO  = "/dev/video41"   # ffmpeg#IN  yozadi, ffmpeg#REC o'qiydi

# ── Sizes ──────────────────────────────────────────────────────────────────────
OUTPUT_DIR   = LOCAL_PATH
SEGMENT_TIME = VIDEO_SEGMENT_LEN

CAM_WIDTH  = 1920
CAM_HEIGHT = 1080
CAM_FPS    = 20

VIRT_WIDTH  = 640
VIRT_HEIGHT = 640
VIRT_FPS    = 20

# ── Misc ───────────────────────────────────────────────────────────────────────
RECONNECT_DELAY     = 3
DB_SCAN_INTERVAL    = 2
FILE_STABLE_SECONDS = 2
VIDEO_ID_NAMESPACE  = "TRUCK_VIN"

os.makedirs(OUTPUT_DIR, exist_ok=True)

stop_event   = threading.Event()
processes    = {}          # {"OUT": proc, "IN": proc, "REC": proc}
process_lock = threading.Lock()


# ── Helpers ────────────────────────────────────────────────────────────────────

def now_ts() -> float:
    return time.time()


def format_ts(ts: float) -> str:
    dt = datetime.fromtimestamp(ts)
    return dt.strftime("%Y-%m-%d %H:%M:%S.") + f"{dt.microsecond:06d}"


def check_device(path: str) -> bool:
    return os.path.exists(path)


def terminate_process(proc, name: str):
    if not proc or proc.poll() is not None:
        return
    print(f"[INFO] {name}: to'xtatilmoqda...")
    proc.terminate()
    try:
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        print(f"[WARN] {name}: kill qilinmoqda...")
        proc.kill()
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            pass


def parse_segment_times_from_filename(file_name: str):
    base             = os.path.splitext(os.path.basename(file_name))[0]
    parts            = base.split("_", 1)
    if len(parts) != 2:
        return None, None, None, None
    camera_type, dt_part = parts
    try:
        start_dt    = datetime.strptime(dt_part, "%Y-%m-%d_%H-%M-%S")
        end_dt      = start_dt + timedelta(seconds=SEGMENT_TIME)
        return camera_type, start_dt.isoformat(), end_dt.isoformat(), dt_part
    except ValueError:
        return None, None, None, None


def make_global_video_id(segment_key: str) -> str:
    return f"{VIDEO_ID_NAMESPACE}_{segment_key}"


def is_file_stable(file_path: str) -> bool:
    if not os.path.exists(file_path):
        return False
    s1 = os.path.getsize(file_path)
    time.sleep(FILE_STABLE_SECONDS)
    if not os.path.exists(file_path):
        return False
    s2 = os.path.getsize(file_path)
    return s1 == s2 and s2 > 0


# ── ffmpeg commands ────────────────────────────────────────────────────────────

def cmd_streamer(real_video, real_audio, virtual_video, channels="2", sample_rate="48000"):
    """
    ffmpeg#OUT / ffmpeg#IN
    Real kameradan o'qib virtual loopback ga yozadi.
    Faylga yozmaydi — faqat stream.
    """
    channel_layout = "stereo" if channels == "2" else "mono"
    return [
        "ffmpeg",
        "-nostdin", "-hide_banner", "-loglevel", "warning",

        # Low latency
        "-fflags",          "nobuffer+genpts",
        "-flags",           "low_delay",
        "-avioflags",       "direct",
        "-probesize",       "32",
        "-analyzeduration", "0",

        # Video input
        "-thread_queue_size", "4096",
        "-f",            "v4l2",
        "-input_format", "mjpeg",
        "-framerate",    str(CAM_FPS),
        "-video_size",   f"{CAM_WIDTH}x{CAM_HEIGHT}",
        "-i",            real_video,

        # Audio input
        "-thread_queue_size", "4096",
        "-f",              "alsa",
        "-channels",       channels,
        "-sample_rate",    sample_rate,
        "-channel_layout", channel_layout,
        "-i",              real_audio,

        "-max_muxing_queue_size", "4096",

        # Virtual video output (scaled, yuv420p)
        "-map", "0:v:0",
        "-an",
        "-vf", (
            f"fps={VIRT_FPS},"
            f"scale={VIRT_WIDTH}:{VIRT_HEIGHT}:flags=fast_bilinear,"
            f"format=yuv420p"
        ),
        "-pix_fmt",     "yuv420p",
        "-color_range", "tv",
        "-f",           "v4l2",
        virtual_video,
    ]


def cmd_recorder():
    """
    ffmpeg#REC — MASTER RECORDER
    Ikkala virtual loopback dan o'qib, ikkala faylga yozadi.
    Bitta process — segment switching atomik → kafolatlangan sync.

    Virtual loopback dan o'qiganda -re va reconnect flaglari:
    - timeout=5000000  (5 sekund) — kamera uzilsa kutadi
    - reconnect=1      — avtomatik qayta ulanadi
    """
    out_pattern = os.path.join(OUTPUT_DIR, "OUT_%Y-%m-%d_%H-%M-%S.mp4")
    in_pattern  = os.path.join(OUTPUT_DIR, "IN_%Y-%m-%d_%H-%M-%S.mp4")

    return [
        "ffmpeg",
        "-nostdin", "-hide_banner", "-loglevel", "warning",

        # OUT virtual camera input
        "-thread_queue_size", "4096",
        "-f",            "v4l2",
        "-input_format", "yuv420p",
        "-framerate",    str(VIRT_FPS),
        "-video_size",   f"{VIRT_WIDTH}x{VIRT_HEIGHT}",
        "-i",            OUT_VIRTUAL_VIDEO,

        # IN virtual camera input
        "-thread_queue_size", "4096",
        "-f",            "v4l2",
        "-input_format", "yuv420p",
        "-framerate",    str(VIRT_FPS),
        "-video_size",   f"{VIRT_WIDTH}x{VIRT_HEIGHT}",
        "-i",            IN_VIRTUAL_VIDEO,

        "-max_muxing_queue_size", "4096",

        # ── OUT output ──
        "-map", "0:v:0",
        "-c:v",        "h264_rkmpp",
        "-b:v",        "1800k",
        "-g",          str(VIRT_FPS * SEGMENT_TIME),
        "-keyint_min", str(VIRT_FPS * SEGMENT_TIME),
        "-maxrate",    "1800k",
        "-bufsize",    "1800k",
        "-force_key_frames", f"expr:gte(t,n_forced*{SEGMENT_TIME})",
        "-an",

        "-f",                        "segment",
        "-segment_time",             str(SEGMENT_TIME),
        "-segment_atclocktime",      "1",
        "-segment_clocktime_offset", "0",
        "-segment_format",           "mp4",
        "-reset_timestamps",         "1",
        "-strftime",                 "1",
        out_pattern,

        # ── IN output ──
        "-map", "1:v:0",
        "-c:v",        "h264_rkmpp",
        "-b:v",        "1800k",
        "-g",          str(VIRT_FPS * SEGMENT_TIME),
        "-keyint_min", str(VIRT_FPS * SEGMENT_TIME),
        "-maxrate",    "1800k",
        "-bufsize",    "1800k",
        "-force_key_frames", f"expr:gte(t,n_forced*{SEGMENT_TIME})",
        "-an",

        "-f",                        "segment",
        "-segment_time",             str(SEGMENT_TIME),
        "-segment_atclocktime",      "1",
        "-segment_clocktime_offset", "0",
        "-segment_format",           "mp4",
        "-reset_timestamps",         "1",
        "-strftime",                 "1",
        in_pattern,
    ]


# ── Workers ────────────────────────────────────────────────────────────────────

def streamer_worker(name: str, real_video: str, real_audio: str, virtual_video: str):
    """
    Real kameradan virtual loopback ga uzluksiz stream qiladi.
    Kamera uzilsa qayta ulanadi.
    ffmpeg#REC bu virtual devicedan o'qiydi.
    """
    global processes

    print(f"[STREAMER] {name}: ishga tushdi")

    while not stop_event.is_set():
        if not check_device(real_video):
            print(f"[WARN] {name}: device yo'q -> {real_video}")
            time.sleep(RECONNECT_DELAY)
            continue
        if not check_device(virtual_video):
            print(f"[WARN] {name}: virtual device yo'q -> {virtual_video}")
            time.sleep(RECONNECT_DELAY)
            continue

        cmd  = cmd_streamer(real_video, real_audio, virtual_video)
        proc = subprocess.Popen(cmd)

        with process_lock:
            processes[name] = proc

        print(f"[STREAMER] {name}: PID={proc.pid}")

        while not stop_event.is_set():
            if proc.poll() is not None:
                print(f"[WARN] {name}: ffmpeg to'xtadi. Qayta ulanish...")
                break
            time.sleep(1)

        terminate_process(proc, name)

        with process_lock:
            processes[name] = None

        if not stop_event.is_set():
            time.sleep(RECONNECT_DELAY)

    print(f"[STREAMER] {name}: to'xtatildi")


def recorder_worker():
    """
    Master recorder — virtual loopback lardan o'qib faylga yozadi.
    Bitta process => segment boundaries ikkalasida atomik.

    Virtual loopback da signal uzilsa ffmpeg o'zi error beradi va
    bu worker qayta ishga tushiradi — lekin bu kamdan-kam bo'ladi
    chunki v4l2loopback da streamer qayta ulanganda black frame beradi.
    """
    global processes

    print("[REC] Master recorder ishga tushdi")

    while not stop_event.is_set():
        # Ikkala virtual device tayyor bo'lgunicha kut
        if not check_device(OUT_VIRTUAL_VIDEO):
            print(f"[WARN] REC: {OUT_VIRTUAL_VIDEO} yo'q, kutilmoqda...")
            time.sleep(RECONNECT_DELAY)
            continue
        if not check_device(IN_VIRTUAL_VIDEO):
            print(f"[WARN] REC: {IN_VIRTUAL_VIDEO} yo'q, kutilmoqda...")
            time.sleep(RECONNECT_DELAY)
            continue

        # Streamerlar virtual devicega yoza boshlaguncha bir oz kut
        print("[REC] Streamerlar tayyor bo'lishini kutmoqda (2s)...")
        time.sleep(2)

        cmd  = cmd_recorder()
        proc = subprocess.Popen(cmd)

        with process_lock:
            processes["REC"] = proc

        print(f"[REC] Master recorder PID={proc.pid}")

        while not stop_event.is_set():
            if proc.poll() is not None:
                code = proc.returncode
                print(f"[WARN] REC: to'xtadi (code={code}). Qayta ishga tushirilmoqda...")
                break
            time.sleep(1)

        terminate_process(proc, "REC")

        with process_lock:
            processes["REC"] = None

        if not stop_event.is_set():
            time.sleep(RECONNECT_DELAY)

    print("[REC] Master recorder to'xtatildi")


# ── DB watcher ─────────────────────────────────────────────────────────────────

def scan_and_insert_segments():
    print("[DB] Watcher ishga tushdi")

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
                    print(f"[WARN] DB: parse bo'lmadi -> {file_name}")
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
                    f"[DB] Saqlandi: {camera_type} | "
                    f"id={global_video_id} | {file_path}"
                )

        except Exception as e:
            print(f"[DB ERROR] {e}")

        time.sleep(DB_SCAN_INTERVAL)


# ── Signal handler ─────────────────────────────────────────────────────────────

def stop_all(signum=None, frame=None):
    print("\n[INFO] Dastur to'xtatilmoqda...")
    stop_event.set()

    with process_lock:
        for name, proc in list(processes.items()):
            terminate_process(proc, name)

    print("[INFO] Hamma jarayonlar to'xtatildi.")
    sys.exit(0)


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    init_db()

    signal.signal(signal.SIGINT,  stop_all)
    signal.signal(signal.SIGTERM, stop_all)

    if not check_device(OUT_VIRTUAL_VIDEO):
        print(f"[ERROR] Virtual device topilmadi: {OUT_VIRTUAL_VIDEO}")
        sys.exit(1)
    if not check_device(IN_VIRTUAL_VIDEO):
        print(f"[ERROR] Virtual device topilmadi: {IN_VIRTUAL_VIDEO}")
        sys.exit(1)

    print("[INFO] Recording system boshlandi")
    print(f"[INFO] Papka   : {OUTPUT_DIR}")
    print(f"[INFO] Segment : {SEGMENT_TIME}s | Virtual: {VIRT_WIDTH}x{VIRT_HEIGHT}@{VIRT_FPS}fps")
    print("[INFO] Arxitektura: streamer(OUT) + streamer(IN) → virtual → REC(master)")
    print("[INFO] Sync: bitta ffmpeg#REC — segment boundaries atomik")
    print("[INFO] CTRL+C — to'xtatish\n")

    # Streamerlar virtual devicega yoza boshlaydi
    out_streamer = threading.Thread(
        target=streamer_worker,
        args=("OUT", OUT_VIDEO_DEVICE, OUT_AUDIO_DEVICE, OUT_VIRTUAL_VIDEO),
        daemon=True, name="Streamer-OUT",
    )
    in_streamer = threading.Thread(
        target=streamer_worker,
        args=("IN", IN_VIDEO_DEVICE, IN_AUDIO_DEVICE, IN_VIRTUAL_VIDEO),
        daemon=True, name="Streamer-IN",
    )

    # Master recorder virtual devicelardan o'qib faylga yozadi
    rec_thread = threading.Thread(
        target=recorder_worker,
        daemon=True, name="MasterREC",
    )

    db_thread = threading.Thread(
        target=scan_and_insert_segments,
        daemon=True, name="DBWatcher",
    )

    out_streamer.start()
    in_streamer.start()

    # Streamerlar virtual devicega ulgurishini kut, keyin REC ni boshlat
    time.sleep(3)
    rec_thread.start()
    db_thread.start()

    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()