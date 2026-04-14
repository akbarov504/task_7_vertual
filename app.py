import os
import sys
import time
import signal
import threading
import subprocess
from datetime import datetime, timedelta
from multiprocessing import shared_memory

from config import LOCAL_PATH, VIDEO_SEGMENT_LEN
from db import init_db, insert_video, video_exists


OUT_VIDEO_DEVICE = "/dev/v4l/by-path/platform-xhci-hcd.0.auto-usb-0:1.3:1.0-video-index0"
OUT_AUDIO_DEVICE = "hw:Camera_1,0"

IN_VIDEO_DEVICE = "/dev/v4l/by-path/platform-xhci-hcd.10.auto-usb-0:1:1.0-video-index0"
IN_AUDIO_DEVICE = "hw:Camera,0"

OUTPUT_DIR = LOCAL_PATH
SEGMENT_TIME = VIDEO_SEGMENT_LEN

# Capture / record
WIDTH = 1920
HEIGHT = 1080
FPS = 20

# Shared memory frame size
SHM_WIDTH = 1280
SHM_HEIGHT = 720
SHM_CHANNELS = 3
FRAME_SIZE = SHM_WIDTH * SHM_HEIGHT * SHM_CHANNELS

RECONNECT_DELAY = 3
DB_SCAN_INTERVAL = 2
FILE_STABLE_SECONDS = 2

GLOBAL_VIDEO_PREFIX = "ADAS-VID"

os.makedirs(OUTPUT_DIR, exist_ok=True)

stop_event = threading.Event()
processes = {}
process_lock = threading.Lock()


class SharedFrameWriter:
    def __init__(self, shm_name, width, height, channels):
        self.shm_name = shm_name
        self.width = width
        self.height = height
        self.channels = channels
        self.frame_size = width * height * channels
        self.total_size = 16 + self.frame_size

        try:
            old = shared_memory.SharedMemory(name=shm_name)
            old.close()
            old.unlink()
        except FileNotFoundError:
            pass
        except Exception:
            pass

        self.shm = shared_memory.SharedMemory(create=True, size=self.total_size, name=shm_name)
        self.buf = self.shm.buf

    def write_bytes(self, frame_bytes: bytes):
        if len(frame_bytes) != self.frame_size:
            return

        # 0 = writing, 1 = ready
        self.buf[0] = 0
        ts = int(time.time() * 1000)
        self.buf[8:16] = ts.to_bytes(8, byteorder="little", signed=False)
        self.buf[16:16 + self.frame_size] = frame_bytes
        self.buf[0] = 1

    def close(self):
        try:
            self.shm.close()
        except Exception:
            pass

    def unlink(self):
        try:
            self.shm.unlink()
        except Exception:
            pass


def wait_until_next_segment_boundary():
    now = time.time()
    wait_seconds = SEGMENT_TIME - (int(now) % SEGMENT_TIME)
    if wait_seconds == SEGMENT_TIME:
        wait_seconds = 0
    if wait_seconds > 0:
        print(f"[INFO] Keyingi {SEGMENT_TIME}s boundary kutilmoqda: {wait_seconds} sec")
        time.sleep(wait_seconds)


def build_ffmpeg_command(video_device, audio_device, prefix):
    timestamp_pattern = os.path.join(
        OUTPUT_DIR,
        f"{prefix}_%Y-%m-%d_%H-%M-%S.mp4"
    )

    cmd = [
        "ffmpeg",
        "-nostdin",
        "-hide_banner",
        "-loglevel", "warning",

        # Input
        "-thread_queue_size", "256",
        "-f", "v4l2",
        "-input_format", "mjpeg",
        "-framerate", str(FPS),
        "-video_size", f"{WIDTH}x{HEIGHT}",
        "-i", video_device,

        "-thread_queue_size", "1024",
        "-f", "alsa",
        "-channels", "2",
        "-sample_rate", "48000",
        "-i", audio_device,

        # Output 1: recording
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
        "-b:a", "96k",
        "-ar", "44100",
        "-af", "aresample=async=1000:min_hard_comp=0.100:first_pts=0",
        "-f", "segment",
        "-segment_time", str(SEGMENT_TIME),
        "-segment_atclocktime", "1",
        "-segment_format", "mp4",
        "-reset_timestamps", "1",
        "-strftime", "1",
        timestamp_pattern,

        # Output 2: rawvideo -> stdout pipe
        "-map", "0:v:0",
        "-an",
        "-vf", f"fps=15,scale={SHM_WIDTH}:{SHM_HEIGHT}:flags=fast_bilinear,format=bgr24",
        "-pix_fmt", "bgr24",
        "-f", "rawvideo",
        "pipe:1",
    ]
    return cmd


def check_video_device_exists(device_path):
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
            files = sorted(f for f in os.listdir(OUTPUT_DIR) if f.lower().endswith(".mp4"))
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
                print(f"[DB] Video saqlandi: {camera_type} | {file_path} | {global_video_id}")
        except Exception as e:
            print(f"[DB WATCHER ERROR] {e}")

        time.sleep(DB_SCAN_INTERVAL)


def ffmpeg_camera_worker(name, video_device, audio_device, shm_name):
    shm_writer = SharedFrameWriter(shm_name, SHM_WIDTH, SHM_HEIGHT, SHM_CHANNELS)
    proc = None

    try:
        while not stop_event.is_set():
            if not check_video_device_exists(video_device):
                print(f"[WARN] {name}: video device yo'q -> {video_device}")
                time.sleep(RECONNECT_DELAY)
                continue

            cmd = build_ffmpeg_command(video_device, audio_device, name)

            print(f"[INFO] {name}: ffmpeg ishga tushiriladi")
            print(f"[INFO] {name}: VIDEO={video_device}")
            print(f"[INFO] {name}: AUDIO={audio_device}")
            print(f"[INFO] {name}: SHM={shm_name}")

            wait_until_next_segment_boundary()

            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=10**8
            )

            with process_lock:
                processes[name] = proc

            stderr_thread = threading.Thread(
                target=consume_stderr,
                args=(proc, name),
                daemon=True
            )
            stderr_thread.start()

            while not stop_event.is_set():
                chunk = read_exact(proc.stdout, FRAME_SIZE)
                if chunk is None:
                    print(f"[WARN] {name}: rawvideo pipe uzildi, restart bo'ladi")
                    break

                shm_writer.write_bytes(chunk)

                if proc.poll() is not None:
                    print(f"[WARN] {name}: ffmpeg to'xtab qoldi (code={proc.returncode})")
                    break

            terminate_process(proc, name)

            with process_lock:
                processes[name] = None

            if not stop_event.is_set():
                time.sleep(RECONNECT_DELAY)

    finally:
        if proc:
            terminate_process(proc, name)
        shm_writer.close()
        shm_writer.unlink()


def read_exact(pipe, size):
    data = bytearray()
    while len(data) < size and not stop_event.is_set():
        chunk = pipe.read(size - len(data))
        if not chunk:
            return None
        data.extend(chunk)
    return bytes(data)


def consume_stderr(proc, name):
    try:
        while not stop_event.is_set():
            line = proc.stderr.readline()
            if not line:
                break
            text = line.decode(errors="ignore").strip()
            if text:
                print(f"[FFMPEG {name}] {text}")
    except Exception:
        pass


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

    print("[INFO] FFmpeg + Shared Memory system boshlandi")
    print(f"[INFO] Papka: {OUTPUT_DIR}")
    print(f"[INFO] Segment: {SEGMENT_TIME} sekund")
    print(f"[INFO] SHM frame size: {SHM_WIDTH}x{SHM_HEIGHT}")

    db_thread = threading.Thread(target=scan_and_insert_segments, daemon=True)
    out_thread = threading.Thread(
        target=ffmpeg_camera_worker,
        args=("OUT", OUT_VIDEO_DEVICE, OUT_AUDIO_DEVICE, "out_camera_shm"),
        daemon=True
    )
    in_thread = threading.Thread(
        target=ffmpeg_camera_worker,
        args=("IN", IN_VIDEO_DEVICE, IN_AUDIO_DEVICE, "in_camera_shm"),
        daemon=True
    )

    db_thread.start()
    out_thread.start()
    in_thread.start()

    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()