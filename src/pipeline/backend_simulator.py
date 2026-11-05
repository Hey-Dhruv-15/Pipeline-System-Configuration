from __future__ import annotations

import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path

from src.pipeline.config import CONFIG


FIRST_NAMES = [
    "Aarav",
    "Diya",
    "Rohan",
    "Meera",
    "Kabir",
    "Anaya",
    "Ishaan",
    "Sara",
    "Vihaan",
    "Nitya",
]

LAST_NAMES = [
    "Sharma",
    "Patel",
    "Khan",
    "Reddy",
    "Das",
    "Menon",
    "Iyer",
    "Singh",
    "Verma",
    "Gupta",
]

UNITS = ["ICU", "ER", "WARD-A", "WARD-B", "LAB", "RAD"]
STATUSES = ["stable", "critical", "observation", "discharged", "awaiting_review"]
TESTS = ["cbc", "lactate", "glucose", "troponin", "creatinine", "electrolytes"]
EXTENSIONS = ("csv", "json", "log", "txt")


def ensure_raw_dir() -> None:
    CONFIG.raw_dir.mkdir(parents=True, exist_ok=True)


def patient_name() -> str:
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"


def patient_record(sequence: int) -> dict[str, str | int]:
    now = datetime.now(timezone.utc)
    patient_id = 500 + sequence
    return {
        "patient_id": patient_id,
        "name": patient_name(),
        "age": random.randint(1, 92),
        "unit": random.choice(UNITS),
        "status": random.choice(STATUSES),
        "test": random.choice(TESTS),
        "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def build_csv_payload(record: dict[str, str | int]) -> str:
    return (
        f"patient update id={record['patient_id']}, name={record['name']}, age={record['age']}, "
        f"unit={record['unit']}, status={record['status']}, captured_at={record['timestamp']}\n"
    )


def build_json_payload(record: dict[str, str | int]) -> str:
    payload = {
        "event_type": "device_stream",
        "patient": {
            "id": str(record["patient_id"]),
            "name": record["name"],
            "age": record["age"],
        },
        "device": {
            "unit": record["unit"],
            "status": record["status"],
        },
        "recorded_at": record["timestamp"],
    }
    return json.dumps(payload, ensure_ascii=True)


def build_log_payload(record: dict[str, str | int]) -> str:
    return (
        f"[{record['timestamp']}] backend=icu-monitor patient={record['patient_id']} "
        f"name=\"{record['name']}\" age={record['age']} unit={record['unit']} "
        f"status={record['status']} action=streamed\n"
    )


def build_txt_payload(record: dict[str, str | int]) -> str:
    return (
        f"Hospital intake note :: patient {record['patient_id']} named {record['name']} age {record['age']} "
        f"entered {record['unit']} and is marked {record['status']} for {record['test']} review at {record['timestamp']}.\n"
    )


def write_payload(target: Path, extension: str, record: dict[str, str | int]) -> None:
    builders = {
        "csv": build_csv_payload,
        "json": build_json_payload,
        "log": build_log_payload,
        "txt": build_txt_payload,
    }
    target.write_text(builders[extension](record), encoding="utf-8")


def next_file_name(extension: str, sequence: int) -> str:
    now = datetime.now(timezone.utc)
    stamp = now.strftime("%Y%m%d_%H%M%S")
    prefix = {
        "csv": "patient_updates",
        "json": "device_logs",
        "log": "monitoring_events",
        "txt": "lab_results",
    }[extension]
    return f"{prefix}_{stamp}_{sequence:04d}.{extension}"


def generate_batch(sequence: int) -> int:
    generated = 0
    for offset in range(CONFIG.backend_batch_size):
        current_sequence = sequence + offset
        extension = random.choice(EXTENSIONS)
        record = patient_record(current_sequence)
        file_name = next_file_name(extension, current_sequence)
        target = CONFIG.raw_dir / file_name
        write_payload(target, extension, record)
        print(
            f"Generated backend file: {file_name} | patient_id={record['patient_id']} | "
            f"unit={record['unit']} | status={record['status']}",
            flush=True,
        )
        generated += 1
    return generated


def main() -> None:
    ensure_raw_dir()
    print(
        f"Backend simulator started. Writing {CONFIG.backend_batch_size} files every "
        f"{CONFIG.backend_generation_interval_seconds}s into {CONFIG.raw_dir}",
        flush=True,
    )
    sequence = 1
    while True:
        generated = generate_batch(sequence)
        sequence += generated
        time.sleep(CONFIG.backend_generation_interval_seconds)


if __name__ == "__main__":
    main()
