from __future__ import annotations

import argparse
import random
import string
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class Patient:
    patient_id: int
    name: str
    age: int
    unit: str
    status: str


UNITS = ["ER", "ICU", "RAD", "OPD", "WARD-3", "LAB"]
STATUSES = ["admitted", "discharged", "observation", "transfer", "unknown"]
FIRST = ["Anaya", "Diya", "Ishaan", "Rohan", "Vihaan", "Aarav", "Meera", "Kabir", "Nisha", "Arjun"]
LAST = ["Gupta", "Sharma", "Patel", "Menon", "Iyer", "Singh", "Khan", "Das", "Reddy", "Jain"]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def rand_run_id() -> str:
    return "run_" + "".join(random.choices("0123456789abcdef", k=10))


def rand_patient() -> Patient:
    patient_id = random.randint(100, 999)
    name = f"{random.choice(FIRST)} {random.choice(LAST)}"
    age = random.randint(0, 95)
    unit = random.choice(UNITS)
    status = random.choice(STATUSES)
    return Patient(patient_id=patient_id, name=name, age=age, unit=unit, status=status)


def maybe_corrupt(value: str) -> str:
    roll = random.random()
    if roll < 0.08:
        return ""  # missing
    if roll < 0.12:
        return value + random.choice(["??", "###", "!", " "])
    if roll < 0.16:
        return value.replace("a", "@").replace("e", "3")
    return value


def random_noise(n: int = 18) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(random.choices(alphabet, k=n))


def as_unstructured_log(patient: Patient, *, kind: str) -> str:
    captured_at = now_iso()
    run_id = rand_run_id()

    pid = maybe_corrupt(str(patient.patient_id))
    name = maybe_corrupt(patient.name)
    age = maybe_corrupt(str(patient.age))
    unit = maybe_corrupt(patient.unit)
    status = maybe_corrupt(patient.status)

    if kind == "kv":
        return (
            f"patient update id={pid}, name={name}, age={age}, unit={unit}, status={status}, "
            f"captured_at={captured_at}, run_id={run_id}, noise={random_noise()}\n"
        )

    if kind == "free_text":
        return (
            "=== PATIENT NOTE ===\n"
            f"Captured: {captured_at}\n"
            f"Pt #{pid} ({name}) age:{age}\n"
            f"Location: unit {unit} | status -> {status}\n"
            f"triage: {random.choice(['green', 'yellow', 'red', 'unknown'])}\n"
            f"comments: {random.choice(['n/a', 'mild fever', 'missing vitals', 'duplicate record suspected'])}\n"
            f"raw_payload: {random_noise(32)} {random_noise(24)}\n"
        )

    if kind == "messy_csv":
        # Intentionally not a proper CSV table: mixed separators, partial quoting, shuffled keys.
        parts = [
            f"unit={unit}",
            f"patient_id:{pid}",
            f"name='{name}",
            f"age={age}",
            f"status {status}",
            f"captured_at {captured_at}",
            f"extra={random_noise()}",
        ]
        random.shuffle(parts)
        return ", ".join(parts) + "\n"

    raise ValueError(f"Unknown kind: {kind}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate unstructured patient raw files into data/raw.")
    parser.add_argument("--out", default="data/raw", help="Output directory (default: data/raw)")
    parser.add_argument("--count", type=int, default=10, help="Number of files to generate (default: 10)")
    parser.add_argument("--sleep-ms", type=int, default=0, help="Sleep between files (default: 0)")
    args = parser.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    kinds = [("kv", ".csv"), ("free_text", ".log"), ("messy_csv", ".txt")]

    for i in range(max(args.count, 0)):
        patient = rand_patient()
        kind, ext = random.choice(kinds)
        payload = as_unstructured_log(patient, kind=kind)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        path = out_dir / f"unstructured_patient_{ts}_{i:04d}{ext}"
        path.write_text(payload, encoding="utf-8")
        if args.sleep_ms > 0:
            time.sleep(args.sleep_ms / 1000.0)

    print(f"Generated {args.count} unstructured files in {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
