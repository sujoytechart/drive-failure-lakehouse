"""Create bounded, reproducible samples from extracted Backblaze CSV releases."""

import argparse
import csv
import hashlib
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any, Final

RELEASE_PATTERN: Final = re.compile(r"^\d{4}-Q[1-4]-r[1-9]\d*$")


def sample_release(
    input_dir: Path,
    output_dir: Path,
    release: str,
    rows_per_file: int,
) -> Path:
    """Sample the first N rows per sorted CSV and return its manifest path.

    The original header and source file name are preserved. The manifest records the
    complete source file hash and row count so the bounded sample can be traced back
    to the exact extracted input.
    """
    if RELEASE_PATTERN.fullmatch(release) is None:
        raise ValueError("release must match YYYY-QN-rN")
    if rows_per_file < 1:
        raise ValueError("rows_per_file must be positive")

    source_files = sorted(
        input_dir.rglob("*.csv"),
        key=lambda path: path.relative_to(input_dir).as_posix(),
    )
    if not source_files:
        raise ValueError(f"No CSV files found in {input_dir}")
    duplicate_names = sorted(
        name for name, count in Counter(path.name for path in source_files).items() if count > 1
    )
    if duplicate_names:
        raise ValueError(f"Duplicate CSV file names: {', '.join(duplicate_names)}")

    release_directory = output_dir / f"release={release}"
    release_directory.mkdir(parents=True, exist_ok=True)
    manifest_files: list[dict[str, Any]] = []

    for source_path in source_files:
        sampled_path = release_directory / source_path.name
        source_row_count, sampled_row_count = _sample_csv(
            source_path,
            sampled_path,
            rows_per_file,
        )
        manifest_files.append(
            {
                "source_file": source_path.name,
                "sha256": _sha256(source_path),
                "source_row_count": source_row_count,
                "sampled_row_count": sampled_row_count,
                "release": release,
            }
        )

    manifest_path = release_directory / "manifest.json"
    manifest_path.write_text(
        json.dumps({"release": release, "files": manifest_files}, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest_path


def _sample_csv(source: Path, target: Path, row_limit: int) -> tuple[int, int]:
    source_row_count = 0
    sampled_row_count = 0
    with (
        source.open(newline="", encoding="utf-8-sig") as source_handle,
        target.open("w", newline="", encoding="utf-8") as target_handle,
    ):
        reader = csv.reader(source_handle)
        writer = csv.writer(target_handle)
        header = next(reader, None)
        if header is None:
            raise ValueError(f"CSV file has no header: {source}")
        writer.writerow(header)

        for row in reader:
            source_row_count += 1
            if sampled_row_count < row_limit:
                writer.writerow(row)
                sampled_row_count += 1
    return source_row_count, sampled_row_count


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    """Parse command-line arguments and create a deterministic release sample."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--release", required=True)
    parser.add_argument("--rows-per-file", type=int, default=500)
    arguments = parser.parse_args()

    manifest_path = sample_release(
        arguments.input_dir,
        arguments.output_dir,
        arguments.release,
        arguments.rows_per_file,
    )
    print(manifest_path)
