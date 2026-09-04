import csv
import hashlib
import json
from pathlib import Path

from drive_failure_lakehouse.sample import sample_release


def write_csv(path: Path, rows: list[list[str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        csv.writer(handle).writerows(rows)


def test_samples_files_deterministically_and_writes_manifest(tmp_path: Path) -> None:
    source = tmp_path / "source"
    output = tmp_path / "sample"
    source.mkdir()
    header = ["date", "serial_number", "failure"]

    write_csv(
        source / "b.csv",
        [header, ["2024-01-02", "B-1", "0"], ["2024-01-02", "B-2", "1"]],
    )
    write_csv(
        source / "a.csv",
        [
            header,
            ["2024-01-01", "A-1", "0"],
            ["2024-01-01", "A-2", "0"],
            ["2024-01-01", "A-3", "1"],
        ],
    )

    manifest_path = sample_release(source, output, "2024-Q1-r1", rows_per_file=2)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    release_directory = output / "release=2024-Q1-r1"

    assert manifest_path == release_directory / "manifest.json"
    assert manifest["release"] == "2024-Q1-r1"
    assert [item["source_file"] for item in manifest["files"]] == ["a.csv", "b.csv"]
    assert manifest["files"][0] == {
        "source_file": "a.csv",
        "sha256": hashlib.sha256((source / "a.csv").read_bytes()).hexdigest(),
        "source_row_count": 3,
        "sampled_row_count": 2,
        "release": "2024-Q1-r1",
    }

    with (release_directory / "a.csv").open(newline="", encoding="utf-8") as handle:
        sampled_rows = list(csv.reader(handle))
    assert sampled_rows == [
        header,
        ["2024-01-01", "A-1", "0"],
        ["2024-01-01", "A-2", "0"],
    ]
