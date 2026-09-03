from drive_failure_lakehouse import __version__


def test_package_exposes_version() -> None:
    """The installed package should expose its public release version."""
    assert __version__ == "0.1.0"
