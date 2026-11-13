"""HTTP API tests.  Run with: pytest -q (requires requirements-dev.txt)."""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from unittest import mock

import pytest
from fastapi.testclient import TestClient

from app import db, service


def _write_manifest(path, snapshots):
    path.write_text(json.dumps({"snapshots": snapshots}), encoding="utf-8")


@pytest.fixture()
def api(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    manifest_path = tmp_path / "manifest.json"
    _write_manifest(
        manifest_path,
        [
            {"snapshot_id": "a", "files": ["a.parquet", "shared.parquet"]},
            {"snapshot_id": "b", "files": ["b.parquet", "shared.parquet"]},
        ],
    )
    for name in ("a.parquet", "b.parquet", "shared.parquet"):
        (data_dir / name).write_bytes(b"PAR1-PAR1")

    monkeypatch.setenv("MANIFEST_PATH", str(manifest_path))
    monkeypatch.setenv("DATA_DIR", str(data_dir))
    monkeypatch.setenv("CLEANUP_DB_PATH", str(tmp_path / "state.db"))
    monkeypatch.setenv("SEED_SAMPLE_DATA_ON_START", "0")
    db.reset_initialization_cache()

    from app.main import app

    with TestClient(app) as client:
        yield client, data_dir


def test_demo_before_and_after_export_completion(api):
    client, data_dir = api

    registration = client.post(
        "/exports/register", json={"snapshot_id": "a", "export_id": "export-1"}
    )
    assert registration.status_code == 201, registration.text
    registration_id = registration.json()["registration_id"]

    for snapshot_id in ("a", "b"):
        response = client.post(f"/snapshots/{snapshot_id}/retire")
        assert response.status_code == 200, response.text

    # Before the export completes: b.parquet is free, but a's files and the
    # shared file are still protected by the unfinished export.
    preview = client.get("/cleanup/preview")
    assert preview.status_code == 200
    body = preview.json()
    assert body["deletable"] == ["b.parquet"]
    assert body["protected_by_active_exports"] == [
        "a.parquet",
        "shared.parquet",
    ]

    cleanup = client.post("/cleanup")
    assert cleanup.status_code == 200, cleanup.text
    assert cleanup.json()["deleted"] == ["b.parquet"]
    assert cleanup.json()["failed"] == []
    assert (data_dir / "a.parquet").exists()
    assert (data_dir / "shared.parquet").exists()

    # Release (export finished) then clean up the rest.
    released = client.post(f"/exports/{registration_id}/release")
    assert released.status_code == 200
    assert released.json()["released"] is True
    assert released.json()["already_released"] is False

    preview = client.get("/cleanup/preview").json()
    assert preview["deletable"] == ["a.parquet", "shared.parquet"]
    assert preview["active_exports"] == []

    cleanup = client.post("/cleanup").json()
    assert cleanup["deleted"] == ["a.parquet", "shared.parquet"]
    assert cleanup["failed"] == []

    assert client.get("/cleanup/preview").json()["deletable"] == []


def test_retirement_rejects_new_registration(api):
    client, _ = api
    assert client.post("/snapshots/a/retire").status_code == 200
    response = client.post("/exports/register", json={"snapshot_id": "a"})
    assert response.status_code == 409
    assert "已退役" in response.json()["detail"]


def test_dry_run_does_not_delete(api):
    client, data_dir = api
    for snapshot_id in ("a", "b"):
        client.post(f"/snapshots/{snapshot_id}/retire")
    response = client.post("/cleanup?dry_run=true")
    assert response.status_code == 200
    body = response.json()
    assert body["dry_run"] is True
    assert body["deleted"] == []
    assert body["deletable"] == ["a.parquet", "shared.parquet", "b.parquet"]
    assert body["already_absent"] == []
    assert (data_dir / "a.parquet").exists()


def test_unknown_ids_and_idempotent_release(api):
    client, _ = api
    assert client.post("/snapshots/nope/retire").status_code == 404
    assert client.post("/exports/register", json={"snapshot_id": "nope"}).status_code == 404
    assert client.post("/exports/nope/release").status_code == 404

    registration_id = client.post(
        "/exports/register", json={"snapshot_id": "a"}
    ).json()["registration_id"]
    first = client.post(f"/exports/{registration_id}/release")
    second = client.post(f"/exports/{registration_id}/release")
    assert first.status_code == second.status_code == 200
    assert second.json()["already_released"] is True


def test_snapshot_listing(api):
    client, _ = api
    client.post("/snapshots/a/retire")
    client.post("/exports/register", json={"snapshot_id": "b"})

    snapshots = {
        item["snapshot_id"]: item
        for item in client.get("/snapshots").json()["snapshots"]
    }
    assert snapshots["a"]["retired"] is True
    assert snapshots["a"]["active_export_count"] == 0
    assert snapshots["b"]["retired"] is False
    assert snapshots["b"]["active_export_count"] == 1
    assert snapshots["b"]["files"] == ["b.parquet", "shared.parquet"]


def test_register_succeeds_and_export_is_protected_while_cleanup_is_stuck(api):
    """POST /cleanup deleting slowly must not block POST /exports/register.

    Snapshot `a` is retained while snapshot `b` is retired, so `b.parquet`
    is eligible for cleanup.  While cleanup is stuck in its unlink, a
    registration for the still-retained `a` must return its receipt at once
    and its files must indeed remain on disk.
    """

    client, data_dir = api
    assert client.post("/snapshots/b/retire").status_code == 200

    unlink_started = threading.Event()
    release_unlink = threading.Event()
    original_delete = service._delete_file

    def slow_delete(data_dir_arg: Path, relative_path: str) -> str:
        unlink_started.set()
        release_unlink.wait(timeout=15)
        return original_delete(data_dir_arg, relative_path)

    cleanup_outcome: dict[str, object] = {}

    def do_cleanup() -> None:
        # Call the service directly: the HTTP threading pool is irrelevant
        # to what is being checked (lock contention during unlink).
        from app.config import get_settings

        cleanup_outcome["result"] = service.run_cleanup(get_settings())

    with mock.patch.object(service, "_delete_file", slow_delete):
        cleanup_thread = threading.Thread(target=do_cleanup)
        cleanup_thread.start()
        assert unlink_started.wait(timeout=10)

        started = time.monotonic()
        response = client.post(
            "/exports/register", json={"snapshot_id": "a", "export_id": "stuck-001"}
        )
        elapsed = time.monotonic() - started

        # The exporter gets its receipt while the deletion is still stuck.
        assert response.status_code == 201, response.text
        assert elapsed < 5.0
        registration = response.json()
        assert registration["files"] == ["a.parquet", "shared.parquet"]

        release_unlink.set()
        cleanup_thread.join(timeout=15)

    assert not cleanup_thread.is_alive()
    cleanup = cleanup_outcome["result"]
    assert cleanup["deleted"] == ["b.parquet"]
    assert cleanup["failed"] == []
    assert (data_dir / "a.parquet").exists()
    assert (data_dir / "shared.parquet").exists()

    # The registration taken during cleanup is fully effective: it protects
    # a's files (and the shared file) until it is released.
    assert client.get("/cleanup/preview").json()["deletable"] == []
    released = client.post(f"/exports/{registration['registration_id']}/release")
    assert released.status_code == 200
    # Snapshot `a` is still retained, so its files stay protected even after
    # the export is gone; retiring `a` makes them eligible again.
    assert client.get("/cleanup/preview").json()["deletable"] == []
    assert client.post("/snapshots/a/retire").status_code == 200
    assert client.get("/cleanup/preview").json()["deletable"] == [
        "a.parquet",
        "shared.parquet",
    ]


def test_concurrent_cleanup_reports_deleted_then_already_absent(api):
    client, data_dir = api
    assert client.post("/snapshots/a/retire").status_code == 200
    assert client.post("/snapshots/b/retire").status_code == 200
    # Fixture files already exist; both rounds race over the same paths.
    for name in ("a.parquet", "b.parquet", "shared.parquet"):
        (data_dir / name).write_bytes(b"PAR1-PAR1")

    barrier = threading.Barrier(2)
    original_delete = service._delete_file

    def racing_delete(data_dir_arg: Path, relative_path: str) -> str:
        barrier.wait(timeout=10)
        return original_delete(data_dir_arg, relative_path)

    outcomes: list[dict[str, object]] = []

    def do_cleanup() -> None:
        from app.config import get_settings

        outcomes.append(service.run_cleanup(get_settings()))

    with mock.patch.object(service, "_delete_file", racing_delete):
        threads = [threading.Thread(target=do_cleanup) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=15)

    assert len(outcomes) == 2
    deleted = [path for result in outcomes for path in result["deleted"]]
    already_absent = [
        path for result in outcomes for path in result["already_absent"]
    ]
    # Every path is reported as deleted by exactly one round and as already
    # absent by the other.
    assert sorted(deleted) == ["a.parquet", "b.parquet", "shared.parquet"]
    assert sorted(already_absent) == ["a.parquet", "b.parquet", "shared.parquet"]
    for result in outcomes:
        assert result["failed"] == []
    for name in ("a.parquet", "b.parquet", "shared.parquet"):
        assert not (data_dir / name).exists()
