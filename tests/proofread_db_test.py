#!/usr/bin/env python3
"""In-process tests for proofreading persistence: schema migration,
atomic/serialized edition publishing (no half-editions), snapshot
immutability and HTTP-level concurrency through the ASGI app."""
import asyncio
import sqlite3
import sys
import tempfile
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "api"))
sys.path.insert(0, "/tmp/pydeps")

import httpx  # noqa: E402

from app.db import DB, EditionError  # noqa: E402
from app import main as api_main  # noqa: E402

FAILURES = []


def check(name, cond, detail=""):
    mark = "PASS" if cond else "FAIL"
    print(f"  [{mark}] {name}" + (f"  ({detail})" if detail else ""), flush=True)
    if not cond:
        FAILURES.append(f"{name} {detail}")


def make_job(db, n=2):
    job_id = "job1"
    db.create_job(job_id, total=n)
    ids = []
    for i in range(n):
        ann_id = db.add_announcement(job_id, f"http://x/{i}")
        db.mark_fetch_success(ann_id, f"raw title {i}", f"raw content {i}")
        ids.append(ann_id)
    return job_id, ids


def test_migration(tmpdir):
    print("test: schema migration from a pre-proofreading database")
    path = tmpdir / "old.db"
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE jobs (id TEXT PRIMARY KEY, created_at REAL NOT NULL, total INTEGER NOT NULL);
        CREATE TABLE announcements (
            id INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL, url TEXT NOT NULL,
            current_url TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
            hops INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0,
            title TEXT, content TEXT, error TEXT, updated_at REAL NOT NULL DEFAULT 0
        );
        CREATE TABLE cooldowns (origin TEXT PRIMARY KEY, until_ts REAL NOT NULL);
        """
    )
    conn.execute("INSERT INTO jobs (id, created_at, total) VALUES ('legacy', 1.0, 1)")
    conn.execute(
        "INSERT INTO announcements (job_id, url, current_url, status, title, content, updated_at)"
        " VALUES ('legacy', 'http://x/1', 'http://x/1', 'success', 'old title', 'old body', 1.0)"
    )
    conn.commit()
    conn.close()

    db = DB(str(path))
    row = db.get_announcement(1)
    check("migrated row keeps raw text", row["title"] == "old title" and row["content"] == "old body")
    check("migrated row defaults unconfirmed", row["confirmed"] == 0 and row["confirmed_at"] is None)
    check("migrated row has no revision", row["revised_title"] is None and row["revised_content"] is None)
    # migrated data can immediately enter the proofreading workflow
    db.confirm_announcement(1)
    edition = db.create_edition("legacy")
    check("migrated DB can publish", edition["version"] == 1)
    check("migrated edition freezes raw text",
          db.list_edition_documents("legacy", 1)[0]["title"] == "old title")
    db.close()


def test_rollback_and_snapshot(tmpdir):
    print("test: failed publish rolls back fully; snapshots are immutable")
    db = DB(str(tmpdir / "a.db"))
    job_id, ids = make_job(db)

    try:
        db.create_edition(job_id)
        check("publish without confirmations raises", False)
    except EditionError:
        check("publish without confirmations raises", True)
    check("no edition row after failed publish", db.list_editions(job_id) == [])
    # verify directly at SQL level that nothing half-written exists
    conn = sqlite3.connect(tmpdir / "a.db")
    check("zero edition rows in storage",
          conn.execute("SELECT COUNT(*) FROM editions").fetchone()[0] == 0)
    check("zero edition_document rows in storage",
          conn.execute("SELECT COUNT(*) FROM edition_documents").fetchone()[0] == 0)
    conn.close()

    db.save_revision(ids[0], title="T0", content="C0")
    db.confirm_announcement(ids[0])
    try:
        db.create_edition(job_id)
        check("partial confirmation still rejected", False)
    except EditionError as e:
        check("partial confirmation still rejected", "not confirmed" in str(e))
    check("still no editions after second failure", db.list_editions(job_id) == [])

    db.confirm_announcement(ids[1])
    v1 = db.create_edition(job_id)
    check("v1 published once all confirmed", v1["version"] == 1)

    # edit confirmed text -> invalidated; v1 must not change
    db.save_revision(ids[0], title="T0-edited")
    check("edit invalidates confirmation", db.get_announcement(ids[0])["confirmed"] == 0)
    db.confirm_announcement(ids[0])
    v2 = db.create_edition(job_id)
    check("v2 gets next version number", v2["version"] == 2)
    d1_v1 = db.list_edition_documents(job_id, 1)
    d1_v2 = db.list_edition_documents(job_id, 2)
    check("v1 frozen on old text", d1_v1[0]["title"] == "T0" and d1_v1[0]["content"] == "C0")
    check("v2 carries new text", d1_v2[0]["title"] == "T0-edited")
    check("both editions complete (2 docs each)", len(d1_v1) == 2 and len(d1_v2) == 2)

    # re-fetch (e.g. resume scenario) wipes proofreading state but not editions
    db.mark_fetch_success(ids[0], "fresh title", "fresh body")
    row = db.get_announcement(ids[0])
    check("re-fetch resets confirmation and revision",
          row["confirmed"] == 0 and row["revised_title"] is None)
    check("published edition survives re-fetch",
          db.list_edition_documents(job_id, 1)[0]["title"] == "T0")
    db.close()


def test_concurrent_publish(tmpdir):
    print("test: concurrent publishes serialize into distinct complete editions")
    db = DB(str(tmpdir / "c.db"))
    job_id, ids = make_job(db, n=3)
    for ann_id in ids:
        db.confirm_announcement(ann_id)

    results, errors = [], []

    def worker():
        try:
            results.append(db.create_edition(job_id))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    check("all 8 concurrent publishes succeeded", not errors, str(errors))
    versions = sorted(e["version"] for e in results)
    check("distinct sequential versions 1..8", versions == list(range(1, 9)), str(versions))
    for v in versions:
        docs = db.list_edition_documents(job_id, v)
        check(f"edition v{v} complete (3 docs)", len(docs) == 3)
    db.close()


async def test_http_concurrency_and_gating(tmpdir):
    print("test: HTTP layer gating and concurrent publish via ASGI transport")
    db = DB(str(tmpdir / "h.db"))
    api_main.app.state.db = db  # no lifespan/engine needed for these endpoints
    job_id, ids = make_job(db)
    transport = httpx.ASGITransport(app=api_main.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        r = await c.post(f"/jobs/{job_id}/editions")
        check("HTTP publish blocked while unconfirmed", r.status_code == 409)
        r = await c.post(f"/jobs/{job_id}/announcements/{ids[0]}/revisions",
                         json={"title": "HTTP 标题", "content": "HTTP 正文"})
        check("HTTP revision ok", r.status_code == 200 and
              r.json()["announcement"]["working_title"] == "HTTP 标题")
        r = await c.post(f"/jobs/{job_id}/announcements/{ids[0]}/confirm")
        check("HTTP confirm ok", r.status_code == 200)
        r = await c.post(f"/jobs/{job_id}/announcements/{ids[1]}/confirm")
        check("HTTP confirm raw text ok", r.status_code == 200)

        async def publish():
            return await c.post(f"/jobs/{job_id}/editions")

        responses = await asyncio.gather(*[publish() for _ in range(5)])
        codes = sorted(r.status_code for r in responses)
        check("5 concurrent HTTP publishes all 201", codes == [201] * 5, str(codes))
        got_versions = sorted(r.json()["version"] for r in responses)
        check("HTTP versions 1..5 unique", got_versions == [1, 2, 3, 4, 5], str(got_versions))

        listed = (await c.get(f"/jobs/{job_id}/editions")).json()["editions"]
        check("all 5 editions listed", [e["version"] for e in listed] == [1, 2, 3, 4, 5])
        r = await c.get(f"/jobs/{job_id}/editions/3/download")
        check("edition v3 downloadable", r.status_code == 200 and r.json()["version"] == 3)
        r = await c.get(f"/jobs/{job_id}/editions/42/download")
        check("missing edition 404", r.status_code == 404)

        # raw download still works and stays raw
        r = await c.get(f"/jobs/{job_id}/download")
        check("raw download ok", r.status_code == 200 and r.json()["kind"] == "raw")
        check("raw download untouched", r.json()["documents"][0]["title"] == "raw title 0")
    db.close()


def main():
    with tempfile.TemporaryDirectory(prefix="proof-db-") as d:
        tmpdir = Path(d)
        test_migration(tmpdir)
        test_rollback_and_snapshot(tmpdir)
        test_concurrent_publish(tmpdir)
        asyncio.run(test_http_concurrency_and_gating(tmpdir))
    print()
    if FAILURES:
        print(f"RESULT: {len(FAILURES)} FAILURE(S)")
        for f in FAILURES:
            print(f"  - {f}")
        return 1
    print("RESULT: ALL CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
