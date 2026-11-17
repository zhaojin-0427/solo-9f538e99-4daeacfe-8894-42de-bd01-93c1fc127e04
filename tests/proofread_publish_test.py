#!/usr/bin/env python3
"""End-to-end self-test for the proofreading + edition publishing workflow.

Spins up the real services as local processes (mirroring docker-compose):
  site_a :8201, site_b :8202, api :8200 (SQLite DB under a temp dir)

Verifies:
  * revisions can only be applied to successfully collected announcements;
  * publishing is blocked until every announcement is confirmed;
  * editing a confirmed announcement invalidates its confirmation;
  * an edition is a point-in-time snapshot of the fully confirmed text;
  * editions are immutable: later revisions never change a published version,
    old versions remain downloadable by version number;
  * a failed publish leaves no downloadable half-edition;
  * the raw extraction download stays available and untouched;
  * proofreading state and editions survive an API restart;
  * request validation and 404/409 semantics.
"""
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
API = "http://127.0.0.1:8200"
A = "http://127.0.0.1:8201"
B = "http://127.0.0.1:8202"

PY = sys.executable
FAILURES = []
PROCS = []
LOGDIR = None
DB_DIR = None


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def check(name, cond, detail=""):
    mark = "PASS" if cond else "FAIL"
    print(f"  [{mark}] {name}" + (f"  ({detail})" if detail else ""), flush=True)
    if not cond:
        FAILURES.append(f"{name} {detail}")


def start_proc(name, args, env, port):
    logfile = open(LOGDIR / f"{name}.log", "w")
    p = subprocess.Popen(args, env=env, stdout=logfile, stderr=subprocess.STDOUT)
    PROCS.append((name, p))
    deadline = time.time() + 30
    while time.time() < deadline:
        if p.poll() is not None:
            raise RuntimeError(f"{name} exited early, see {LOGDIR}/{name}.log")
        try:
            httpx.get(f"http://127.0.0.1:{port}/", timeout=1.0)
            return p
        except Exception:
            time.sleep(0.2)
    raise RuntimeError(f"{name} did not start on port {port}")


def start_api():
    env = dict(os.environ, DB_PATH=str(DB_DIR / "collector.db"))
    return start_proc("api", [PY, "-m", "uvicorn", "app.main:app", "--app-dir", str(ROOT / "api"),
                              "--host", "127.0.0.1", "--port", "8200"], env, 8200)


def start_site(name, port, site_name, peer_base):
    env = dict(os.environ, SITE_NAME=site_name, PEER_BASE=peer_base)
    return start_proc(name, [PY, "-m", "uvicorn", "app:app", "--app-dir", str(ROOT / "sites"),
                             "--host", "127.0.0.1", "--port", str(port)], env, port)


def stop_api():
    for i, (name, p) in enumerate(PROCS):
        if name == "api" and p.poll() is None:
            p.send_signal(signal.SIGTERM)
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
            PROCS.pop(i)
            return


def wait_job(client, job_id, timeout=60.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        r = client.get(f"{API}/jobs/{job_id}")
        r.raise_for_status()
        body = r.json()
        if body["status"] != "running":
            return body
        time.sleep(0.2)
    raise TimeoutError(f"job {job_id} still running after {timeout}s")


def submit(client, urls):
    r = client.post(f"{API}/jobs", json={"urls": urls})
    assert r.status_code == 202, r.text
    return r.json()["job_id"]


def main():
    global LOGDIR, DB_DIR
    work = Path(tempfile.mkdtemp(prefix="collector-proof-"))
    LOGDIR = work / "logs"
    LOGDIR.mkdir(parents=True)
    DB_DIR = work / "db"
    DB_DIR.mkdir()
    log(f"workdir: {work}")

    start_site("site_a", 8201, "site_a", B)
    start_site("site_b", 8202, "site_b", A)
    start_api()
    client = httpx.Client(timeout=30.0)

    try:
        # ---------- phase 1: collect a successful two-document job
        log("phase 1: successful job, raw extraction available")
        job = submit(client, [f"{A}/article/1", f"{B}/article/2"])
        body = wait_job(client, job)
        check("p1 job success", body["status"] == "success", str(body["status"]))
        check("p1 nothing confirmed initially", body["confirmed"] == 0, str(body["confirmed"]))
        check("p1 not all_confirmed", body["all_confirmed"] is False)
        anns = {a["id"]: a for a in body["announcements"]}
        ann_ids = sorted(anns)
        id1, id2 = ann_ids[0], ann_ids[1]
        check("p1 working text equals raw text initially",
              anns[id1]["working_title"] == anns[id1]["raw_title"]
              and anns[id1]["working_content"] == anns[id1]["raw_content"])

        r = client.get(f"{API}/jobs/{job}/download")
        check("p1 raw download 200", r.status_code == 200)
        check("p1 raw download marked kind=raw", r.json()["kind"] == "raw")
        raw_docs = {d["url"]: d for d in r.json()["documents"]}
        raw_title_1 = raw_docs[f"{A}/article/1"]["title"]
        raw_content_1 = raw_docs[f"{A}/article/1"]["content"]
        check("p1 raw title is extracted text", "招标公告 1 号" in raw_title_1, raw_title_1)

        # ---------- phase 2: publishing blocked until everything is confirmed
        log("phase 2: publish gating and revision/confirmation state")
        r = client.post(f"{API}/jobs/{job}/editions")
        check("p2 publish blocked while unconfirmed (409)", r.status_code == 409, r.text)
        check("p2 409 names unconfirmed ids", str(id1) in r.json()["detail"], r.text)
        check("p2 blocked publish created no edition",
              client.get(f"{API}/jobs/{job}/editions").json()["editions"] == [])

        r = client.post(f"{API}/jobs/{job}/announcements/{id1}/revisions",
                        json={"title": "修订标题一", "content": "修订正文一"})
        check("p2 revision saved 200", r.status_code == 200, r.text)
        a1 = r.json()["announcement"]
        check("p2 working text updated",
              a1["working_title"] == "修订标题一" and a1["working_content"] == "修订正文一")
        check("p2 raw text preserved on the announcement",
              a1["raw_title"] == raw_title_1 and a1["raw_content"] == raw_content_1)
        check("p2 revision leaves announcement unconfirmed", a1["confirmed"] is False)

        r = client.post(f"{API}/jobs/{job}/announcements/{id1}/confirm")
        check("p2 confirm 200", r.status_code == 200)
        a1 = r.json()["announcement"]
        check("p2 confirmed snapshot freezes working text",
              a1["confirmed"] is True
              and a1["confirmed_title"] == "修订标题一"
              and a1["confirmed_content"] == "修订正文一")

        r = client.post(f"{API}/jobs/{job}/editions")
        check("p2 still blocked: second announcement unconfirmed", r.status_code == 409)

        # partial revision: only content changes, title keeps working value
        r = client.post(f"{API}/jobs/{job}/announcements/{id2}/revisions",
                        json={"content": "只改正文二"})
        check("p2 partial revision 200", r.status_code == 200)
        a2 = r.json()["announcement"]
        check("p2 partial revision keeps working title", a2["working_title"] == a2["raw_title"])
        check("p2 partial revision updates content", a2["working_content"] == "只改正文二")
        r = client.post(f"{API}/jobs/{job}/announcements/{id2}/confirm")
        check("p2 second confirm 200", r.status_code == 200)

        body = client.get(f"{API}/jobs/{job}").json()
        check("p2 all confirmed now", body["all_confirmed"] is True,
              f"confirmed={body['confirmed']}/{body['total']}")

        # ---------- phase 3: first edition, point-in-time snapshot
        log("phase 3: publish edition v1 and download it")
        r = client.post(f"{API}/jobs/{job}/editions")
        check("p3 publish v1 201", r.status_code == 201, r.text)
        v1 = r.json()
        check("p3 v1 numbering", v1["version"] == 1 and v1["document_count"] == 2, str(v1))
        check("p3 v1 listed",
              [e["version"] for e in client.get(f"{API}/jobs/{job}/editions").json()["editions"]] == [1])

        r = client.get(f"{API}/jobs/{job}/editions/1/download")
        check("p3 v1 download 200", r.status_code == 200)
        check("p3 v1 attachment header",
              f'filename="corpus-{job}-v1.json"' in r.headers.get("content-disposition", ""),
              r.headers.get("content-disposition"))
        v1_payload = r.json()
        check("p3 v1 payload marked edition", v1_payload["kind"] == "edition"
              and v1_payload["version"] == 1)
        v1_docs = {d["announcement_id"]: d for d in v1_payload["documents"]}
        check("p3 v1 carries confirmed text of ann1",
              v1_docs[id1]["title"] == "修订标题一" and v1_docs[id1]["content"] == "修订正文一")
        check("p3 v1 carries confirmed text of ann2",
              v1_docs[id2]["content"] == "只改正文二")

        # ---------- phase 4: editing confirmed text invalidates confirmation;
        # ---------- published v1 stays byte-for-byte stable
        log("phase 4: post-confirm edit requires re-confirmation; v1 immutable")
        r = client.post(f"{API}/jobs/{job}/announcements/{id1}/revisions",
                        json={"title": "再次修订标题一"})
        check("p4 revision after confirm 200", r.status_code == 200)
        a1 = r.json()["announcement"]
        check("p4 confirmation invalidated by edit", a1["confirmed"] is False
              and a1["confirmed_title"] is None)
        body = client.get(f"{API}/jobs/{job}").json()
        check("p4 job no longer all-confirmed", body["all_confirmed"] is False)

        r = client.post(f"{API}/jobs/{job}/editions")
        check("p4 publish blocked after invalidation (409)", r.status_code == 409)
        check("p4 blocked publish added no edition",
              [e["version"] for e in client.get(f"{API}/jobs/{job}/editions").json()["editions"]] == [1])

        r = client.get(f"{API}/jobs/{job}/editions/1/download")
        v1_again = r.json()
        check("p4 v1 still downloadable after later edits", r.status_code == 200)
        check("p4 v1 content unchanged by later edits",
              json.dumps(v1_again, sort_keys=True, ensure_ascii=False)
              == json.dumps(v1_payload, sort_keys=True, ensure_ascii=False))

        r = client.post(f"{API}/jobs/{job}/announcements/{id1}/confirm")
        check("p4 re-confirm 200", r.status_code == 200)
        check("p4 re-confirmed snapshot has new text",
              r.json()["announcement"]["confirmed_title"] == "再次修订标题一")

        r = client.post(f"{API}/jobs/{job}/editions")
        check("p4 publish v2 201", r.status_code == 201, r.text)
        check("p4 v2 numbering", r.json()["version"] == 2)
        r = client.get(f"{API}/jobs/{job}/editions/2/download")
        v2_docs = {d["announcement_id"]: d for d in r.json()["documents"]}
        check("p4 v2 has the re-confirmed text",
              v2_docs[id1]["title"] == "再次修订标题一")
        r = client.get(f"{API}/jobs/{job}/editions/1/download")
        v1_docs_final = {d["announcement_id"]: d for d in r.json()["documents"]}
        check("p4 v1 still has the old confirmed text",
              v1_docs_final[id1]["title"] == "修订标题一")
        check("p4 two editions listed in order",
              [e["version"] for e in client.get(f"{API}/jobs/{job}/editions").json()["editions"]]
              == [1, 2])

        # ---------- phase 5: raw download remains available and untouched
        log("phase 5: raw corpus stays available and never reflects revisions")
        r = client.get(f"{API}/jobs/{job}/download")
        check("p5 raw download still 200", r.status_code == 200)
        raw_again = {d["url"]: d for d in r.json()["documents"]}
        check("p5 raw title untouched by proofreading",
              raw_again[f"{A}/article/1"]["title"] == raw_title_1)
        check("p5 raw content untouched by proofreading",
              raw_again[f"{A}/article/1"]["content"] == raw_content_1)

        # ---------- phase 6: failed-job gating
        log("phase 6: failed announcements cannot be proofread or published")
        bad = submit(client, [f"{B}/busy"])
        bbody = wait_job(client, bad)
        check("p6 job failed", bbody["status"] == "failed")
        bad_id = bbody["announcements"][0]["id"]
        r = client.post(f"{API}/jobs/{bad}/announcements/{bad_id}/revisions",
                        json={"title": "x"})
        check("p6 revision on failed announcement 409", r.status_code == 409, r.text)
        r = client.post(f"{API}/jobs/{bad}/announcements/{bad_id}/confirm")
        check("p6 confirm on failed announcement 409", r.status_code == 409)
        r = client.post(f"{API}/jobs/{bad}/editions")
        check("p6 publish failed job 409", r.status_code == 409)
        check("p6 failed job has no editions",
              client.get(f"{API}/jobs/{bad}/editions").json()["editions"] == [])
        r = client.get(f"{API}/jobs/{bad}/download")
        check("p6 raw download of failed job 409", r.status_code == 409)

        # ---------- phase 7: validation and 404s
        log("phase 7: validation and not-found semantics")
        r = client.post(f"{API}/jobs/{job}/announcements/{id1}/revisions", json={})
        check("p7 revision without fields 422", r.status_code == 422)
        r = client.post(f"{API}/jobs/{job}/announcements/{id1}/revisions",
                        json={"title": "   "})
        check("p7 whitespace-only revision 422", r.status_code == 422)
        r = client.post(f"{API}/jobs/{job}/announcements/999999/revisions",
                        json={"title": "x"})
        check("p7 unknown announcement 404", r.status_code == 404)
        r = client.post(f"{API}/jobs/doesnotexist/editions")
        check("p7 unknown job publish 404", r.status_code == 404)
        r = client.get(f"{API}/jobs/doesnotexist")
        check("p7 unknown job progress 404", r.status_code == 404)
        r = client.get(f"{API}/jobs/{job}/editions/99/download")
        check("p7 unknown edition 404", r.status_code == 404)

        # ---------- phase 8: restart persistence
        log("phase 8: restart keeps revisions, confirmations and editions")
        stop_api()
        log("  api stopped, restarting...")
        time.sleep(1.0)
        start_api()
        body = client.get(f"{API}/jobs/{job}").json()
        check("p8 all_confirmed survives restart", body["all_confirmed"] is True)
        a1 = next(a for a in body["announcements"] if a["id"] == id1)
        check("p8 working/confirmed text survives restart",
              a1["working_title"] == "再次修订标题一"
              and a1["confirmed_title"] == "再次修订标题一"
              and a1["confirmed"] is True)
        editions = client.get(f"{API}/jobs/{job}/editions").json()["editions"]
        check("p8 both editions survive restart", [e["version"] for e in editions] == [1, 2])
        r = client.get(f"{API}/jobs/{job}/editions/1/download")
        check("p8 v1 downloadable after restart", r.status_code == 200
              and r.json()["documents"][0]["title"] == "修订标题一")
        r = client.get(f"{API}/jobs/{job}/download")
        check("p8 raw download after restart", r.status_code == 200
              and r.json()["documents"][0]["title"] == raw_title_1)
        r = client.post(f"{API}/jobs/{job}/editions")
        check("p8 can still publish v3 after restart", r.status_code == 201
              and r.json()["version"] == 3)

    finally:
        for name, p in PROCS:
            if p.poll() is None:
                p.send_signal(signal.SIGTERM)
        time.sleep(0.5)
        for name, p in PROCS:
            if p.poll() is None:
                p.kill()

    print()
    if FAILURES:
        print(f"RESULT: {len(FAILURES)} FAILURE(S)")
        for f in FAILURES:
            print(f"  - {f}")
        print(f"logs kept at {LOGDIR}")
        return 1
    print("RESULT: ALL CHECKS PASSED")
    shutil.rmtree(work, ignore_errors=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
