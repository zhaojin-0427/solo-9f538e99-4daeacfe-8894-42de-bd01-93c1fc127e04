"""FastAPI application: submit jobs, query progress, proofread and publish."""
import logging
import os
import time
import uuid
from collections import Counter
from contextlib import asynccontextmanager
from urllib.parse import urlparse

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel, Field, field_validator

from .db import DB, EditionError
from .engine import FetchEngine
from .extract import extract_document

DB_PATH = os.environ.get("DB_PATH", "./collector.db")

log = logging.getLogger("collector.api")

PROOFREAD_TAGS = ["proofreading & publishing"]


@asynccontextmanager
async def lifespan(app: FastAPI):
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    collector_logger = logging.getLogger("collector")
    collector_logger.setLevel(logging.INFO)
    if not collector_logger.handlers:
        collector_logger.addHandler(handler)

    db_dir = os.path.dirname(os.path.abspath(DB_PATH))
    os.makedirs(db_dir, exist_ok=True)
    db = DB(DB_PATH)
    engine = FetchEngine(db, extract_document)
    await engine.start()
    app.state.db = db
    app.state.engine = engine

    # Resume unfinished announcements; successful ones are NOT re-fetched.
    resumed = 0
    for row in db.unfinished_announcements():
        db.update_ann(row["id"], status="pending")
        engine.submit(row["id"], row["current_url"], row["hops"], row["attempts"])
        resumed += 1
    log.info("startup: resumed %d unfinished announcement(s), active cooldowns: %s",
             resumed, engine.cooldowns_snapshot())
    yield
    await engine.stop()
    db.close()


app = FastAPI(
    title="Tender Corpus Collector",
    version="1.1.0",
    description=(
        "Collect tender announcements from multiple official sites. "
        "Submit up to 20 announcement URLs per job; the collector extracts "
        "HTML title and body text with shared quotas (2 in-flight requests "
        "globally, 1 per origin), per-origin cooldowns on 429/503 + "
        "Retry-After, max 3 redirect hops and max 2 retries per URL. "
        "The full JSON corpus is downloadable only when every announcement "
        "of a job succeeded. "
        "Proofreaders can then revise and confirm the extracted title/body "
        "(editing a confirmed announcement invalidates its confirmation); an "
        "immutable, versioned edition can be published only when the whole "
        "job succeeded and every announcement is confirmed. Editions and the "
        "raw extraction are independently downloadable."
    ),
    lifespan=lifespan,
)


class JobCreate(BaseModel):
    urls: list[str] = Field(min_length=1, max_length=20,
                            description="1-20 announcement URLs (http/https)")

    @field_validator("urls")
    @classmethod
    def validate_and_dedupe(cls, urls):
        seen, out = set(), []
        for raw in urls:
            u = raw.strip()
            p = urlparse(u)
            if p.scheme not in ("http", "https") or not p.netloc:
                raise ValueError(f"invalid URL (http/https required): {raw!r}")
            if u not in seen:
                seen.add(u)
                out.append(u)
        if not out:
            raise ValueError("no usable URLs")
        return out


class RevisionCreate(BaseModel):
    title: str | None = Field(
        default=None,
        description="Revised title. Omit to keep the current working title.",
    )
    content: str | None = Field(
        default=None,
        description="Revised body text. Omit to keep the current working content.",
    )

    @field_validator("title", "content")
    @classmethod
    def reject_empty_strings(cls, v):
        if v is not None and not v.strip():
            raise ValueError("revised text must not be empty or whitespace-only")
        return v


def _working_title(row) -> str:
    return row["title"] if row["revised_title"] is None else row["revised_title"]


def _working_content(row) -> str:
    return row["content"] if row["revised_content"] is None else row["revised_content"]


def _announcement_view(a) -> dict:
    """Serialization of one announcement including proofreading state."""
    return {
        "id": a["id"],
        "url": a["url"],
        "current_url": a["current_url"],
        "status": a["status"],
        "hops": a["hops"],
        "attempts": a["attempts"],
        # raw, untouched extraction output (also used by the raw download)
        "raw_title": a["title"],
        "raw_content": a["content"],
        # current working text (revision applied, if any)
        "working_title": _working_title(a),
        "working_content": _working_content(a),
        "revised_title": a["revised_title"],
        "revised_content": a["revised_content"],
        "confirmed": bool(a["confirmed"]),
        "confirmed_title": a["confirmed_title"],
        "confirmed_content": a["confirmed_content"],
        "confirmed_at": a["confirmed_at"],
        "error": a["error"],
        "updated_at": a["updated_at"],
    }


@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse(url="/docs")


@app.post("/jobs", status_code=202, summary="Submit a collection job (up to 20 URLs)")
async def create_job(payload: JobCreate):
    db: DB = app.state.db
    engine: FetchEngine = app.state.engine
    job_id = uuid.uuid4().hex[:12]
    db.create_job(job_id, total=len(payload.urls))
    for url in payload.urls:
        ann_id = db.add_announcement(job_id, url)
        engine.submit(ann_id, url, hops=0, attempts=0)
    return {
        "job_id": job_id,
        "total": len(payload.urls),
        "status": "running",
        "progress_url": f"/jobs/{job_id}",
        "download_url": f"/jobs/{job_id}/download",
        "editions_url": f"/jobs/{job_id}/editions",
    }


@app.get("/jobs", summary="List all jobs with progress counters")
async def list_jobs():
    rows = app.state.db.list_jobs()
    jobs = []
    for r in rows:
        status = "running" if r["unfinished"] else ("failed" if r["failed"] else "success")
        jobs.append({
            "job_id": r["id"], "created_at": r["created_at"], "total": r["total"],
            "success": r["success"], "failed": r["failed"], "unfinished": r["unfinished"],
            "status": status,
        })
    return {"jobs": jobs}


def _job_or_404(job_id: str):
    job = app.state.db.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return job


def _successful_ann_or_409(job_id: str, ann_id: int):
    """Fetch the announcement and ensure it belongs to the job and succeeded."""
    _job_or_404(job_id)
    ann = app.state.db.get_announcement_for_job(job_id, ann_id)
    if ann is None:
        raise HTTPException(status_code=404, detail="announcement not found in this job")
    if ann["status"] != "success":
        raise HTTPException(
            status_code=409,
            detail=f"announcement {ann_id} is not collected successfully "
                   f"(current status: {ann['status']}); only successful announcements "
                   "can be proofread",
        )
    return ann


@app.get("/jobs/{job_id}", summary="Job progress with per-announcement and proofreading state")
async def job_progress(job_id: str):
    job = _job_or_404(job_id)
    db: DB = app.state.db
    anns = db.list_announcements(job_id)
    counts = Counter(a["status"] for a in anns)
    if counts["pending"] or counts["fetching"]:
        status = "running"
    elif counts["failed"]:
        status = "failed"
    else:
        status = "success"
    confirmed = sum(1 for a in anns if a["confirmed"])
    editions = db.list_editions(job_id)
    return {
        "job_id": job_id,
        "status": status,
        "created_at": job["created_at"],
        "total": len(anns),
        "pending": counts["pending"],
        "fetching": counts["fetching"],
        "success": counts["success"],
        "failed": counts["failed"],
        "confirmed": confirmed,
        "all_confirmed": confirmed == len(anns) and len(anns) > 0 and status == "success",
        "raw_downloadable": status == "success",
        "editions": [dict(e) for e in editions],
        "announcements": [_announcement_view(a) for a in anns],
    }


@app.get("/jobs/{job_id}/download",
         summary="Download the RAW extracted JSON corpus (only when ALL succeeded)")
async def download_raw_corpus(job_id: str):
    """Original extraction output. Proofreading never alters this payload."""
    _job_or_404(job_id)
    anns = app.state.db.list_announcements(job_id)
    not_done = [a for a in anns if a["status"] != "success"]
    if not_done:
        counts = Counter(a["status"] for a in anns)
        raise HTTPException(
            status_code=409,
            detail=(
                "corpus is only available when every announcement succeeded; "
                f"current counts: {dict(counts)}. Partial results are not provided."
            ),
        )
    documents = [
        {
            "url": a["url"],
            "final_url": a["current_url"],
            "hops": a["hops"],
            "title": a["title"],
            "content": a["content"],
        }
        for a in anns
    ]
    payload = {
        "job_id": job_id,
        "kind": "raw",
        "generated_at": time.time(),
        "document_count": len(documents),
        "documents": documents,
    }
    return JSONResponse(
        content=payload,
        headers={"Content-Disposition": f'attachment; filename="corpus-{job_id}-raw.json"'},
    )


# ----------------------------------------------------------------------
# Proofreading & publishing
# ----------------------------------------------------------------------
@app.post(
    "/jobs/{job_id}/announcements/{ann_id}/revisions",
    tags=PROOFREAD_TAGS,
    summary="Submit a revision for one successfully collected announcement",
)
async def revise_announcement(job_id: str, ann_id: int, payload: RevisionCreate):
    if payload.title is None and payload.content is None:
        raise HTTPException(
            status_code=422,
            detail="provide at least one of 'title' or 'content' to revise",
        )
    _successful_ann_or_409(job_id, ann_id)
    db: DB = app.state.db
    updated = db.save_revision(ann_id, title=payload.title, content=payload.content)
    return {
        "announcement": _announcement_view(updated),
        "message": "revision saved; confirmation is required before publishing",
    }


@app.post(
    "/jobs/{job_id}/announcements/{ann_id}/confirm",
    tags=PROOFREAD_TAGS,
    summary="Confirm the current working title/body of one announcement",
)
async def confirm_announcement(job_id: str, ann_id: int):
    _successful_ann_or_409(job_id, ann_id)
    db: DB = app.state.db
    updated = db.confirm_announcement(ann_id)
    return {
        "announcement": _announcement_view(updated),
        "message": "current working text confirmed",
    }


@app.post(
    "/jobs/{job_id}/editions",
    tags=PROOFREAD_TAGS,
    status_code=201,
    summary="Publish a new immutable proofread edition (all success + all confirmed)",
)
async def publish_edition(job_id: str):
    _job_or_404(job_id)
    db: DB = app.state.db
    try:
        edition = db.create_edition(job_id)
    except EditionError as exc:
        msg = str(exc)
        # Unknown job is a 404; every other precondition failure is a 409.
        raise HTTPException(status_code=404 if "job not found" in msg else 409, detail=msg)
    edition["download_url"] = f"/jobs/{job_id}/editions/{edition['version']}/download"
    return edition


@app.get(
    "/jobs/{job_id}/editions",
    tags=PROOFREAD_TAGS,
    summary="List published editions of a job (versioned, immutable)",
)
async def list_editions(job_id: str):
    _job_or_404(job_id)
    rows = app.state.db.list_editions(job_id)
    return {
        "job_id": job_id,
        "editions": [
            {
                **dict(r),
                "download_url": f"/jobs/{job_id}/editions/{r['version']}/download",
            }
            for r in rows
        ],
    }


@app.get(
    "/jobs/{job_id}/editions/{version}/download",
    tags=PROOFREAD_TAGS,
    summary="Download one published edition by version number (frozen snapshot)",
)
async def download_edition(job_id: str, version: int):
    _job_or_404(job_id)
    db: DB = app.state.db
    edition = db.get_edition(job_id, version)
    if edition is None:
        raise HTTPException(status_code=404, detail=f"edition v{version} not found")
    rows = db.list_edition_documents(job_id, version)
    documents = [
        {
            "announcement_id": r["ann_id"],
            "url": r["url"],
            "final_url": r["final_url"],
            "hops": r["hops"],
            "title": r["title"],
            "content": r["content"],
        }
        for r in rows
    ]
    payload = {
        "job_id": job_id,
        "kind": "edition",
        "version": version,
        "published_at": edition["created_at"],
        "document_count": edition["document_count"],
        "documents": documents,
    }
    return JSONResponse(
        content=payload,
        headers={
            "Content-Disposition":
                f'attachment; filename="corpus-{job_id}-v{version}.json"'
        },
    )


@app.get("/cooldowns", summary="Currently active per-origin cooldowns")
async def cooldowns():
    now = time.time()
    active = [
        {"origin": origin, "until_ts": until, "remaining_seconds": round(max(0.0, until - now), 3)}
        for origin, until in app.state.engine.cooldowns_snapshot().items()
        if until > now
    ]
    return {"cooldowns": active}
