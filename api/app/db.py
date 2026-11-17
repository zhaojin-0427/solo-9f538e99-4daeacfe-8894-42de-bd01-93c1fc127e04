"""SQLite persistence layer.

Stores jobs, per-announcement fetch state and per-origin cooldowns so that a
restart can resume unfinished work without re-fetching successful documents
and without losing active cooldown deadlines.

Also stores the proofreading workflow state:

  announcements.revised_title / revised_content
      the proofreader's current *working* text (NULL means "still the
      originally extracted text");
  announcements.confirmed + confirmed_title / confirmed_content / confirmed_at
      whether the working text has been confirmed, and an immutable snapshot
      of the exact text that was confirmed;

and the published editions:

  editions / edition_documents
      immutable per-job versioned snapshots. A publish is a single write
      transaction, so a failure can never leave a downloadable half-edition,
      and later revisions never touch rows already copied into an edition.
"""
import sqlite3
import threading
import time

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id         TEXT PRIMARY KEY,
    created_at REAL NOT NULL,
    total      INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS announcements (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id      TEXT NOT NULL REFERENCES jobs(id),
    url         TEXT NOT NULL,           -- originally submitted URL
    current_url TEXT NOT NULL,           -- URL currently being fetched (changes on redirect)
    status      TEXT NOT NULL DEFAULT 'pending',  -- pending|fetching|success|failed
    hops        INTEGER NOT NULL DEFAULT 0,       -- redirects followed so far
    attempts    INTEGER NOT NULL DEFAULT 0,       -- retries used on current_url
    title       TEXT,
    content     TEXT,
    error       TEXT,
    updated_at  REAL NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_ann_job ON announcements(job_id);
CREATE TABLE IF NOT EXISTS cooldowns (
    origin   TEXT PRIMARY KEY,
    until_ts REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS editions (
    job_id         TEXT NOT NULL,
    version        INTEGER NOT NULL,     -- 1-based, per job
    created_at     REAL NOT NULL,
    document_count INTEGER NOT NULL,
    PRIMARY KEY (job_id, version)
);
CREATE TABLE IF NOT EXISTS edition_documents (
    job_id    TEXT NOT NULL,
    version   INTEGER NOT NULL,
    doc_order INTEGER NOT NULL,
    ann_id    INTEGER NOT NULL,
    url       TEXT NOT NULL,
    final_url TEXT NOT NULL,
    hops      INTEGER NOT NULL,
    title     TEXT NOT NULL,             -- frozen confirmed snapshot
    content   TEXT NOT NULL,
    PRIMARY KEY (job_id, version, doc_order)
);
"""

# Columns added after the initial release; migrated with ALTER TABLE so that
# databases created by older versions keep working.
_ANN_ADDED_COLUMNS = [
    ("revised_title", "revised_title TEXT"),
    ("revised_content", "revised_content TEXT"),
    ("confirmed", "confirmed INTEGER NOT NULL DEFAULT 0"),
    ("confirmed_title", "confirmed_title TEXT"),
    ("confirmed_content", "confirmed_content TEXT"),
    ("confirmed_at", "confirmed_at REAL"),
]

_UPDATABLE = {"current_url", "status", "hops", "attempts", "title", "content", "error"}


class EditionError(Exception):
    """Raised when an edition cannot be published (precondition not met)."""


class DB:
    def __init__(self, path: str) -> None:
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._lock = threading.Lock()
        with self._lock, self._conn:
            self._conn.executescript(SCHEMA)
            existing = {r["name"] for r in self._conn.execute("PRAGMA table_info(announcements)")}
            for name, decl in _ANN_ADDED_COLUMNS:
                if name not in existing:
                    self._conn.execute(f"ALTER TABLE announcements ADD COLUMN {decl}")

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ---- jobs ---------------------------------------------------------
    def create_job(self, job_id: str, total: int) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO jobs (id, created_at, total) VALUES (?, ?, ?)",
                (job_id, time.time(), total),
            )

    def get_job(self, job_id: str):
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM jobs WHERE id = ?", (job_id,)
            ).fetchone()

    def list_jobs(self):
        with self._lock:
            return self._conn.execute(
                """
                SELECT j.id, j.created_at, j.total,
                       COALESCE(SUM(a.status = 'success'), 0)  AS success,
                       COALESCE(SUM(a.status = 'failed'), 0)   AS failed,
                       COALESCE(SUM(a.status IN ('pending','fetching')), 0) AS unfinished
                FROM jobs j LEFT JOIN announcements a ON a.job_id = j.id
                GROUP BY j.id ORDER BY j.created_at DESC
                """
            ).fetchall()

    # ---- announcements -------------------------------------------------
    def add_announcement(self, job_id: str, url: str) -> int:
        with self._lock, self._conn:
            cur = self._conn.execute(
                "INSERT INTO announcements (job_id, url, current_url, status, updated_at)"
                " VALUES (?, ?, ?, 'pending', ?)",
                (job_id, url, url, time.time()),
            )
            return cur.lastrowid

    def update_ann(self, ann_id: int, **fields) -> None:
        fields = {k: v for k, v in fields.items() if k in _UPDATABLE}
        fields["updated_at"] = time.time()
        cols = ", ".join(f"{k} = ?" for k in fields)
        with self._lock, self._conn:
            self._conn.execute(
                f"UPDATE announcements SET {cols} WHERE id = ?",
                (*fields.values(), ann_id),
            )

    def mark_fetch_success(self, ann_id: int, title: str, content: str) -> None:
        """Record a successful fetch and reset any proofreading state.

        A freshly extracted document is unconfirmed with no revisions; the
        raw title/content stay untouched by later proofreading.
        """
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE announcements SET status='success', title=?, content=?, error=NULL,"
                " revised_title=NULL, revised_content=NULL, confirmed=0,"
                " confirmed_title=NULL, confirmed_content=NULL, confirmed_at=NULL,"
                " updated_at=? WHERE id=?",
                (title, content, time.time(), ann_id),
            )

    def get_announcement(self, ann_id: int):
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM announcements WHERE id = ?", (ann_id,)
            ).fetchone()

    def get_announcement_for_job(self, job_id: str, ann_id: int):
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM announcements WHERE id = ? AND job_id = ?",
                (ann_id, job_id),
            ).fetchone()

    def list_announcements(self, job_id: str):
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM announcements WHERE job_id = ? ORDER BY id", (job_id,)
            ).fetchall()

    def unfinished_announcements(self):
        with self._lock:
            return self._conn.execute(
                "SELECT id, current_url, hops, attempts FROM announcements"
                " WHERE status IN ('pending', 'fetching') ORDER BY id"
            ).fetchall()

    # ---- proofreading ---------------------------------------------------
    def save_revision(self, ann_id: int, title=None, content=None):
        """Apply a partial revision to the working text.

        Omitted fields keep their current working value. Any revision - even
        one that happens after a confirmation - invalidates the confirmation:
        the announcement must be confirmed again before it can be published.
        """
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE announcements SET"
                " revised_title = COALESCE(?, revised_title),"
                " revised_content = COALESCE(?, revised_content),"
                " confirmed = 0, confirmed_title = NULL, confirmed_content = NULL,"
                " confirmed_at = NULL, updated_at = ?"
                " WHERE id = ?",
                (title, content, time.time(), ann_id),
            )
        return self.get_announcement(ann_id)

    def confirm_announcement(self, ann_id: int):
        """Freeze the current working text as the confirmed snapshot."""
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE announcements SET confirmed = 1,"
                " confirmed_title = COALESCE(revised_title, title),"
                " confirmed_content = COALESCE(revised_content, content),"
                " confirmed_at = ?, updated_at = ?"
                " WHERE id = ? AND status = 'success'",
                (time.time(), time.time(), ann_id),
            )
        return self.get_announcement(ann_id)

    # ---- editions --------------------------------------------------------
    def list_editions(self, job_id: str):
        with self._lock:
            return self._conn.execute(
                "SELECT job_id, version, created_at, document_count FROM editions"
                " WHERE job_id = ? ORDER BY version",
                (job_id,),
            ).fetchall()

    def get_edition(self, job_id: str, version: int):
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM editions WHERE job_id = ? AND version = ?",
                (job_id, version),
            ).fetchone()

    def list_edition_documents(self, job_id: str, version: int):
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM edition_documents WHERE job_id = ? AND version = ?"
                " ORDER BY doc_order",
                (job_id, version),
            ).fetchall()

    def create_edition(self, job_id: str) -> dict:
        """Atomically publish a new edition from the confirmed snapshots.

        Preconditions (all checked inside one write transaction):
          * every announcement of the job fetched successfully;
          * every announcement is currently confirmed.

        The version number, the edition row and all document rows are inserted
        in the same BEGIN IMMEDIATE transaction. Any error rolls the whole
        publish back, so a failed publish can never leave a half-edition that
        shows up in the edition list or downloads. Because the confirmed
        snapshots are copied verbatim, later revisions cannot affect editions
        that were already published.
        """
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                if self._conn.execute(
                    "SELECT 1 FROM jobs WHERE id = ?", (job_id,)
                ).fetchone() is None:
                    raise EditionError("job not found")
                anns = self._conn.execute(
                    "SELECT * FROM announcements WHERE job_id = ? ORDER BY id", (job_id,)
                ).fetchall()
                if not anns:
                    raise EditionError("job has no announcements")
                not_success = [a["id"] for a in anns if a["status"] != "success"]
                if not_success:
                    raise EditionError(
                        "an edition is only available when every announcement succeeded; "
                        f"not successful: {not_success}"
                    )
                unconfirmed = [a["id"] for a in anns if not a["confirmed"]]
                if unconfirmed:
                    raise EditionError(
                        "an edition is only available when every announcement is confirmed; "
                        f"not confirmed: {unconfirmed}"
                    )
                version = self._conn.execute(
                    "SELECT COALESCE(MAX(version), 0) + 1 AS next_version"
                    " FROM editions WHERE job_id = ?",
                    (job_id,),
                ).fetchone()["next_version"]
                created_at = time.time()
                self._conn.execute(
                    "INSERT INTO editions (job_id, version, created_at, document_count)"
                    " VALUES (?, ?, ?, ?)",
                    (job_id, version, created_at, len(anns)),
                )
                for order, a in enumerate(anns):
                    self._conn.execute(
                        "INSERT INTO edition_documents"
                        " (job_id, version, doc_order, ann_id, url, final_url, hops, title, content)"
                        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (job_id, version, order, a["id"], a["url"], a["current_url"],
                         a["hops"], a["confirmed_title"], a["confirmed_content"]),
                    )
                self._conn.execute("COMMIT")
            except Exception:
                try:
                    self._conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise
        return {
            "job_id": job_id,
            "version": version,
            "created_at": created_at,
            "document_count": len(anns),
        }

    # ---- cooldowns ------------------------------------------------------
    def set_cooldown(self, origin: str, until_ts: float) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO cooldowns (origin, until_ts) VALUES (?, ?)"
                " ON CONFLICT(origin) DO UPDATE SET until_ts = excluded.until_ts",
                (origin, until_ts),
            )

    def active_cooldowns(self, now: float):
        with self._lock:
            return self._conn.execute(
                "SELECT origin, until_ts FROM cooldowns WHERE until_ts > ?", (now,)
            ).fetchall()
