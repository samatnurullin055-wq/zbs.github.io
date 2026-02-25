import json
import sqlite3
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer


DB_PATH = "stogram.db"
HOST = "0.0.0.0"
PORT = 8000
MAX_REQUEST_BYTES = 25 * 1024 * 1024
MAX_TOTAL_STATE_CHARS = 4_000_000
MAX_MEDIA_SRC_CHARS = 700_000
MAX_POSTS = 120
MAX_MEDIA_PER_POST = 6


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def db_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS app_state (
          id INTEGER PRIMARY KEY CHECK (id = 1),
          state_json TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO app_state (id, state_json, updated_at)
        VALUES (1, ?, ?)
        """,
        (json.dumps({"users": [], "posts": [], "follows": [], "notifs": [], "stories": [], "verifyRequests": [], "presence": {}}), utc_now()),
    )
    conn.commit()
    return conn


def get_state(conn):
    row = conn.execute("SELECT state_json, updated_at FROM app_state WHERE id = 1").fetchone()
    if not row:
        return {"users": [], "posts": [], "follows": [], "notifs": [], "stories": [], "verifyRequests": [], "presence": {}}, utc_now()
    return json.loads(row[0]), row[1]


def set_state(conn, state):
    updated_at = utc_now()
    conn.execute(
        "UPDATE app_state SET state_json = ?, updated_at = ? WHERE id = 1",
        (json.dumps(state, ensure_ascii=False), updated_at),
    )
    conn.commit()
    return updated_at


def sanitize_state(data):
    if not isinstance(data, dict):
        return None
    out = {}
    for key in ("users", "posts", "follows", "notifs", "stories"):
        value = data.get(key, [])
        out[key] = value if isinstance(value, list) else []
    verify_requests = data.get("verifyRequests", [])
    out["verifyRequests"] = verify_requests if isinstance(verify_requests, list) else []
    presence = data.get("presence", {})
    out["presence"] = presence if isinstance(presence, dict) else {}
    return out


def _safe_comments(comments):
    if not isinstance(comments, list):
        return []
    out = []
    for c in comments:
        if not isinstance(c, dict):
            continue
        out.append(
            {
                "id": c.get("id"),
                "userId": c.get("userId"),
                "text": (c.get("text") or "")[:500],
                "createdAt": c.get("createdAt"),
                "likes": c.get("likes") if isinstance(c.get("likes"), list) else [],
                "replies": _safe_comments(c.get("replies")),
            }
        )
    return out


def compact_state(state):
    # Keep structure stable and prune large payloads (mostly base64 media).
    users = state.get("users", []) if isinstance(state.get("users"), list) else []
    follows = state.get("follows", []) if isinstance(state.get("follows"), list) else []
    notifs = state.get("notifs", []) if isinstance(state.get("notifs"), list) else []
    stories = state.get("stories", []) if isinstance(state.get("stories"), list) else []
    verify_requests = state.get("verifyRequests", []) if isinstance(state.get("verifyRequests"), list) else []
    presence = state.get("presence", {}) if isinstance(state.get("presence"), dict) else {}
    posts = state.get("posts", []) if isinstance(state.get("posts"), list) else []

    cleaned_posts = []
    for p in posts:
        if not isinstance(p, dict):
            continue
        media = p.get("media", [])
        if not isinstance(media, list):
            media = []
        cleaned_media = []
        for m in media[:MAX_MEDIA_PER_POST]:
            if not isinstance(m, dict):
                continue
            src = m.get("src") or ""
            if len(src) > MAX_MEDIA_SRC_CHARS:
                src = ""
            cleaned_media.append(
                {
                    "id": m.get("id"),
                    "type": "video" if m.get("type") == "video" else "image",
                    "src": src,
                }
            )
        cleaned_posts.append(
            {
                "id": p.get("id"),
                "userId": p.get("userId"),
                "caption": (p.get("caption") or "")[:4000],
                "createdAt": p.get("createdAt"),
                "media": cleaned_media,
                "likes": p.get("likes") if isinstance(p.get("likes"), list) else [],
                "comments": _safe_comments(p.get("comments")),
                "savedBy": p.get("savedBy") if isinstance(p.get("savedBy"), list) else [],
                "reposts": p.get("reposts") if isinstance(p.get("reposts"), list) else [],
                "views": int(p.get("views") or 0),
                "viewers": (p.get("viewers") if isinstance(p.get("viewers"), list) else [])[-800:],
            }
        )

    cleaned_posts = cleaned_posts[-MAX_POSTS:]
    cleaned_verify = []
    for r in verify_requests[-600:]:
        if not isinstance(r, dict):
            continue
        attachments = r.get("attachments", [])
        if not isinstance(attachments, list):
            attachments = []
        safe_attachments = []
        for a in attachments[:12]:
            if not isinstance(a, dict):
                continue
            src = a.get("src") or ""
            if len(src) > MAX_MEDIA_SRC_CHARS:
                src = ""
            safe_attachments.append(
                {
                    "name": (a.get("name") or "")[:140],
                    "size": int(a.get("size") or 0),
                    "type": (a.get("type") or "")[:80],
                    "src": src,
                }
            )
        cleaned_verify.append(
            {
                "id": r.get("id"),
                "userId": r.get("userId"),
                "text": (r.get("text") or "")[:2400],
                "attachments": safe_attachments,
                "status": (r.get("status") or "pending")[:20],
                "createdAt": r.get("createdAt"),
                "handledAt": r.get("handledAt"),
                "handledBy": r.get("handledBy"),
                "note": (r.get("note") or "")[:500],
            }
        )
    cleaned_presence = {}
    for uid, ts in presence.items():
        if not isinstance(uid, str):
            continue
        if isinstance(ts, (int, float)):
            cleaned_presence[uid] = int(ts)

    compacted = {
        "users": users,
        "posts": cleaned_posts,
        "follows": follows,
        "notifs": notifs,
        "stories": stories,
        "verifyRequests": cleaned_verify,
        "presence": cleaned_presence,
    }

    def size_chars(obj):
        return len(json.dumps(obj, ensure_ascii=False))

    # Hard cap: first strip media from oldest posts, then drop oldest posts.
    while size_chars(compacted) > MAX_TOTAL_STATE_CHARS and compacted["posts"]:
        target = compacted["posts"][0]
        if target.get("media"):
            target["media"] = []
            continue
        compacted["posts"].pop(0)

    return compacted


class Handler(BaseHTTPRequestHandler):
    def _set_headers(self, status=200):
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_OPTIONS(self):
        self._set_headers(204)

    def do_GET(self):
        if self.path == "/api/health":
            self._set_headers(200)
            self.wfile.write(json.dumps({"ok": True, "service": "stogram-db"}).encode("utf-8"))
            return

        if self.path == "/api/state":
            conn = db_conn()
            state, updated_at = get_state(conn)
            state = compact_state(state)
            set_state(conn, state)
            conn.close()
            self._set_headers(200)
            try:
                self.wfile.write(json.dumps({"ok": True, "updated_at": updated_at, "state": state}).encode("utf-8"))
            except (ConnectionAbortedError, BrokenPipeError):
                return
            return

        self._set_headers(404)
        self.wfile.write(json.dumps({"ok": False, "error": "Not found"}).encode("utf-8"))

    def do_POST(self):
        if self.path != "/api/state":
            self._set_headers(404)
            self.wfile.write(json.dumps({"ok": False, "error": "Not found"}).encode("utf-8"))
            return

        length = int(self.headers.get("Content-Length", "0"))
        if length > MAX_REQUEST_BYTES:
            self._set_headers(413)
            self.wfile.write(json.dumps({"ok": False, "error": "Payload too large"}).encode("utf-8"))
            return
        raw = self.rfile.read(length) if length > 0 else b"{}"
        try:
            data = json.loads(raw.decode("utf-8"))
        except Exception:
            self._set_headers(400)
            self.wfile.write(json.dumps({"ok": False, "error": "Invalid JSON"}).encode("utf-8"))
            return

        state = sanitize_state(data.get("state"))
        if state is None:
            self._set_headers(400)
            self.wfile.write(json.dumps({"ok": False, "error": "Invalid state payload"}).encode("utf-8"))
            return

        state = compact_state(state)
        conn = db_conn()
        updated_at = set_state(conn, state)
        conn.close()
        self._set_headers(200)
        self.wfile.write(json.dumps({"ok": True, "updated_at": updated_at}).encode("utf-8"))


if __name__ == "__main__":
    httpd = HTTPServer((HOST, PORT), Handler)
    print(f"StoGram DB API running on http://{HOST}:{PORT}")
    httpd.serve_forever()
