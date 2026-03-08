"""
Database layer — SQLite with groups, machines, categories queue, jobs, audit logs.
"""
import sqlite3, json, os
from datetime import datetime
from contextlib import contextmanager

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tracker.db")

def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

@contextmanager
def db():
    conn = get_conn()
    try:
        yield conn
        conn.commit()
    except:
        conn.rollback()
        raise
    finally:
        conn.close()

def init_db():
    with db() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY, value TEXT
        );

        CREATE TABLE IF NOT EXISTS machine_groups (
            group_id    INTEGER PRIMARY KEY AUTOINCREMENT,
            group_name  TEXT UNIQUE NOT NULL,
            description TEXT,
            -- macro config
            excel_file_name TEXT DEFAULT '',
            macro_name      TEXT DEFAULT '',
            target_cell     TEXT DEFAULT 'A1',
            is_active   INTEGER DEFAULT 1,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS machines (
            machine_id    INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id      INTEGER NOT NULL,
            machine_name  TEXT NOT NULL,
            system_name   TEXT DEFAULT '',
            ip_address    TEXT DEFAULT '',
            shared_folder TEXT NOT NULL,
            username      TEXT DEFAULT '',
            password      TEXT DEFAULT '',
            department    TEXT DEFAULT '',
            location      TEXT DEFAULT '',
            is_active     INTEGER DEFAULT 1,
            created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (group_id) REFERENCES machine_groups(group_id) ON DELETE CASCADE,
            UNIQUE(group_id, machine_name)
        );

        CREATE TABLE IF NOT EXISTS group_files (
            file_id       INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id      INTEGER NOT NULL,
            original_name TEXT NOT NULL,
            stored_path   TEXT NOT NULL,
            file_size_kb  REAL DEFAULT 0,
            is_macro_file INTEGER DEFAULT 0,
            uploaded_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (group_id) REFERENCES machine_groups(group_id) ON DELETE CASCADE
        );

        -- Maj categories per group
        CREATE TABLE IF NOT EXISTS categories (
            cat_id    INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id  INTEGER NOT NULL,
            cat_value TEXT NOT NULL,
            sort_order INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1,
            FOREIGN KEY (group_id) REFERENCES machine_groups(group_id) ON DELETE CASCADE,
            UNIQUE(group_id, cat_value)
        );

        -- Jobs
        CREATE TABLE IF NOT EXISTS jobs (
            job_id        INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id      INTEGER NOT NULL,
            job_date      DATE NOT NULL,
            started_at    TIMESTAMP NOT NULL,
            finished_at   TIMESTAMP,
            status        TEXT DEFAULT 'RUNNING',
            total_cats    INTEGER DEFAULT 0,
            completed_cats INTEGER DEFAULT 0,
            failed_cats   INTEGER DEFAULT 0,
            triggered_by  TEXT DEFAULT 'manual',
            FOREIGN KEY (group_id) REFERENCES machine_groups(group_id)
        );

        -- Category queue items
        CREATE TABLE IF NOT EXISTS job_queue (
            queue_id    INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id      INTEGER NOT NULL,
            cat_id      INTEGER NOT NULL,
            cat_value   TEXT NOT NULL,
            machine_id  INTEGER,
            status      TEXT DEFAULT 'QUEUED',
            started_at  TIMESTAMP,
            finished_at TIMESTAMP,
            date_folder TEXT,
            output_files TEXT,
            error_message TEXT,
            duration_secs REAL,
            FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE,
            FOREIGN KEY (cat_id) REFERENCES categories(cat_id),
            FOREIGN KEY (machine_id) REFERENCES machines(machine_id)
        );

        -- Audit logs
        CREATE TABLE IF NOT EXISTS run_logs (
            log_id     INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id     INTEGER,
            queue_id   INTEGER,
            machine_id INTEGER,
            log_level  TEXT DEFAULT 'INFO',
            step       TEXT,
            message    TEXT,
            details    TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Email log
        CREATE TABLE IF NOT EXISTS email_log (
            email_id    INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id      INTEGER,
            machine_name TEXT,
            subject     TEXT,
            recipients  TEXT,
            status      TEXT DEFAULT 'SENT',
            error       TEXT,
            sent_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_queue_job ON job_queue(job_id);
        CREATE INDEX IF NOT EXISTS idx_queue_status ON job_queue(status);
        CREATE INDEX IF NOT EXISTS idx_logs_job ON run_logs(job_id);
        """)

# ── SETTINGS ────────────────────────────────────────────
def get_setting(key, default=""):
    with db() as c:
        r = c.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return r["value"] if r else default

def set_setting(key, val):
    with db() as c:
        c.execute("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, val))

def get_all_settings():
    with db() as c:
        return {r["key"]: r["value"] for r in c.execute("SELECT * FROM settings").fetchall()}

# ── GROUPS ──────────────────────────────────────────────
def create_group(name, desc=""):
    with db() as c:
        return c.execute("INSERT INTO machine_groups(group_name,description) VALUES(?,?)", (name,desc)).lastrowid

def get_groups():
    with db() as c:
        return c.execute("""
            SELECT g.*,
                COUNT(DISTINCT m.machine_id) as machine_count,
                COUNT(DISTINCT f.file_id) as file_count,
                COUNT(DISTINCT cat.cat_id) as cat_count
            FROM machine_groups g
            LEFT JOIN machines m ON g.group_id=m.group_id AND m.is_active=1
            LEFT JOIN group_files f ON g.group_id=f.group_id
            LEFT JOIN categories cat ON g.group_id=cat.group_id AND cat.is_active=1
            GROUP BY g.group_id ORDER BY g.group_name
        """).fetchall()

def get_group(gid):
    with db() as c:
        return c.execute("SELECT * FROM machine_groups WHERE group_id=?", (gid,)).fetchone()

def update_group(gid, **kw):
    allowed = {"group_name","description","excel_file_name","macro_name","target_cell"}
    u = {k:v for k,v in kw.items() if k in allowed}
    if not u: return
    with db() as c:
        clause = ", ".join(f"{k}=?" for k in u)
        c.execute(f"UPDATE machine_groups SET {clause} WHERE group_id=?", list(u.values())+[gid])

def delete_group(gid):
    with db() as c:
        # clean up files on disk
        for r in c.execute("SELECT stored_path FROM group_files WHERE group_id=?", (gid,)).fetchall():
            try:
                if r["stored_path"] and os.path.exists(r["stored_path"]):
                    os.remove(r["stored_path"])
            except: pass

        # 1. Get all job IDs for this group
        jids = [r["job_id"] for r in c.execute("SELECT job_id FROM jobs WHERE group_id=?", (gid,)).fetchall()]
        if jids:
            ph = ",".join("?" * len(jids))
            # Delete job_queue FIRST (has FK to categories + machines)
            c.execute(f"DELETE FROM job_queue WHERE job_id IN ({ph})", jids)
            c.execute(f"DELETE FROM run_logs WHERE job_id IN ({ph})", jids)
            c.execute(f"DELETE FROM email_log WHERE job_id IN ({ph})", jids)
        c.execute("DELETE FROM jobs WHERE group_id=?", (gid,))

        # 2. Now safe to delete categories (no more FK refs from job_queue)
        c.execute("DELETE FROM categories WHERE group_id=?", (gid,))

        # 3. Delete machines
        c.execute("DELETE FROM machines WHERE group_id=?", (gid,))

        # 4. Delete files
        c.execute("DELETE FROM group_files WHERE group_id=?", (gid,))

        # 5. Finally delete the group
        c.execute("DELETE FROM machine_groups WHERE group_id=?", (gid,))

# ── MACHINES ────────────────────────────────────────────
def add_machine(gid, name, system_name, ip, shared_folder, username="", password="", dept="", loc=""):
    with db() as c:
        return c.execute("""INSERT INTO machines(group_id,machine_name,system_name,ip_address,shared_folder,username,password,department,location)
            VALUES(?,?,?,?,?,?,?,?,?)""", (gid,name,system_name,ip,shared_folder,username,password,dept,loc)).lastrowid

def get_machines(gid):
    with db() as c:
        return c.execute("SELECT * FROM machines WHERE group_id=? ORDER BY machine_name", (gid,)).fetchall()

def get_machine(mid):
    with db() as c:
        return c.execute("SELECT * FROM machines WHERE machine_id=?", (mid,)).fetchone()

def delete_machine(mid):
    with db() as c:
        # Clear FK refs in job_queue first
        c.execute("UPDATE job_queue SET machine_id=NULL WHERE machine_id=?", (mid,))
        c.execute("DELETE FROM machines WHERE machine_id=?", (mid,))

def update_machine(mid, **kw):
    allowed = {"machine_name","system_name","ip_address","shared_folder","username","password","department","location"}
    u = {k:v for k,v in kw.items() if k in allowed}
    # Don't overwrite password with empty string (user left field blank = keep existing)
    if "password" in u and not u["password"]:
        del u["password"]
    if not u: return
    with db() as c:
        clause = ", ".join(f"{k}=?" for k in u)
        c.execute(f"UPDATE machines SET {clause} WHERE machine_id=?", list(u.values())+[mid])

def toggle_machine(mid):
    with db() as c:
        c.execute("UPDATE machines SET is_active=1-is_active WHERE machine_id=?", (mid,))

def bulk_import_machines(gid, rows):
    added = 0
    with db() as c:
        for m in rows:
            try:
                c.execute("""INSERT INTO machines(group_id,machine_name,system_name,ip_address,shared_folder,username,password,department,location)
                    VALUES(?,?,?,?,?,?,?,?,?)""",
                    (gid, m.get("name",""), m.get("system_name",""), m.get("ip",""),
                     m.get("shared_folder",""), m.get("username",""), m.get("password",""),
                     m.get("department",""), m.get("location","")))
                added += 1
            except sqlite3.IntegrityError:
                pass
    return added

# ── FILES ───────────────────────────────────────────────
def add_file(gid, orig, stored, size, is_macro=False):
    with db() as c:
        return c.execute("INSERT INTO group_files(group_id,original_name,stored_path,file_size_kb,is_macro_file) VALUES(?,?,?,?,?)",
            (gid, orig, stored, size, 1 if is_macro else 0)).lastrowid

def get_files(gid):
    with db() as c:
        return c.execute("SELECT * FROM group_files WHERE group_id=? ORDER BY uploaded_at", (gid,)).fetchall()

def delete_file(fid):
    with db() as c:
        r = c.execute("SELECT stored_path FROM group_files WHERE file_id=?", (fid,)).fetchone()
        if r and os.path.exists(r["stored_path"]):
            os.remove(r["stored_path"])
        c.execute("DELETE FROM group_files WHERE file_id=?", (fid,))

def set_macro_file(fid):
    with db() as c:
        r = c.execute("SELECT group_id FROM group_files WHERE file_id=?", (fid,)).fetchone()
        if r:
            c.execute("UPDATE group_files SET is_macro_file=0 WHERE group_id=?", (r["group_id"],))
        c.execute("UPDATE group_files SET is_macro_file=1 WHERE file_id=?", (fid,))

# ── CATEGORIES ──────────────────────────────────────────
def add_category(gid, val, sort=0):
    with db() as c:
        try:
            return c.execute("INSERT INTO categories(group_id,cat_value,sort_order) VALUES(?,?,?)", (gid,val,sort)).lastrowid
        except sqlite3.IntegrityError:
            return None

def get_categories(gid):
    with db() as c:
        return c.execute("SELECT * FROM categories WHERE group_id=? AND is_active=1 ORDER BY sort_order,cat_id", (gid,)).fetchall()

def delete_category(cid):
    with db() as c:
        # Clear FK refs in job_queue first
        c.execute("DELETE FROM job_queue WHERE cat_id=?", (cid,))
        c.execute("DELETE FROM categories WHERE cat_id=?", (cid,))

def delete_all_categories(gid):
    """Delete all categories for a group (bulk clear)."""
    with db() as c:
        cat_ids = [r["cat_id"] for r in c.execute("SELECT cat_id FROM categories WHERE group_id=?", (gid,)).fetchall()]
        if cat_ids:
            ph = ",".join("?" * len(cat_ids))
            c.execute(f"DELETE FROM job_queue WHERE cat_id IN ({ph})", cat_ids)
        c.execute("DELETE FROM categories WHERE group_id=?", (gid,))

def bulk_import_categories(gid, values):
    added = 0
    # Common header values to skip
    headers = {"cat_value", "category", "cat", "value", "maj", "majcat", "maj_cat",
               "cat_no", "catno", "category_value", "sr", "sno", "s.no", "sr.no", "no"}
    with db() as c:
        for i, v in enumerate(values):
            v = v.strip()
            if not v: continue
            if v.lower() in headers: continue  # skip header row
            try:
                c.execute("INSERT INTO categories(group_id,cat_value,sort_order) VALUES(?,?,?)", (gid,v,i))
                added += 1
            except sqlite3.IntegrityError:
                pass
    return added

# ── JOBS & QUEUE ────────────────────────────────────────
def create_job(gid, triggered_by="manual"):
    now = datetime.now()
    cats = get_categories(gid)
    with db() as c:
        cur = c.execute("INSERT INTO jobs(group_id,job_date,started_at,total_cats,triggered_by) VALUES(?,?,?,?,?)",
            (gid, now.strftime("%Y-%m-%d"), now.isoformat(), len(cats), triggered_by))
        jid = cur.lastrowid
        for cat in cats:
            c.execute("INSERT INTO job_queue(job_id,cat_id,cat_value) VALUES(?,?,?)",
                (jid, cat["cat_id"], cat["cat_value"]))
        return jid

def get_job(jid):
    with db() as c:
        return c.execute("SELECT j.*,g.group_name,g.excel_file_name,g.macro_name,g.target_cell FROM jobs j JOIN machine_groups g ON j.group_id=g.group_id WHERE j.job_id=?", (jid,)).fetchone()

def get_jobs(limit=50, gid=None):
    with db() as c:
        q = "SELECT j.*,g.group_name FROM jobs j JOIN machine_groups g ON j.group_id=g.group_id"
        p = []
        if gid:
            q += " WHERE j.group_id=?"; p.append(gid)
        q += " ORDER BY j.started_at DESC LIMIT ?"; p.append(limit)
        return c.execute(q, p).fetchall()

def get_queue(jid):
    with db() as c:
        return c.execute("""SELECT q.*, m.machine_name, m.ip_address, m.system_name
            FROM job_queue q LEFT JOIN machines m ON q.machine_id=m.machine_id
            WHERE q.job_id=? ORDER BY q.queue_id""", (jid,)).fetchall()

def claim_next(jid, mid):
    """Atomically claim next QUEUED category for a machine."""
    with db() as c:
        row = c.execute("SELECT queue_id FROM job_queue WHERE job_id=? AND status='QUEUED' ORDER BY queue_id LIMIT 1", (jid,)).fetchone()
        if not row:
            return None
        c.execute("UPDATE job_queue SET status='RUNNING',machine_id=?,started_at=? WHERE queue_id=?",
            (mid, datetime.now().isoformat(), row["queue_id"]))
        return c.execute("SELECT * FROM job_queue WHERE queue_id=?", (row["queue_id"],)).fetchone()

def finish_queue_item(qid, status, **kw):
    allowed = {"finished_at","date_folder","output_files","error_message","duration_secs"}
    u = {k:v for k,v in kw.items() if k in allowed}
    u["status"] = status
    with db() as c:
        clause = ", ".join(f"{k}=?" for k in u)
        c.execute(f"UPDATE job_queue SET {clause} WHERE queue_id=?", list(u.values())+[qid])

def finish_job(jid):
    with db() as c:
        s = c.execute("SELECT SUM(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END) ok, SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) fail FROM job_queue WHERE job_id=?", (jid,)).fetchone()
        st = "COMPLETED" if (s["fail"] or 0)==0 else "COMPLETED_WITH_ERRORS"
        c.execute("UPDATE jobs SET status=?,finished_at=?,completed_cats=?,failed_cats=? WHERE job_id=?",
            (st, datetime.now().isoformat(), s["ok"] or 0, s["fail"] or 0, jid))

# ── LOGS ────────────────────────────────────────────────
def add_log(job_id=None, queue_id=None, machine_id=None, level="INFO", step="", message="", details=""):
    with db() as c:
        c.execute("INSERT INTO run_logs(job_id,queue_id,machine_id,log_level,step,message,details) VALUES(?,?,?,?,?,?,?)",
            (job_id, queue_id, machine_id, level, step, message, details))

def get_logs(job_id=None, limit=500):
    with db() as c:
        if job_id:
            return c.execute("""SELECT l.*, m.machine_name FROM run_logs l
                LEFT JOIN machines m ON l.machine_id=m.machine_id
                WHERE l.job_id=? ORDER BY l.created_at DESC LIMIT ?""", (job_id,limit)).fetchall()
        return c.execute("""SELECT l.*, m.machine_name FROM run_logs l
            LEFT JOIN machines m ON l.machine_id=m.machine_id
            ORDER BY l.created_at DESC LIMIT ?""", (limit,)).fetchall()

def log_email(jid, machine, subject, recipients, status="SENT", error=""):
    with db() as c:
        c.execute("INSERT INTO email_log(job_id,machine_name,subject,recipients,status,error) VALUES(?,?,?,?,?,?)",
            (jid, machine, subject, recipients, status, error))

def get_email_logs(limit=100):
    with db() as c:
        return c.execute("SELECT * FROM email_log ORDER BY sent_at DESC LIMIT ?", (limit,)).fetchall()

def clear_logs(job_id=None):
    """Delete logs. If job_id, only that job's logs. Otherwise all."""
    with db() as c:
        if job_id:
            c.execute("DELETE FROM run_logs WHERE job_id=?", (job_id,))
        else:
            c.execute("DELETE FROM run_logs")

def clear_email_logs():
    with db() as c:
        c.execute("DELETE FROM email_log")

def kill_job_db(jid):
    """Mark a running job as KILLED in DB."""
    with db() as c:
        c.execute("UPDATE job_queue SET status='CANCELLED' WHERE job_id=? AND status='QUEUED'", (jid,))
        c.execute("UPDATE jobs SET status='KILLED',finished_at=? WHERE job_id=? AND status='RUNNING'",
                  (datetime.now().isoformat(), jid))
# ── DASHBOARD ───────────────────────────────────────────
def get_dashboard():
    with db() as c:
        s = {}
        s["groups"] = c.execute("SELECT COUNT(*) c FROM machine_groups WHERE is_active=1").fetchone()["c"]
        s["machines"] = c.execute("SELECT COUNT(*) c FROM machines WHERE is_active=1").fetchone()["c"]
        s["total_jobs"] = c.execute("SELECT COUNT(*) c FROM jobs").fetchone()["c"]
        s["today_jobs"] = c.execute("SELECT COUNT(*) c FROM jobs WHERE job_date=date('now')").fetchone()["c"]
        w = c.execute("SELECT SUM(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END) ok, SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) fail FROM job_queue WHERE started_at>=datetime('now','-7 days')").fetchone()
        s["week_ok"] = w["ok"] or 0
        s["week_fail"] = w["fail"] or 0
        s["recent_jobs"] = c.execute("SELECT j.*,g.group_name FROM jobs j JOIN machine_groups g ON j.group_id=g.group_id ORDER BY j.started_at DESC LIMIT 10").fetchall()
        s["failures"] = c.execute("""SELECT q.*,m.machine_name,m.ip_address,j.job_date,j.group_id
            FROM job_queue q JOIN machines m ON q.machine_id=m.machine_id JOIN jobs j ON q.job_id=j.job_id
            WHERE q.status='FAILED' ORDER BY q.finished_at DESC LIMIT 15""").fetchall()
        return s
