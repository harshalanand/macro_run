"""
Database layer -- SQLite with groups, machines, categories queue, jobs, audit logs.
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
            remote_path   TEXT DEFAULT '',
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

        -- Master machine registry (global, not per-group)
        CREATE TABLE IF NOT EXISTS machine_master (
            master_id       INTEGER PRIMARY KEY AUTOINCREMENT,
            machine_name    TEXT NOT NULL UNIQUE,
            system_name     TEXT DEFAULT '',
            ip_address      TEXT DEFAULT '',
            shared_folder   TEXT DEFAULT '',
            remote_path     TEXT DEFAULT '',
            username        TEXT DEFAULT '',
            password        TEXT DEFAULT '',
            department      TEXT DEFAULT '',
            location        TEXT DEFAULT '',
            tags            TEXT DEFAULT '',
            assigned_group_id INTEGER DEFAULT NULL,
            health_status   TEXT DEFAULT 'UNKNOWN',
            health_checked_at TIMESTAMP,
            health_detail   TEXT DEFAULT '',
            is_active       INTEGER DEFAULT 1,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        # Migrations
        try:
            c.execute("SELECT remote_path FROM machines LIMIT 1")
        except:
            c.execute("ALTER TABLE machines ADD COLUMN remote_path TEXT DEFAULT ''")
        try:
            c.execute("SELECT assigned_group_id FROM machine_master LIMIT 1")
        except:
            c.execute("ALTER TABLE machine_master ADD COLUMN assigned_group_id INTEGER DEFAULT NULL")



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

# ── MACHINE MASTER ───────────────────────────────────────
def get_master_machines(tag=None):
    with db() as c:
        base = """SELECT mm.*, g.group_name as assigned_group_name
                  FROM machine_master mm
                  LEFT JOIN machine_groups g ON mm.assigned_group_id=g.group_id"""
        if tag:
            return c.execute(base + " WHERE mm.tags LIKE ? ORDER BY mm.machine_name",
                             (f"%{tag}%",)).fetchall()
        return c.execute(base + " ORDER BY mm.machine_name").fetchall()

def get_master_machines_unassigned():
    """Return master machines not yet assigned to any group."""
    with db() as c:
        return c.execute("""SELECT mm.*, g.group_name as assigned_group_name
                            FROM machine_master mm
                            LEFT JOIN machine_groups g ON mm.assigned_group_id=g.group_id
                            WHERE mm.assigned_group_id IS NULL
                            ORDER BY mm.machine_name""").fetchall()

def get_master_machine(mid):
    with db() as c:
        return c.execute("""SELECT mm.*, g.group_name as assigned_group_name
                            FROM machine_master mm
                            LEFT JOIN machine_groups g ON mm.assigned_group_id=g.group_id
                            WHERE mm.master_id=?""", (mid,)).fetchone()

def ip_exists_in_master(ip, exclude_mid=None):
    """Check if IP already registered in master (for uniqueness validation)."""
    if not ip or not ip.strip():
        return False
    with db() as c:
        if exclude_mid:
            r = c.execute("SELECT master_id FROM machine_master WHERE ip_address=? AND master_id!=? AND ip_address!=''",
                          (ip.strip(), exclude_mid)).fetchone()
        else:
            r = c.execute("SELECT master_id FROM machine_master WHERE ip_address=? AND ip_address!=''",
                          (ip.strip(),)).fetchone()
        return r is not None

def create_master_machine(name, system_name, ip, shared_folder, remote_path,
                          username, password, department, location, tags):
    with db() as c:
        return c.execute("""INSERT INTO machine_master
            (machine_name,system_name,ip_address,shared_folder,remote_path,
             username,password,department,location,tags)
            VALUES(?,?,?,?,?,?,?,?,?,?)""",
            (name, system_name, ip, shared_folder, remote_path,
             username, password, department, location, tags)).lastrowid

def update_master_machine(mid, **kw):
    allowed = {"machine_name","system_name","ip_address","shared_folder","remote_path",
               "username","password","department","location","tags","is_active",
               "assigned_group_id"}
    u = {k:v for k,v in kw.items() if k in allowed and v is not None}
    if not u: return
    with db() as c:
        clause = ", ".join(f"{k}=?" for k in u)
        c.execute(f"UPDATE machine_master SET {clause} WHERE master_id=?", list(u.values())+[mid])

def delete_master_machine(mid):
    with db() as c:
        m = c.execute("SELECT machine_name, assigned_group_id FROM machine_master WHERE master_id=?", (mid,)).fetchone()
        if m and m["assigned_group_id"]:
            # Null FK refs in job_queue before removing from group machines
            c.execute("""UPDATE job_queue SET machine_id=NULL
                         WHERE machine_id IN (
                             SELECT machine_id FROM machines
                             WHERE machine_name=? AND group_id=?
                         )""", (m["machine_name"], m["assigned_group_id"]))
            c.execute("DELETE FROM machines WHERE machine_name=? AND group_id=?",
                      (m["machine_name"], m["assigned_group_id"]))
        c.execute("DELETE FROM machine_master WHERE master_id=?", (mid,))

def update_master_health(mid, status, detail=""):
    with db() as c:
        c.execute("""UPDATE machine_master SET health_status=?,health_checked_at=?,health_detail=?
                     WHERE master_id=?""",
                  (status, datetime.now().isoformat(), detail[:1000], mid))

def sync_health_to_master(machine_name, status, detail=""):
    """Sync health status from a group machine test back to master record."""
    with db() as c:
        c.execute("""UPDATE machine_master SET health_status=?,health_checked_at=?,health_detail=?
                     WHERE machine_name=?""",
                  (status, datetime.now().isoformat(), detail[:1000], machine_name))

def get_master_tags():
    """Return sorted unique tag list across all master machines."""
    with db() as c:
        rows = c.execute("SELECT tags FROM machine_master WHERE tags!=''").fetchall()
    tags = set()
    for r in rows:
        for t in r["tags"].split(","):
            t = t.strip()
            if t:
                tags.add(t)
    return sorted(tags)

def add_master_to_group(gid, master_id):
    """Copy a master machine record into a group's machines table and mark it assigned."""
    m = get_master_machine(master_id)
    if not m:
        return None
    with db() as c:
        exists = c.execute(
            "SELECT machine_id FROM machines WHERE group_id=? AND machine_name=?",
            (gid, m["machine_name"])).fetchone()
        if exists:
            # Still mark assigned in master
            c.execute("UPDATE machine_master SET assigned_group_id=? WHERE master_id=?",
                      (gid, master_id))
            return exists["machine_id"]
        mid = c.execute("""INSERT INTO machines
            (group_id,machine_name,system_name,ip_address,shared_folder,remote_path,
             username,password,department,location)
            VALUES(?,?,?,?,?,?,?,?,?,?)""",
            (gid, m["machine_name"], m["system_name"], m["ip_address"],
             m["shared_folder"], m["remote_path"], m["username"], m["password"],
             m["department"], m["location"])).lastrowid
        # Tag master record as assigned to this group
        c.execute("UPDATE machine_master SET assigned_group_id=? WHERE master_id=?",
                  (gid, master_id))
        return mid

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
        # 0. Untag all master machines assigned to this group
        c.execute("UPDATE machine_master SET assigned_group_id=NULL WHERE assigned_group_id=?", (gid,))

        # 1. Clean up uploaded files from disk
        for r in c.execute("SELECT stored_path FROM group_files WHERE group_id=?", (gid,)).fetchall():
            try:
                if r["stored_path"] and os.path.exists(r["stored_path"]):
                    os.remove(r["stored_path"])
            except: pass

        # 2. Delete job queue + logs for all jobs in this group
        jids = [r["job_id"] for r in c.execute("SELECT job_id FROM jobs WHERE group_id=?", (gid,)).fetchall()]
        if jids:
            ph = ",".join("?" * len(jids))
            c.execute(f"DELETE FROM job_queue WHERE job_id IN ({ph})", jids)
            c.execute(f"DELETE FROM run_logs WHERE job_id IN ({ph})", jids)
            c.execute(f"DELETE FROM email_log WHERE job_id IN ({ph})", jids)
        c.execute("DELETE FROM jobs WHERE group_id=?", (gid,))

        # 3. Categories
        c.execute("DELETE FROM categories WHERE group_id=?", (gid,))

        # 4. Machines (nullify FK refs first to avoid constraint issues)
        c.execute("UPDATE job_queue SET machine_id=NULL WHERE machine_id IN (SELECT machine_id FROM machines WHERE group_id=?)", (gid,))
        c.execute("DELETE FROM machines WHERE group_id=?", (gid,))

        # 5. Files
        c.execute("DELETE FROM group_files WHERE group_id=?", (gid,))

        # 6. Group itself
        c.execute("DELETE FROM machine_groups WHERE group_id=?", (gid,))

# ── MACHINES ────────────────────────────────────────────
def add_machine(gid, name, system_name, ip, shared_folder, remote_path="", username="", password="", dept="", loc=""):
    with db() as c:
        return c.execute("""INSERT INTO machines(group_id,machine_name,system_name,ip_address,shared_folder,remote_path,username,password,department,location)
            VALUES(?,?,?,?,?,?,?,?,?,?)""", (gid,name,system_name,ip,shared_folder,remote_path,username,password,dept,loc)).lastrowid

def get_machines(gid):
    with db() as c:
        return c.execute("SELECT * FROM machines WHERE group_id=? ORDER BY machine_name", (gid,)).fetchall()

def get_machine(mid):
    with db() as c:
        return c.execute("SELECT * FROM machines WHERE machine_id=?", (mid,)).fetchone()

def delete_machine(mid):
    with db() as c:
        m = c.execute("SELECT machine_name, group_id FROM machines WHERE machine_id=?", (mid,)).fetchone()
        # Null FK refs in job_queue before deleting the machine row
        c.execute("UPDATE job_queue SET machine_id=NULL WHERE machine_id=?", (mid,))
        c.execute("DELETE FROM machines WHERE machine_id=?", (mid,))
        # Untag the master record so it becomes available again
        if m:
            c.execute("""UPDATE machine_master SET assigned_group_id=NULL
                         WHERE machine_name=? AND assigned_group_id=?""",
                      (m["machine_name"], m["group_id"]))

def get_running_job_ids_for_group(gid):
    """Return list of job_ids that have status=RUNNING for this group."""
    with db() as c:
        rows = c.execute(
            "SELECT job_id FROM jobs WHERE group_id=? AND status='RUNNING'", (gid,)
        ).fetchall()
        return [r["job_id"] for r in rows]

def group_has_active_job(gid):
    """Return True if any job for this group is currently RUNNING in the DB."""
    with db() as c:
        r = c.execute("SELECT COUNT(*) c FROM jobs WHERE group_id=? AND status='RUNNING'", (gid,)).fetchone()
        return (r["c"] or 0) > 0

def fix_stale_running_jobs(running_job_ids):
    """Mark DB-RUNNING jobs that are no longer in-memory as KILLED (stale from server restart)."""
    with db() as c:
        rows = c.execute("SELECT job_id FROM jobs WHERE status='RUNNING'").fetchall()
        for row in rows:
            jid = row["job_id"]
            if jid not in running_job_ids:
                c.execute(
                    "UPDATE jobs SET status='KILLED', finished_at=? WHERE job_id=? AND status='RUNNING'",
                    (datetime.now().isoformat(), jid)
                )
                c.execute(
                    "UPDATE job_queue SET status='CANCELLED' WHERE job_id=? AND status IN ('QUEUED','RUNNING')",
                    (jid,)
                )

def untag_master_from_group(master_id):
    """Remove group assignment from master AND delete the machine from the group's machines table."""
    with db() as c:
        m = c.execute("SELECT machine_name, assigned_group_id FROM machine_master WHERE master_id=?",
                      (master_id,)).fetchone()
        if m and m["assigned_group_id"]:
            # Null out FK references in job_queue before deleting the machine row
            c.execute("""UPDATE job_queue SET machine_id=NULL
                         WHERE machine_id IN (
                             SELECT machine_id FROM machines
                             WHERE machine_name=? AND group_id=?
                         )""", (m["machine_name"], m["assigned_group_id"]))
            c.execute("DELETE FROM machines WHERE machine_name=? AND group_id=?",
                      (m["machine_name"], m["assigned_group_id"]))
        c.execute("UPDATE machine_master SET assigned_group_id=NULL WHERE master_id=?", (master_id,))

def bulk_untag_machines(machine_ids):
    """Untag multiple machines from their groups (by machines.machine_id list)."""
    with db() as c:
        for mid in machine_ids:
            row = c.execute("SELECT machine_name, group_id FROM machines WHERE machine_id=?", (mid,)).fetchone()
            if not row:
                continue
            # Null FK refs in job_queue before deleting
            c.execute("UPDATE job_queue SET machine_id=NULL WHERE machine_id=?", (mid,))
            c.execute("DELETE FROM machines WHERE machine_id=?", (mid,))
            c.execute("""UPDATE machine_master SET assigned_group_id=NULL
                         WHERE machine_name=? AND assigned_group_id=?""",
                      (row["machine_name"], row["group_id"]))

def toggle_machine(mid):
    with db() as c:
        c.execute("UPDATE machines SET is_active=1-is_active WHERE machine_id=?", (mid,))

def update_machine(mid, **kw):
    allowed = {"machine_name","system_name","ip_address","shared_folder","remote_path","username","password","department","location"}
    u = {k:v for k,v in kw.items() if k in allowed}
    # Don't overwrite password with empty string (user left field blank = keep existing)
    if "password" in u and not u["password"]:
        del u["password"]
    if not u: return
    with db() as c:
        clause = ", ".join(f"{k}=?" for k in u)
        c.execute(f"UPDATE machines SET {clause} WHERE machine_id=?", list(u.values())+[mid])

def bulk_import_machines(gid, rows):
    added = 0
    with db() as c:
        for m in rows:
            try:
                c.execute("""INSERT INTO machines(group_id,machine_name,system_name,ip_address,shared_folder,remote_path,username,password,department,location)
                    VALUES(?,?,?,?,?,?,?,?,?,?)""",
                    (gid, m.get("name",""), m.get("system_name",""), m.get("ip",""),
                     m.get("shared_folder",""), m.get("remote_path",""),
                     m.get("username",""), m.get("password",""),
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
        c.execute("UPDATE job_queue SET cat_id=NULL WHERE cat_id=?", (cid,))
        c.execute("DELETE FROM categories WHERE cat_id=?", (cid,))

def delete_all_categories(gid):
    with db() as c:
        c.execute("""UPDATE job_queue SET cat_id=NULL
                     WHERE cat_id IN (SELECT cat_id FROM categories WHERE group_id=?)""", (gid,))
        c.execute("DELETE FROM categories WHERE group_id=?", (gid,))

def bulk_import_categories(gid, values):
    """Insert categories, skipping blank lines and common header names."""
    SKIP_HEADERS = {"cat_value", "category", "maj_no", "maj no", "value",
                    "cat_no", "catvalue", "categories", "header"}
    added = 0
    with db() as c:
        for i, v in enumerate(values):
            v = v.strip()
            if not v:
                continue
            # Skip if this looks like a header row
            if v.lower() in SKIP_HEADERS:
                continue
            try:
                c.execute("INSERT INTO categories(group_id,cat_value,sort_order) VALUES(?,?,?)", (gid, v, i))
                added += 1
            except sqlite3.IntegrityError:
                pass  # duplicate — skip silently
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
    """Atomically claim the next QUEUED category for a machine.

    Uses BEGIN IMMEDIATE to get an exclusive write lock BEFORE selecting,
    so concurrent machine threads cannot select the same row simultaneously.
    Without this, 3 machines all SELECT the same first QUEUED row, then
    all UPDATE it — resulting in the same category running on all machines.
    """
    import time, random
    for _attempt in range(10):          # retry up to 10x on db busy
        try:
            conn = get_conn()
            try:
                conn.execute("BEGIN IMMEDIATE")   # exclusive write lock
                row = conn.execute(
                    "SELECT queue_id FROM job_queue "
                    "WHERE job_id=? AND status='QUEUED' "
                    "ORDER BY queue_id LIMIT 1",
                    (jid,)
                ).fetchone()
                if not row:
                    conn.commit()
                    return None
                conn.execute(
                    "UPDATE job_queue SET status='RUNNING', machine_id=?, started_at=? "
                    "WHERE queue_id=? AND status='QUEUED'",
                    (mid, datetime.now().isoformat(), row["queue_id"])
                )
                if conn.total_changes == 0:
                    # Another thread claimed it between our SELECT and UPDATE — retry
                    conn.rollback()
                    time.sleep(random.uniform(0.05, 0.15))
                    continue
                result = conn.execute(
                    "SELECT * FROM job_queue WHERE queue_id=?",
                    (row["queue_id"],)
                ).fetchone()
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()
        except Exception as e:
            err = str(e).lower()
            if "locked" in err or "busy" in err:
                time.sleep(random.uniform(0.1, 0.3))
                continue
            raise
    return None   # exhausted retries

def finish_queue_item(qid, status, **kw):
    allowed = {"finished_at","date_folder","output_files","error_message","duration_secs"}
    u = {k:v for k,v in kw.items() if k in allowed}
    u["status"] = status
    with db() as c:
        clause = ", ".join(f"{k}=?" for k in u)
        c.execute(f"UPDATE job_queue SET {clause} WHERE queue_id=?", list(u.values())+[qid])

def finish_job(jid):
    with db() as c:
        s = c.execute("""SELECT 
            SUM(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END) ok, 
            SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) fail,
            SUM(CASE WHEN status IN ('QUEUED','CANCELLED') THEN 1 ELSE 0 END) unrun
            FROM job_queue WHERE job_id=?""", (jid,)).fetchone()
        ok = s["ok"] or 0
        fail = s["fail"] or 0
        unrun = s["unrun"] or 0
        if ok == 0 and fail == 0:
            st = "FAILED"  # Nothing ran at all (prep failed)
        elif fail > 0 or unrun > 0:
            st = "COMPLETED_WITH_ERRORS"
        else:
            st = "COMPLETED"
        c.execute("UPDATE jobs SET status=?,finished_at=?,completed_cats=?,failed_cats=? WHERE job_id=?",
            (st, datetime.now().isoformat(), ok, fail, jid))

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

def delete_job(jid):
    """Permanently delete a job and all its queue items and logs from DB."""
    with db() as c:
        c.execute("DELETE FROM run_logs WHERE job_id=?", (jid,))
        c.execute("DELETE FROM email_log WHERE job_id=?", (jid,))
        c.execute("DELETE FROM job_queue WHERE job_id=?", (jid,))
        c.execute("DELETE FROM jobs WHERE job_id=?", (jid,))
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
