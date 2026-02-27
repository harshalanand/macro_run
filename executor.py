"""
Queue-based executor:
- N machines process M categories (M can be > N)
- Each machine: copy files → paste category into cell → run macro → collect output
- When a machine finishes, it picks the next queued category
- Parallel file copy via ThreadPoolExecutor
- Network auth via 'net use' if credentials provided
"""
import os, json, shutil, subprocess, threading, time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import database as D
import notifier as N

COMPILED_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "compiled_output")
_running_jobs = {}

def is_running(jid): return _running_jobs.get(jid, False)

def run_async(jid):
    _running_jobs[jid] = True
    threading.Thread(target=_run_job, args=(jid,), daemon=True).start()


def _run_job(jid):
    try:
        job = D.get_job(jid)
        if not job: return
        gid = job["group_id"]
        group_name = job["group_name"]
        excel_file = job["excel_file_name"]
        macro_name = job["macro_name"]
        target_cell = job["target_cell"] or "A1"
        today = datetime.now().strftime("%Y-%m-%d")
        compile_dir = D.get_setting("compile_path", COMPILED_DIR)

        files = D.get_files(gid)
        machines = [m for m in D.get_machines(gid) if m["is_active"]]
        queue_items = D.get_queue(jid)

        if not machines:
            D.add_log(jid, level="ERROR", step="INIT", message="No active machines")
            D.finish_job(jid)
            return
        if not queue_items:
            D.add_log(jid, level="ERROR", step="INIT", message="No categories queued")
            D.finish_job(jid)
            return

        D.add_log(jid, level="INFO", step="START",
                   message=f"Job #{jid}: {len(queue_items)} categories across {len(machines)} machines")

        # Pre-authenticate and prep all machines in parallel
        machine_ready = {}
        with ThreadPoolExecutor(max_workers=min(len(machines), 8)) as pool:
            futs = {pool.submit(_prep_machine, m, today, files, jid): m for m in machines}
            for fut in as_completed(futs):
                m = futs[fut]
                try:
                    result = fut.result()
                    machine_ready[m["machine_id"]] = result
                    D.add_log(jid, machine_id=m["machine_id"], level="INFO", step="PREP_OK",
                              message=f"{m['machine_name']}: files copied, ready")
                except Exception as e:
                    D.add_log(jid, machine_id=m["machine_id"], level="ERROR", step="PREP_FAIL",
                              message=f"{m['machine_name']}: {e}")
                    N.notify_copy_failure(jid, None, m["machine_name"],
                                          m["ip_address"], m["shared_folder"], str(e), "PREP")

        if not machine_ready:
            D.add_log(jid, level="ERROR", step="ABORT", message="No machines available after prep")
            D.finish_job(jid)
            return

        # Worker function: each machine thread keeps claiming categories
        def machine_worker(mid, date_folder):
            m = D.get_machine(mid)
            mname = m["machine_name"]
            while True:
                item = D.claim_next(jid, mid)
                if not item:
                    break  # no more categories
                qid = item["queue_id"]
                cat = item["cat_value"]
                start = datetime.now()
                D.add_log(jid, qid, mid, "INFO", "CAT_START",
                          f"{mname}: processing category '{cat}'")
                try:
                    # 1. Paste category value into target cell of the macro file
                    excel_path = os.path.join(date_folder, excel_file)
                    D.add_log(jid, qid, mid, "INFO", "MACRO_PREP",
                              f"{mname}: opening '{excel_path}' cell={target_cell} value='{cat}'")

                    if not os.path.exists(excel_path):
                        raise FileNotFoundError(f"Macro file missing: {excel_path}")

                    _paste_and_run(excel_path, macro_name, target_cell, cat)

                    D.add_log(jid, qid, mid, "INFO", "MACRO_DONE",
                              f"{mname}: macro done for '{cat}'")

                    # 2. Collect output
                    out_dir = os.path.join(compile_dir, today, group_name, mname, cat)
                    _collect_output(date_folder, files, out_dir, jid, qid, mid)

                    elapsed = (datetime.now() - start).total_seconds()
                    D.finish_queue_item(qid, "SUCCESS",
                        finished_at=datetime.now().isoformat(),
                        date_folder=date_folder, duration_secs=elapsed)
                    D.add_log(jid, qid, mid, "INFO", "CAT_DONE",
                              f"{mname}: '{cat}' completed in {elapsed:.1f}s")

                except Exception as e:
                    elapsed = (datetime.now() - start).total_seconds()
                    D.finish_queue_item(qid, "FAILED",
                        finished_at=datetime.now().isoformat(),
                        error_message=str(e)[:500], duration_secs=elapsed)
                    D.add_log(jid, qid, mid, "ERROR", "CAT_FAIL",
                              f"{mname}: '{cat}' failed: {e}")
                    N.notify_macro_failure(jid, qid, mname,
                        m["ip_address"], excel_file, macro_name, str(e), cat)

        # Launch worker threads per machine
        workers = []
        with ThreadPoolExecutor(max_workers=len(machine_ready)) as pool:
            for mid, date_folder in machine_ready.items():
                workers.append(pool.submit(machine_worker, mid, date_folder))
            for w in as_completed(workers):
                try:
                    w.result()
                except Exception as e:
                    D.add_log(jid, level="ERROR", step="WORKER_CRASH", message=str(e))

        D.finish_job(jid)
        D.add_log(jid, level="INFO", step="JOB_DONE", message=f"Job #{jid} completed")

        # Summary email
        job = D.get_job(jid)
        if (job["failed_cats"] or 0) > 0:
            fails = []
            for q in D.get_queue(jid):
                if q["status"] == "FAILED":
                    fails.append({"machine": q["machine_name"] or "?", "ip": q["ip_address"] or "",
                                  "step": f"CAT:{q['cat_value']}", "error": q["error_message"] or ""})
            N.notify_job_summary(jid, group_name, job["total_cats"],
                                 job["completed_cats"], job["failed_cats"], fails)

    except Exception as e:
        D.add_log(jid, level="ERROR", step="JOB_CRASH", message=str(e))
        with D.db() as c:
            c.execute("UPDATE jobs SET status='CRASHED',finished_at=? WHERE job_id=?",
                      (datetime.now().isoformat(), jid))
    finally:
        _running_jobs.pop(jid, None)


def _prep_machine(machine, today, files, jid):
    """
    Prepare a machine: authenticate if needed, create date folder, copy ALL files in parallel.
    Returns the date_folder path.
    """
    shared = machine["shared_folder"]
    system_name = machine["system_name"] or ""
    username = machine["username"] or ""
    password = machine["password"] or ""

    # Build UNC path from system_name if shared_folder starts with \\
    # If not a UNC path, use as-is (local path)
    target_path = shared

    # Network auth if credentials provided and UNC path
    if target_path.startswith("\\\\") and username and password:
        _net_use(target_path, username, password, jid, machine["machine_id"])

    # Verify access
    date_folder = os.path.join(target_path, today)
    try:
        os.makedirs(date_folder, exist_ok=True)
    except PermissionError:
        # Retry with net use if system_name available
        if system_name and username and password:
            unc = f"\\\\{system_name}"
            _net_use(unc, username, password, jid, machine["machine_id"])
            os.makedirs(date_folder, exist_ok=True)
        else:
            raise

    # Copy all files in parallel
    def copy_one(f):
        src = f["stored_path"]
        dst = os.path.join(date_folder, f["original_name"])
        shutil.copy2(src, dst)
        return f["original_name"]

    with ThreadPoolExecutor(max_workers=min(len(files), 4)) as pool:
        futs = [pool.submit(copy_one, f) for f in files]
        for fut in as_completed(futs):
            fut.result()  # raise if error

    D.add_log(jid, machine_id=machine["machine_id"], level="INFO", step="FILES_COPIED",
              message=f"Copied {len(files)} files to {date_folder}")

    return date_folder


def _net_use(unc_path, username, password, jid=None, mid=None):
    """Authenticate to a network share using 'net use'."""
    # Extract server part if full path
    parts = unc_path.replace("/", "\\").split("\\")
    # \\server\share → take first share level
    server_share = "\\\\".join([""] + [p for p in parts if p][:2]) if len(parts) > 3 else unc_path

    try:
        # Disconnect first (ignore errors)
        subprocess.run(["net", "use", server_share, "/delete", "/y"],
                       capture_output=True, timeout=10)
    except:
        pass

    result = subprocess.run(
        ["net", "use", server_share, f"/user:{username}", password, "/persistent:no"],
        capture_output=True, text=True, timeout=15
    )
    if result.returncode != 0:
        err = result.stderr.strip() or result.stdout.strip()
        D.add_log(jid, machine_id=mid, level="WARN", step="NET_USE",
                  message=f"net use failed: {err}")
        # Don't raise — folder might still be accessible
    else:
        D.add_log(jid, machine_id=mid, level="INFO", step="NET_USE",
                  message=f"Authenticated to {server_share}")


def _paste_and_run(excel_path, macro_name, target_cell, cat_value, timeout=600):
    """Open Excel, paste category value into target cell, run macro, save and close."""
    import tempfile

    # DON'T use os.path.abspath — it mangles UNC paths like \\server\share
    # Keep the path exactly as constructed from shared_folder + date + filename
    # Replace any forward slashes just in case
    excel_path = excel_path.replace("/", "\\")

    # Write VBS to LOCAL temp dir — cscript can't always run scripts from network shares
    vbs_dir = tempfile.gettempdir()
    vbs_path = os.path.join(vbs_dir, f"_macro_{os.getpid()}_{id(cat_value)}.vbs")

    # Escape single quotes in cat_value for VBS string safety
    safe_cat = str(cat_value).replace('"', '""')

    vbs = f'''
Dim xlApp, xlWb, xlWs
Dim filePath
filePath = "{excel_path}"

WScript.Echo "OPENING: " & filePath

' Check if file exists
Dim fso
Set fso = CreateObject("Scripting.FileSystemObject")
If Not fso.FileExists(filePath) Then
    WScript.Echo "ERROR_OPEN:File not found: " & filePath
    WScript.Quit 1
End If
Set fso = Nothing

On Error Resume Next
Set xlApp = CreateObject("Excel.Application")
If Err.Number <> 0 Then
    WScript.Echo "ERROR_OPEN:Cannot create Excel: " & Err.Description
    WScript.Quit 1
End If

xlApp.Visible = False
xlApp.DisplayAlerts = False
xlApp.AskToUpdateLinks = False
Err.Clear

Set xlWb = xlApp.Workbooks.Open(filePath)
If Err.Number <> 0 Then
    WScript.Echo "ERROR_OPEN:" & Err.Description & " | Path=" & filePath
    xlApp.Quit: Set xlApp = Nothing: WScript.Quit 1
End If
Err.Clear

' Paste category value into target cell
Set xlWs = xlWb.Sheets(1)
xlWs.Range("{target_cell}").Value = "{safe_cat}"
If Err.Number <> 0 Then
    WScript.Echo "ERROR_PASTE:" & Err.Description
    xlWb.Close False: xlApp.Quit: Set xlApp = Nothing: WScript.Quit 3
End If
Err.Clear

WScript.Echo "RUNNING_MACRO: {macro_name}"

' Run macro
xlApp.Run "{macro_name}"
If Err.Number <> 0 Then
    WScript.Echo "ERROR_MACRO:" & Err.Description
    xlWb.Close False: xlApp.Quit: Set xlApp = Nothing: WScript.Quit 2
End If

xlWb.Save
xlWb.Close False
xlApp.Quit
Set xlApp = Nothing
WScript.Echo "SUCCESS"
WScript.Quit 0
'''
    with open(vbs_path, "w", encoding="utf-8") as f:
        f.write(vbs)
    try:
        r = subprocess.run(["cscript", "//NoLogo", vbs_path],
                           capture_output=True, text=True, timeout=timeout)
        out = r.stdout.strip()
        err = r.stderr.strip()
        if r.returncode != 0 or "ERROR" in out:
            raise RuntimeError(f"exit={r.returncode}: {out} {err}")
        return out
    finally:
        try: os.remove(vbs_path)
        except: pass


def _collect_output(date_folder, source_files, out_dir, jid, qid, mid):
    """Copy any NEW files (not in source) from date_folder to output dir."""
    src_names = {f["original_name"] for f in source_files}
    all_files = set(os.listdir(date_folder))
    new_files = [f for f in (all_files - src_names)
                 if f.lower().endswith((".xlsx",".xls",".csv",".xlsm",".xlsb"))
                 and not f.startswith("_run_")]

    if new_files:
        os.makedirs(out_dir, exist_ok=True)
        for f in new_files:
            src = os.path.join(date_folder, f)
            dst = os.path.join(out_dir, f)
            shutil.copy2(src, dst)
            D.add_log(jid, qid, mid, "INFO", "OUTPUT",
                      f"Collected: {f} ({os.path.getsize(dst)/1024:.1f}KB)")
        D.finish_queue_item(qid, "RUNNING", output_files=json.dumps(new_files))
    else:
        D.add_log(jid, qid, mid, "WARN", "NO_OUTPUT", "No new output files found")
