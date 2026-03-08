"""
Executor v6 - Zero local copy. Execute directly on remote machine.

PREP:   Copy files to remote share ONCE (//machine/share/date/)
EXEC:   Write VBS to same folder -> execute via wmic/psexec/cscript
        VBS opens Excel from its OWN folder (script-relative path)
        VBS writes _result_NNN.txt when done
POLL:   Read _result_NNN.txt via UNC path
COLLECT: Copy new output files to compile_dir

NO temp copy per category. 303MB files stay on remote. Only VBS + result file per cat.
60 cats / 40 machines = first 40 parallel, rest queued automatically.
"""
import os, json, shutil, subprocess, threading, time, csv
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import database as D
import notifier as N

COMPILED_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "compiled_output")
_running_jobs = {}
_kill_jobs = {}

def is_running(jid): return _running_jobs.get(jid, False)

def kill_job(jid):
    _kill_jobs[jid] = True
    D.add_log(jid, level="WARN", step="KILL", message=f"Kill signal for job #{jid}")
    with D.db() as c:
        c.execute("UPDATE job_queue SET status='CANCELLED' WHERE job_id=? AND status='QUEUED'", (jid,))

def run_async(jid):
    _running_jobs[jid] = True
    _kill_jobs.pop(jid, None)
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
        compile_dir = D.get_setting("compile_path", "") or COMPILED_DIR

        files = D.get_files(gid)
        machines = [m for m in D.get_machines(gid) if m["is_active"]]
        queue_items = D.get_queue(jid)

        if not machines:
            D.add_log(jid, level="ERROR", step="INIT", message="No active machines")
            D.finish_job(jid); return
        if not queue_items:
            D.add_log(jid, level="ERROR", step="INIT", message="No categories queued")
            D.finish_job(jid); return

        D.add_log(jid, level="INFO", step="START",
                  message=f"Job #{jid}: {len(queue_items)} cats x {len(machines)} machines")

        # == PHASE 1: Prep all machines (auth + folder + copy files ONCE) ==
        machine_ready = {}
        with ThreadPoolExecutor(max_workers=min(len(machines), 8)) as pool:
            futs = {pool.submit(_prep_machine, m, today, files, jid): m for m in machines}
            for fut in as_completed(futs):
                m = futs[fut]
                try:
                    remote_folder = fut.result()
                    machine_ready[m["machine_id"]] = remote_folder
                except Exception as e:
                    D.add_log(jid, machine_id=m["machine_id"], level="ERROR", step="PREP_FAIL",
                              message=f"{m['machine_name']}: {e}")
                    N.notify_copy_failure(jid, None, m["machine_name"],
                                          m["ip_address"], m["shared_folder"], str(e), "PREP")

        if not machine_ready:
            D.add_log(jid, level="ERROR", step="ABORT", message="No machines ready")
            D.finish_job(jid); return

        # == PHASE 2: Process categories - NO local copy ==
        def machine_worker(mid, remote_folder):
            m = D.get_machine(mid)
            mname = m["machine_name"]
            hostname = (m["system_name"] or m["ip_address"] or "").strip()
            username = (m["username"] or "").strip()
            password = (m["password"] or "").strip()

            while not _kill_jobs.get(jid):
                item = D.claim_next(jid, mid)
                if not item:
                    break

                qid = item["queue_id"]
                cat = item["cat_value"]
                start = datetime.now()
                D.add_log(jid, qid, mid, "INFO", "CAT_START", f"{mname}: '{cat}'")

                vbs_path = None
                result_path = None

                try:
                    safe_cat = str(cat).replace('"', '""')
                    vbs_name = f"_run_{qid}.vbs"
                    result_name = f"_result_{qid}.txt"
                    vbs_path = os.path.join(remote_folder, vbs_name)
                    result_path = os.path.join(remote_folder, result_name)

                    # Clean old result
                    try: os.remove(result_path)
                    except: pass

                    # Write VBS to remote folder (uses script-relative paths)
                    vbs_code = _make_vbs(excel_file, macro_name, target_cell,
                                         safe_cat, result_name)
                    with open(vbs_path, "w", encoding="utf-8") as f:
                        f.write(vbs_code)

                    D.add_log(jid, qid, mid, "INFO", "VBS_READY",
                              f"VBS written to {vbs_path}")

                    # Execute VBS
                    _execute_vbs(vbs_path, hostname, username, password,
                                 jid, qid, mid, mname)

                    # Poll for result
                    timeout_secs = int(D.get_setting("macro_timeout", "600"))
                    got_result = _poll_result(result_path, timeout_secs, jid, qid, mid, mname)

                    if not got_result:
                        raise RuntimeError(f"Timeout ({timeout_secs}s) waiting for macro")

                    # Read result
                    result_text = ""
                    try:
                        with open(result_path, "r", encoding="utf-8") as rf:
                            result_text = rf.read().strip()
                    except:
                        result_text = "UNKNOWN"

                    if result_text.startswith("ERROR"):
                        raise RuntimeError(f"Macro: {result_text}")

                    D.add_log(jid, qid, mid, "INFO", "MACRO_DONE",
                              f"{mname}: '{cat}' -> {result_text[:80]}")

                    # Collect output files
                    new_files = _collect_output(remote_folder, files, compile_dir,
                                                today, group_name, mname, cat,
                                                jid, qid, mid)

                    elapsed = (datetime.now() - start).total_seconds()
                    D.finish_queue_item(qid, "SUCCESS",
                        finished_at=datetime.now().isoformat(),
                        date_folder=remote_folder, duration_secs=elapsed,
                        output_files=json.dumps(new_files) if new_files else "")
                    D.add_log(jid, qid, mid, "INFO", "CAT_DONE",
                              f"{mname}: '{cat}' done in {elapsed:.1f}s ({len(new_files)} outputs)")

                except Exception as e:
                    elapsed = (datetime.now() - start).total_seconds()
                    D.finish_queue_item(qid, "FAILED",
                        finished_at=datetime.now().isoformat(),
                        error_message=str(e)[:500], duration_secs=elapsed)
                    D.add_log(jid, qid, mid, "ERROR", "CAT_FAIL",
                              f"{mname}: '{cat}' failed: {e}")
                    N.notify_macro_failure(jid, qid, mname,
                        m["ip_address"], excel_file, macro_name, str(e), cat)
                finally:
                    # Cleanup VBS + result
                    for fp in [vbs_path, result_path]:
                        if fp:
                            try: os.remove(fp)
                            except: pass

        # Launch workers
        with ThreadPoolExecutor(max_workers=len(machine_ready)) as pool:
            futs = [pool.submit(machine_worker, mid, rf)
                    for mid, rf in machine_ready.items()]
            for w in as_completed(futs):
                try: w.result()
                except Exception as e:
                    D.add_log(jid, level="ERROR", step="WORKER_CRASH", message=str(e))

        # Finalize
        if _kill_jobs.get(jid):
            with D.db() as c:
                c.execute("UPDATE jobs SET status='KILLED',finished_at=? WHERE job_id=?",
                          (datetime.now().isoformat(), jid))
            D.add_log(jid, level="WARN", step="KILLED", message=f"Job #{jid} killed")
        else:
            D.finish_job(jid)
            D.add_log(jid, level="INFO", step="JOB_DONE", message=f"Job #{jid} completed")

        # Tracker CSV
        track_collected_files(jid)

        # Summary email if failures
        job = D.get_job(jid)
        if (job["failed_cats"] or 0) > 0:
            fails = []
            for q in D.get_queue(jid):
                if q["status"] == "FAILED":
                    fails.append({"machine": q["machine_name"] or "?",
                                  "ip": q["ip_address"] or "",
                                  "step": f"CAT:{q['cat_value']}",
                                  "error": q["error_message"] or ""})
            N.notify_job_summary(jid, group_name, job["total_cats"],
                                 job["completed_cats"], job["failed_cats"], fails)

    except Exception as e:
        D.add_log(jid, level="ERROR", step="JOB_CRASH", message=str(e))
        with D.db() as c:
            c.execute("UPDATE jobs SET status='CRASHED',finished_at=? WHERE job_id=?",
                      (datetime.now().isoformat(), jid))
    finally:
        _running_jobs.pop(jid, None)
        _kill_jobs.pop(jid, None)


# =====================================================================
#  PREP MACHINE (copy files ONCE)
# =====================================================================

def _prep_machine(machine, today, files, jid):
    shared = machine["shared_folder"].strip()
    system_name = (machine["system_name"] or "").strip()
    username = (machine["username"] or "").strip()
    password = (machine["password"] or "").strip()
    mid = machine["machine_id"]
    mname = machine["machine_name"]

    # Authenticate
    if shared.startswith("\\\\") and username and password:
        _net_use(shared, username, password, jid, mid)

    # Create date folder
    date_folder = os.path.join(shared, today)
    try:
        os.makedirs(date_folder, exist_ok=True)
    except PermissionError:
        if system_name and username and password:
            _net_use(f"\\\\{system_name}", username, password, jid, mid)
            os.makedirs(date_folder, exist_ok=True)
        else:
            raise

    # Copy ALL files in parallel (this happens ONCE per machine)
    D.add_log(jid, machine_id=mid, level="INFO", step="PREP_START",
              message=f"{mname}: copying {len(files)} files")

    def copy_one(f):
        dst = os.path.join(date_folder, f["original_name"])
        shutil.copy2(f["stored_path"], dst)
        sz = os.path.getsize(dst) / 1024 / 1024
        D.add_log(jid, machine_id=mid, level="INFO", step="FILE_COPY_OK",
                  message=f"{f['original_name']} ({sz:.1f}MB)")

    with ThreadPoolExecutor(max_workers=min(len(files), 4)) as pool:
        futs = [pool.submit(copy_one, f) for f in files]
        for fut in as_completed(futs):
            fut.result()

    D.add_log(jid, machine_id=mid, level="INFO", step="PREP_OK",
              message=f"{mname}: all {len(files)} files copied to {date_folder}")
    return date_folder


# =====================================================================
#  NET USE
# =====================================================================

def _net_use(unc_path, username, password, jid=None, mid=None):
    clean = unc_path.replace("/", "\\").rstrip("\\")
    parts = [p for p in clean.split("\\") if p]

    if len(parts) >= 2:
        server_share = f"\\\\{parts[0]}\\{parts[1]}"
    elif len(parts) == 1:
        server_share = f"\\\\{parts[0]}"
    else:
        return

    D.add_log(jid, machine_id=mid, level="INFO", step="NET_USE",
              message=f"net use {server_share} /user:{username}")

    try:
        subprocess.run(["net", "use", server_share, "/delete", "/y"],
                       capture_output=True, timeout=10)
    except: pass

    time.sleep(0.5)

    r = subprocess.run(
        ["net", "use", server_share, f"/user:{username}", password, "/persistent:no"],
        capture_output=True, text=True, timeout=15
    )
    if r.returncode != 0:
        err = (r.stderr.strip() or r.stdout.strip())[:200]
        D.add_log(jid, machine_id=mid, level="WARN", step="NET_USE",
                  message=f"net use {server_share} failed: {err}")
    else:
        D.add_log(jid, machine_id=mid, level="INFO", step="NET_USE",
                  message=f"OK: authenticated to {server_share}")


# =====================================================================
#  VBS GENERATION (self-contained, script-relative paths)
# =====================================================================

def _make_vbs(excel_file, macro_name, target_cell, cat_value, result_filename):
    """
    VBS that:
    1. Finds its own folder (works from UNC or local)
    2. Opens Excel from THAT folder
    3. Pastes category -> runs macro -> saves
    4. Writes result file in same folder
    """
    return f'''On Error Resume Next

Dim fso, scriptFolder, excelPath, resultFile
Set fso = CreateObject("Scripting.FileSystemObject")
scriptFolder = fso.GetParentFolderName(WScript.ScriptFullName) & "\\"
excelPath = scriptFolder & "{excel_file}"
resultFile = scriptFolder & "{result_filename}"

' Mark as running
Dim rf
Set rf = fso.CreateTextFile(resultFile, True)
rf.Write "RUNNING"
rf.Close

' Check file exists
If Not fso.FileExists(excelPath) Then
    Set rf = fso.CreateTextFile(resultFile, True)
    rf.Write "ERROR_OPEN:File not found: " & excelPath
    rf.Close
    WScript.Quit 1
End If

' Kill any orphan Excel processes (clean slate)
' Start Excel
Dim xlApp
Set xlApp = CreateObject("Excel.Application")
If Err.Number <> 0 Then
    Set rf = fso.CreateTextFile(resultFile, True)
    rf.Write "ERROR_OPEN:Cannot start Excel: " & Err.Description
    rf.Close
    WScript.Quit 1
End If

xlApp.Visible = False
xlApp.DisplayAlerts = False
xlApp.AskToUpdateLinks = False
xlApp.EnableEvents = False
Err.Clear

' Open workbook
Dim xlWb
Set xlWb = xlApp.Workbooks.Open(excelPath, 0, False)
If Err.Number <> 0 Then
    Set rf = fso.CreateTextFile(resultFile, True)
    rf.Write "ERROR_OPEN:" & Err.Number & ":" & Err.Description
    rf.Close
    xlApp.Quit: Set xlApp = Nothing
    WScript.Quit 1
End If
Err.Clear

' Paste category
xlWb.Sheets(1).Range("{target_cell}").Value = "{cat_value}"
If Err.Number <> 0 Then
    Set rf = fso.CreateTextFile(resultFile, True)
    rf.Write "ERROR_PASTE:" & Err.Number & ":" & Err.Description
    rf.Close
    xlWb.Close False: xlApp.Quit: Set xlApp = Nothing
    WScript.Quit 3
End If
Err.Clear

' Run macro
xlApp.Run "{macro_name}"
If Err.Number <> 0 Then
    Set rf = fso.CreateTextFile(resultFile, True)
    rf.Write "ERROR_MACRO:" & Err.Number & ":" & Err.Description
    rf.Close
    xlWb.Close False: xlApp.Quit: Set xlApp = Nothing
    WScript.Quit 2
End If

' Save and close
xlWb.Save
xlWb.Close False
xlApp.Quit
Set xlApp = Nothing

' Write success
Set rf = fso.CreateTextFile(resultFile, True)
rf.Write "SUCCESS"
rf.Close
WScript.Quit 0
'''


# =====================================================================
#  EXECUTE VBS (try remote first, then local)
# =====================================================================

def _execute_vbs(vbs_path, hostname, username, password, jid, qid, mid, mname):
    """Execute VBS. Try wmic for remote, fall back to local cscript."""
    vbs_win = vbs_path.replace("/", "\\")

    # If hostname available, try remote execution first
    if hostname:
        # Try wmic
        ok = _try_wmic(vbs_win, hostname, username, password, jid, qid, mid, mname)
        if ok:
            return

        # Try psexec
        psexec_path = D.get_setting("psexec_path", "psexec")
        ok = _try_psexec(vbs_win, hostname, username, password, psexec_path,
                         jid, qid, mid, mname)
        if ok:
            return

        D.add_log(jid, qid, mid, "WARN", "EXEC_FALLBACK",
                  f"Remote exec failed, using local cscript")

    # Local execution (works when server IS the target machine, or as fallback)
    D.add_log(jid, qid, mid, "INFO", "EXEC_LOCAL",
              f"cscript {vbs_win}")
    subprocess.Popen(
        ["cscript", "//NoLogo", vbs_win],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0)
    )


def _try_wmic(vbs_path, hostname, username, password, jid, qid, mid, mname):
    """Execute on remote via wmic process call create. Returns True if OK."""
    wmic_cmd = f'cscript //NoLogo "{vbs_path}"'
    cmd = ["wmic", f"/node:{hostname}"]
    if username:
        cmd.append(f"/user:{username}")
    if password:
        cmd.append(f"/password:{password}")
    cmd += ["process", "call", "create", wmic_cmd]

    D.add_log(jid, qid, mid, "INFO", "EXEC_WMIC",
              f"wmic /node:{hostname} process call create cscript...")
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        out = (r.stdout + r.stderr).strip()
        if "ReturnValue = 0" in out or r.returncode == 0:
            D.add_log(jid, qid, mid, "INFO", "EXEC_WMIC", f"Process created on {hostname}")
            return True
        D.add_log(jid, qid, mid, "WARN", "EXEC_WMIC", f"wmic: {out[:150]}")
    except Exception as e:
        D.add_log(jid, qid, mid, "WARN", "EXEC_WMIC", f"wmic error: {e}")
    return False


def _try_psexec(vbs_path, hostname, username, password, psexec_path,
                jid, qid, mid, mname):
    """Execute on remote via PsExec. Returns True if OK."""
    cmd = [psexec_path, f"\\\\{hostname}", "-accepteula", "-d",
           "-i"]  # -i = interactive session (needed for Excel)
    if username:
        cmd += ["-u", username]
    if password:
        cmd += ["-p", password]
    cmd += ["cscript", "//NoLogo", vbs_path]

    D.add_log(jid, qid, mid, "INFO", "EXEC_PSEXEC", f"PsExec -> {hostname}")
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if r.returncode in (0, -1):
            D.add_log(jid, qid, mid, "INFO", "EXEC_PSEXEC", f"Launched on {hostname}")
            return True
        D.add_log(jid, qid, mid, "WARN", "EXEC_PSEXEC",
                  f"exit={r.returncode}: {(r.stderr or r.stdout)[:150]}")
    except FileNotFoundError:
        D.add_log(jid, qid, mid, "WARN", "EXEC_PSEXEC", f"psexec not found at '{psexec_path}'")
    except Exception as e:
        D.add_log(jid, qid, mid, "WARN", "EXEC_PSEXEC", f"error: {e}")
    return False


# =====================================================================
#  POLL RESULT FILE
# =====================================================================

def _poll_result(result_path, timeout_secs, jid, qid, mid, mname):
    """Poll result file every 3s until SUCCESS/ERROR or timeout."""
    start = time.time()
    last_log = 0

    while (time.time() - start) < timeout_secs:
        if _kill_jobs.get(jid):
            return False

        try:
            if os.path.exists(result_path):
                with open(result_path, "r", encoding="utf-8") as f:
                    content = f.read().strip()
                if content and content != "RUNNING":
                    return True
        except:
            pass

        elapsed = time.time() - start
        if elapsed - last_log >= 30:
            D.add_log(jid, qid, mid, "INFO", "WAITING",
                      f"{mname}: waiting for macro ({int(elapsed)}s)")
            last_log = elapsed

        time.sleep(3)

    return False


# =====================================================================
#  COLLECT OUTPUT FILES
# =====================================================================

def _collect_output(remote_folder, files, compile_dir, today, group_name,
                    mname, cat, jid, qid, mid):
    """Copy NEW output files from remote to compile dir."""
    original_names = {f["original_name"] for f in files}
    skip_prefixes = ("_run_", "_result_", "_macro_", "~$")
    new_files = []

    try:
        for fname in os.listdir(remote_folder):
            if fname in original_names:
                continue
            if any(fname.startswith(p) for p in skip_prefixes):
                continue
            fpath = os.path.join(remote_folder, fname)
            if not os.path.isfile(fpath):
                continue
            new_files.append(fname)
    except Exception as e:
        D.add_log(jid, qid, mid, "WARN", "COLLECT", f"Cannot list folder: {e}")
        return []

    if new_files:
        out_dir = os.path.join(compile_dir, today, group_name, mname, cat)
        os.makedirs(out_dir, exist_ok=True)
        for nf in new_files:
            try:
                src = os.path.join(remote_folder, nf)
                dst = os.path.join(out_dir, nf)
                shutil.copy2(src, dst)
                sz = os.path.getsize(dst) / 1024
                D.add_log(jid, qid, mid, "INFO", "OUTPUT", f"Collected: {nf} ({sz:.1f}KB)")
            except Exception as e:
                D.add_log(jid, qid, mid, "WARN", "OUTPUT", f"Failed: {nf}: {e}")

    return new_files


# =====================================================================
#  TRACKER CSV
# =====================================================================

def track_collected_files(jid):
    """Create tracker CSV listing all collected output files."""
    job = D.get_job(jid)
    if not job:
        return

    today = datetime.now().strftime("%Y-%m-%d")
    group_name = job["group_name"]
    compile_base = D.get_setting("compile_path", "") or COMPILED_DIR
    compile_root = os.path.join(compile_base, today, group_name)

    try:
        os.makedirs(compile_root, exist_ok=True)
        tracker_path = os.path.join(compile_root,
                                    f"TRACKER_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")

        collected = []
        for item in D.get_queue(jid):
            item_dict = dict(item)
            if item_dict.get("output_files"):
                try:
                    for fname in json.loads(item_dict["output_files"]):
                        collected.append({
                            "job_id": jid, "date": today, "group": group_name,
                            "machine": item_dict.get("machine_name") or "Unknown",
                            "category": item_dict["cat_value"],
                            "filename": fname,
                            "collected_at": datetime.now().isoformat()
                        })
                except: pass

        with open(tracker_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["job_id","date","group","machine",
                                               "category","filename","collected_at"])
            w.writeheader()
            w.writerows(collected)

        D.add_log(jid, level="INFO", step="TRACKER",
                  message=f"Tracker: {os.path.basename(tracker_path)} ({len(collected)} files)")
    except Exception as e:
        D.add_log(jid, level="WARN", step="TRACKER", message=f"Tracker failed: {e}")
