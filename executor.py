"""
Executor v5 — REMOTE execution on target machines.

Flow per category:
1. Files already on remote machine (from prep phase)
2. Generate VBS on remote shared folder (self-contained, uses script-relative paths)
3. VBS writes _result_xxx.txt when done
4. Execute VBS on remote machine via: PsExec > wmic > schtasks
5. Poll _result_xxx.txt for completion
6. Collect output files

60 categories / 40 machines = first 40 run in parallel, as each finishes picks next.
Kill support: set _kill_jobs[jid] = True to stop.
"""
import os, json, shutil, subprocess, threading, time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import database as D
import notifier as N

COMPILED_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "compiled_output")
_running_jobs = {}   # jid → True
_kill_jobs = {}      # jid → True (signals stop)

def is_running(jid): return _running_jobs.get(jid, False)

def kill_job(jid):
    """Signal a running job to stop."""
    _kill_jobs[jid] = True
    D.add_log(jid, level="WARN", step="KILL", message=f"Kill signal sent for job #{jid}")
    # Mark all QUEUED items as CANCELLED
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
        exec_method = D.get_setting("exec_method", "auto")  # auto|psexec|wmic|local
        psexec_path = D.get_setting("psexec_path", "psexec")

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
                  message=f"Job #{jid}: {len(queue_items)} cats × {len(machines)} machines [exec={exec_method}]")

        # ── PHASE 1: Prep all machines (auth + folder + copy files) ──
        machine_ready = {}
        with ThreadPoolExecutor(max_workers=min(len(machines), 8)) as pool:
            futs = {pool.submit(_prep_machine, m, today, files, jid): m for m in machines}
            for fut in as_completed(futs):
                m = futs[fut]
                try:
                    date_folder = fut.result()
                    machine_ready[m["machine_id"]] = date_folder
                    D.add_log(jid, machine_id=m["machine_id"], level="INFO", step="PREP_OK",
                              message=f"{m['machine_name']}: ready ({date_folder})")
                except Exception as e:
                    D.add_log(jid, machine_id=m["machine_id"], level="ERROR", step="PREP_FAIL",
                              message=f"{m['machine_name']}: {e}")
                    N.notify_copy_failure(jid, None, m["machine_name"],
                                          m["ip_address"], m["shared_folder"], str(e), "PREP")

        if not machine_ready:
            D.add_log(jid, level="ERROR", step="ABORT", message="No machines available after prep")
            D.finish_job(jid); return

        # ── PHASE 2: Workers process queue ──
        def machine_worker(mid, remote_date_folder):
            m = D.get_machine(mid)
            mname = m["machine_name"]
            hostname = m["system_name"] or m["ip_address"] or ""
            username = m["username"] or ""
            password = m["password"] or ""

            while not _kill_jobs.get(jid):
                item = D.claim_next(jid, mid)
                if not item:
                    break

                qid = item["queue_id"]
                cat = item["cat_value"]
                start = datetime.now()
                D.add_log(jid, qid, mid, "INFO", "CAT_START", f"{mname}: '{cat}'")

                try:
                    # 1. Generate VBS on remote machine
                    safe_cat = str(cat).replace('"', '""').replace("\\", "\\\\")
                    result_name = f"_result_{qid}.txt"
                    vbs_name = f"_macro_{qid}.vbs"
                    vbs_remote = os.path.join(remote_date_folder, vbs_name)
                    result_remote = os.path.join(remote_date_folder, result_name)

                    # Clean old result file
                    try: os.remove(result_remote)
                    except: pass

                    vbs_content = _make_vbs(excel_file, macro_name, target_cell,
                                            safe_cat, result_name)
                    with open(vbs_remote, "w", encoding="utf-8") as f:
                        f.write(vbs_content)

                    D.add_log(jid, qid, mid, "INFO", "VBS_DEPLOY",
                              f"VBS deployed: {vbs_remote}")

                    # 2. Execute on remote machine
                    _execute_remote(vbs_remote, hostname, username, password,
                                    exec_method, psexec_path, jid, qid, mid, mname)

                    # 3. Poll for result file
                    timeout_secs = int(D.get_setting("macro_timeout", "600"))
                    poll_ok = _poll_result(result_remote, timeout_secs, jid, qid, mid, mname)

                    if not poll_ok:
                        raise RuntimeError(f"Timeout ({timeout_secs}s) waiting for macro to finish")

                    # 4. Read result
                    result_text = ""
                    try:
                        with open(result_remote, "r", encoding="utf-8") as rf:
                            result_text = rf.read().strip()
                    except:
                        result_text = "UNKNOWN"

                    if result_text.startswith("ERROR"):
                        raise RuntimeError(f"Macro error: {result_text}")

                    D.add_log(jid, qid, mid, "INFO", "MACRO_DONE",
                              f"{mname}: '{cat}' → {result_text[:100]}")

                    # 5. Collect output files
                    _collect_output(remote_date_folder, files, compile_dir,
                                    today, group_name, mname, cat, jid, qid, mid)

                    elapsed = (datetime.now() - start).total_seconds()
                    D.finish_queue_item(qid, "SUCCESS",
                        finished_at=datetime.now().isoformat(),
                        date_folder=remote_date_folder, duration_secs=elapsed)
                    D.add_log(jid, qid, mid, "INFO", "CAT_DONE",
                              f"{mname}: '{cat}' done in {elapsed:.1f}s")

                except Exception as e:
                    elapsed = (datetime.now() - start).total_seconds()
                    D.finish_queue_item(qid, "FAILED",
                        finished_at=datetime.now().isoformat(),
                        error_message=str(e)[:500], duration_secs=elapsed)
                    D.add_log(jid, qid, mid, "ERROR", "CAT_FAIL",
                              f"{mname}: '{cat}' failed: {e}")
                    N.notify_macro_failure(jid, qid, mname,
                        m["ip_address"], excel_file, macro_name, str(e), cat)

                # Cleanup VBS + result
                for fn in [vbs_remote, result_remote]:
                    try: os.remove(fn)
                    except: pass

        # Launch worker per machine
        with ThreadPoolExecutor(max_workers=len(machine_ready)) as pool:
            futs = [pool.submit(machine_worker, mid, df)
                    for mid, df in machine_ready.items()]
            for w in as_completed(futs):
                try: w.result()
                except Exception as e:
                    D.add_log(jid, level="ERROR", step="WORKER_CRASH", message=str(e))

        # Finalize
        killed = _kill_jobs.get(jid, False)
        if killed:
            with D.db() as c:
                c.execute("UPDATE jobs SET status='KILLED',finished_at=? WHERE job_id=?",
                          (datetime.now().isoformat(), jid))
            D.add_log(jid, level="WARN", step="KILLED", message=f"Job #{jid} killed by user")
        else:
            D.finish_job(jid)
            D.add_log(jid, level="INFO", step="JOB_DONE", message=f"Job #{jid} completed")

        # Summary email
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


# ══════════════════════════════════════════════════════════════════════
#  PREP MACHINE
# ══════════════════════════════════════════════════════════════════════

def _prep_machine(machine, today, files, jid):
    """Auth → create folder → copy files in parallel. Returns remote date_folder."""
    shared = machine["shared_folder"].strip()
    system_name = (machine["system_name"] or "").strip()
    username = (machine["username"] or "").strip()
    password = (machine["password"] or "").strip()
    mid = machine["machine_id"]

    if shared.startswith("\\\\") and username and password:
        _net_use(shared, username, password, jid, mid)

    date_folder = os.path.join(shared, today)
    try:
        os.makedirs(date_folder, exist_ok=True)
    except PermissionError:
        if system_name and username and password:
            _net_use(f"\\\\{system_name}", username, password, jid, mid)
            os.makedirs(date_folder, exist_ok=True)
        else:
            raise

    def copy_one(f):
        shutil.copy2(f["stored_path"], os.path.join(date_folder, f["original_name"]))

    with ThreadPoolExecutor(max_workers=min(len(files), 4)) as pool:
        futs = [pool.submit(copy_one, f) for f in files]
        for fut in as_completed(futs):
            fut.result()

    D.add_log(jid, machine_id=mid, level="INFO", step="FILES_COPIED",
              message=f"Copied {len(files)} files to {date_folder}")
    return date_folder


# ══════════════════════════════════════════════════════════════════════
#  NET USE
# ══════════════════════════════════════════════════════════════════════

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
                  message=f"Authenticated to {server_share}")


# ══════════════════════════════════════════════════════════════════════
#  VBS GENERATION (self-contained, uses script-relative paths)
# ══════════════════════════════════════════════════════════════════════

def _make_vbs(excel_file, macro_name, target_cell, cat_value, result_filename):
    """
    Generate VBScript that:
    - Finds its own folder (works from UNC or local path)
    - Opens the Excel file FROM THAT FOLDER
    - Pastes category → runs macro → saves
    - Writes result to file
    """
    return f'''On Error Resume Next

' Get folder where this VBS lives
Dim fso, scriptFolder, resultFile, excelPath
Set fso = CreateObject("Scripting.FileSystemObject")
scriptFolder = fso.GetParentFolderName(WScript.ScriptFullName) & "\\"
excelPath = scriptFolder & "{excel_file}"
resultFile = scriptFolder & "{result_filename}"

' Write RUNNING status
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
Err.Clear

' Open workbook
Dim xlWb
Set xlWb = xlApp.Workbooks.Open(excelPath)
If Err.Number <> 0 Then
    Set rf = fso.CreateTextFile(resultFile, True)
    rf.Write "ERROR_OPEN:" & Err.Description
    rf.Close
    xlApp.Quit: Set xlApp = Nothing
    WScript.Quit 1
End If
Err.Clear

' Paste category into cell
Dim xlWs
Set xlWs = xlWb.Sheets(1)
xlWs.Range("{target_cell}").Value = "{cat_value}"
If Err.Number <> 0 Then
    Set rf = fso.CreateTextFile(resultFile, True)
    rf.Write "ERROR_PASTE:" & Err.Description
    rf.Close
    xlWb.Close False: xlApp.Quit: Set xlApp = Nothing
    WScript.Quit 3
End If
Err.Clear

' Run macro
xlApp.Run "{macro_name}"
If Err.Number <> 0 Then
    Set rf = fso.CreateTextFile(resultFile, True)
    rf.Write "ERROR_MACRO:" & Err.Description
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


# ══════════════════════════════════════════════════════════════════════
#  REMOTE EXECUTION
# ══════════════════════════════════════════════════════════════════════

def _execute_remote(vbs_path, hostname, username, password, method, psexec_path,
                    jid, qid, mid, mname):
    """
    Execute VBS on the remote machine. Tries multiple methods.
    Does NOT wait for completion — caller polls result file.
    """
    vbs_path_win = vbs_path.replace("/", "\\")

    if method == "local":
        # Run locally (for testing or when server has Excel)
        D.add_log(jid, qid, mid, "INFO", "EXEC_LOCAL",
                  f"Running locally: cscript {vbs_path_win}")
        subprocess.Popen(["cscript", "//NoLogo", vbs_path_win],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return

    if not hostname:
        D.add_log(jid, qid, mid, "WARN", "EXEC",
                  "No hostname/IP — falling back to local execution")
        subprocess.Popen(["cscript", "//NoLogo", vbs_path_win],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return

    errors = []

    # Strategy 1: PsExec
    if method in ("auto", "psexec"):
        ok = _try_psexec(vbs_path_win, hostname, username, password,
                         psexec_path, jid, qid, mid, mname)
        if ok:
            return
        errors.append("psexec failed")

    # Strategy 2: wmic
    if method in ("auto", "wmic"):
        ok = _try_wmic(vbs_path_win, hostname, username, password,
                       jid, qid, mid, mname)
        if ok:
            return
        errors.append("wmic failed")

    # Strategy 3: schtasks
    if method in ("auto", "schtasks"):
        ok = _try_schtasks(vbs_path_win, hostname, username, password,
                           qid, jid, mid, mname)
        if ok:
            return
        errors.append("schtasks failed")

    # All failed — try local as last resort
    D.add_log(jid, qid, mid, "WARN", "EXEC_FALLBACK",
              f"Remote exec failed ({'; '.join(errors)}). Trying local cscript...")
    subprocess.Popen(["cscript", "//NoLogo", vbs_path_win],
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def _try_psexec(vbs_path, hostname, username, password, psexec_path,
                jid, qid, mid, mname):
    """Run VBS on remote via PsExec. Returns True if launch succeeded."""
    cmd = [psexec_path, f"\\\\{hostname}", "-accepteula", "-d"]
    if username:
        cmd += ["-u", username]
    if password:
        cmd += ["-p", password]
    cmd += ["cscript", "//NoLogo", vbs_path]

    D.add_log(jid, qid, mid, "INFO", "EXEC_PSEXEC",
              f"PsExec → {hostname}: {' '.join(cmd[:6])}...")
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if r.returncode in (0, -1):  # -1 = psexec background mode
            D.add_log(jid, qid, mid, "INFO", "EXEC_PSEXEC", f"Launched on {hostname}")
            return True
        D.add_log(jid, qid, mid, "WARN", "EXEC_PSEXEC",
                  f"PsExec exit={r.returncode}: {(r.stderr or r.stdout)[:150]}")
    except FileNotFoundError:
        D.add_log(jid, qid, mid, "WARN", "EXEC_PSEXEC",
                  f"psexec not found at '{psexec_path}'")
    except Exception as e:
        D.add_log(jid, qid, mid, "WARN", "EXEC_PSEXEC", f"PsExec error: {e}")
    return False


def _try_wmic(vbs_path, hostname, username, password, jid, qid, mid, mname):
    """Run VBS on remote via wmic process call create."""
    wmic_cmd = f'cscript //NoLogo "{vbs_path}"'
    cmd = ["wmic", f"/node:{hostname}"]
    if username:
        cmd.append(f"/user:{username}")
    if password:
        cmd.append(f"/password:{password}")
    cmd += ["process", "call", "create", wmic_cmd]

    D.add_log(jid, qid, mid, "INFO", "EXEC_WMIC",
              f"wmic → {hostname}: process call create cscript...")
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


def _try_schtasks(vbs_path, hostname, username, password, qid, jid, mid, mname):
    """Run VBS on remote via scheduled task."""
    task_name = f"MacroRun_{qid}"
    task_cmd = f'cscript //NoLogo "{vbs_path}"'

    create_cmd = ["schtasks", "/create", "/s", hostname, "/tn", task_name,
                  "/tr", task_cmd, "/sc", "once", "/st", "00:00", "/f"]
    if username:
        create_cmd += ["/ru", username]
        if password:
            create_cmd += ["/rp", password]

    D.add_log(jid, qid, mid, "INFO", "EXEC_SCHTASKS",
              f"schtasks → {hostname}: creating task {task_name}")
    try:
        r = subprocess.run(create_cmd, capture_output=True, text=True, timeout=15)
        if r.returncode != 0:
            D.add_log(jid, qid, mid, "WARN", "EXEC_SCHTASKS",
                      f"Create failed: {(r.stderr or r.stdout)[:150]}")
            return False

        run_cmd = ["schtasks", "/run", "/s", hostname, "/tn", task_name]
        r2 = subprocess.run(run_cmd, capture_output=True, text=True, timeout=15)
        if r2.returncode != 0:
            D.add_log(jid, qid, mid, "WARN", "EXEC_SCHTASKS",
                      f"Run failed: {(r2.stderr or r2.stdout)[:150]}")
            return False

        D.add_log(jid, qid, mid, "INFO", "EXEC_SCHTASKS", f"Task running on {hostname}")

        # Cleanup task (non-blocking)
        threading.Timer(300, lambda: subprocess.run(
            ["schtasks", "/delete", "/s", hostname, "/tn", task_name, "/f"],
            capture_output=True, timeout=10
        )).start()
        return True
    except Exception as e:
        D.add_log(jid, qid, mid, "WARN", "EXEC_SCHTASKS", f"schtasks error: {e}")
    return False


# ══════════════════════════════════════════════════════════════════════
#  POLL RESULT FILE
# ══════════════════════════════════════════════════════════════════════

def _poll_result(result_path, timeout_secs, jid, qid, mid, mname):
    """Poll result file until it contains SUCCESS/ERROR or timeout."""
    start = time.time()
    poll_interval = 3  # seconds
    last_log = 0

    while (time.time() - start) < timeout_secs:
        if _kill_jobs.get(jid):
            D.add_log(jid, qid, mid, "WARN", "POLL_KILL", f"{mname}: killed while waiting")
            return False

        try:
            if os.path.exists(result_path):
                with open(result_path, "r", encoding="utf-8") as f:
                    content = f.read().strip()
                if content and content != "RUNNING":
                    return True  # Got a final result
        except:
            pass

        # Log progress every 30 seconds
        elapsed = time.time() - start
        if elapsed - last_log >= 30:
            D.add_log(jid, qid, mid, "INFO", "POLL_WAIT",
                      f"{mname}: waiting for macro... ({int(elapsed)}s)")
            last_log = elapsed

        time.sleep(poll_interval)

    return False  # Timeout


# ══════════════════════════════════════════════════════════════════════
#  COLLECT OUTPUT
# ══════════════════════════════════════════════════════════════════════

def _collect_output(remote_folder, files, compile_dir, today, group_name,
                    mname, cat, jid, qid, mid):
    """Copy new output files from remote to compile dir."""
    original_names = {f["original_name"] for f in files}
    skip_prefixes = ("_macro_", "_result_", "~$")

    try:
        all_files = set(os.listdir(remote_folder))
    except:
        D.add_log(jid, qid, mid, "WARN", "COLLECT", "Cannot list remote folder")
        return

    new_files = [f for f in (all_files - original_names)
                 if not any(f.startswith(p) for p in skip_prefixes)
                 and os.path.isfile(os.path.join(remote_folder, f))]

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
                D.add_log(jid, qid, mid, "WARN", "OUTPUT", f"Failed collecting {nf}: {e}")
        D.finish_queue_item(qid, "RUNNING", output_files=json.dumps(new_files))
    else:
        D.add_log(jid, qid, mid, "INFO", "NO_NEW_OUTPUT",
                  "No new output files (macro may modify existing file)")
