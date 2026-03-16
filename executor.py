"""
Executor v8 - Remote execution using LOCAL paths on target machines.

Machine config has TWO paths:
  shared_folder: UNC path (server copies files here)
  remote_path:   local drive path on remote PC (VBS runs from here)

FLOW:
  1. PREP: Copy Excel files to remote share via UNC
  2. PER CATEGORY:
     a. Write VBS to remote share via UNC
     b. schtasks creates task on remote PC using LOCAL path
     c. VBS runs ON remote PC, opens Excel from local drive
     d. Server polls result file via UNC
     e. Collect output, pick next QUEUED category

  60 cats / 40 machines = 40 parallel, 20 queued.
  Each machine runs ONE category at a time.
"""
import os, json, shutil, subprocess, threading, time, csv
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import database as D
import notifier as N

COMPILED_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "compiled_output")
_running_jobs = {}
_kill_jobs = {}


def is_running(jid):
    return _running_jobs.get(jid, False)


def kill_job(jid):
    _kill_jobs[jid] = True
    D.add_log(jid, level="WARN", step="KILL", message=f"Kill signal for job #{jid}")
    with D.db() as c:
        c.execute("UPDATE job_queue SET status='CANCELLED' WHERE job_id=? AND status='QUEUED'", (jid,))


def run_async(jid):
    _running_jobs[jid] = True
    _kill_jobs.pop(jid, None)
    threading.Thread(target=_run_job, args=(jid,), daemon=True).start()


# =====================================================================
#  MAIN JOB
# =====================================================================

def _run_job(jid):
    try:
        job = D.get_job(jid)
        if not job:
            return
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
            D.finish_job(jid)
            return
        if not queue_items:
            D.add_log(jid, level="ERROR", step="INIT", message="No categories queued")
            D.finish_job(jid)
            return

        D.add_log(jid, level="INFO", step="START",
                  message=f"Job #{jid}: {len(queue_items)} cats x {len(machines)} machines")

        # == PHASE 1: Copy files to each machine ONCE ==
        machine_ready = {}  # mid -> (unc_folder, local_folder)
        with ThreadPoolExecutor(max_workers=min(len(machines), 8)) as pool:
            futs = {pool.submit(_prep_machine, m, today, files, jid): m for m in machines}
            for fut in as_completed(futs):
                m = futs[fut]
                try:
                    unc_folder, local_folder = fut.result()
                    machine_ready[m["machine_id"]] = (unc_folder, local_folder)
                except Exception as e:
                    D.add_log(jid, machine_id=m["machine_id"], level="ERROR",
                              step="PREP_FAIL", message=f"{m['machine_name']}: {e}")

        if not machine_ready:
            D.add_log(jid, level="ERROR", step="ABORT", message="No machines ready")
            D.finish_job(jid)
            return

        # == PHASE 2: Each machine processes categories ==
        def machine_worker(mid, unc_folder, local_folder):
            m = D.get_machine(mid)
            mname = m["machine_name"]
            hostname = (m["system_name"] or m["ip_address"] or "").strip()
            username = (m["username"] or "").strip()
            password = (m["password"] or "").strip()

            while not _kill_jobs.get(jid):
                item = D.claim_next(jid, mid)
                if not item:
                    break  # Queue empty

                qid = item["queue_id"]
                cat = item["cat_value"]
                start = datetime.now()
                task_name = f"MQ_{jid}_{qid}"
                vbs_unc = None
                result_unc = None

                D.add_log(jid, qid, mid, "INFO", "CAT_START",
                          f"{mname}: '{cat}'")

                try:
                    safe_cat = str(cat).replace('"', '""')
                    vbs_name = f"_run_{qid}.vbs"
                    result_name = f"_result_{qid}.txt"

                    # UNC paths (server writes/polls via these)
                    vbs_unc = os.path.join(unc_folder, vbs_name)
                    result_unc = os.path.join(unc_folder, result_name)

                    # LOCAL paths (VBS runs with these on remote machine)
                    vbs_local = os.path.join(local_folder, vbs_name)
                    result_local = os.path.join(local_folder, result_name)

                    # Clean old result
                    try:
                        os.remove(result_unc)
                    except:
                        pass

                    # Write VBS to remote share via UNC
                    vbs_code = _make_vbs(excel_file, macro_name, target_cell,
                                         safe_cat, result_name)
                    with open(vbs_unc, "w", encoding="utf-8") as f:
                        f.write(vbs_code)

                    D.add_log(jid, qid, mid, "INFO", "VBS_READY",
                              f"UNC: {vbs_unc} | LOCAL: {vbs_local}")

                    # Execute on remote machine using LOCAL path
                    _execute_on_machine(
                        vbs_local_path=vbs_local,
                        hostname=hostname,
                        username=username,
                        password=password,
                        task_name=task_name,
                        vbs_unc_path=vbs_unc,
                        jid=jid, qid=qid, mid=mid, mname=mname
                    )

                    # Poll result via UNC
                    timeout_secs = int(D.get_setting("macro_timeout", "1800"))
                    got_result = _poll_result(result_unc, timeout_secs,
                                              jid, qid, mid, mname)

                    if not got_result:
                        raise RuntimeError(
                            f"Timeout ({timeout_secs}s) waiting for macro")

                    # Read result
                    result_text = ""
                    try:
                        with open(result_unc, "r", encoding="utf-8") as rf:
                            result_text = rf.read().strip()
                    except:
                        result_text = "UNKNOWN"

                    if result_text.startswith("ERROR"):
                        raise RuntimeError(result_text)

                    D.add_log(jid, qid, mid, "INFO", "MACRO_DONE",
                              f"{mname}: '{cat}' -> {result_text[:80]}")

                    # Collect output
                    new_files = _collect_output(
                        unc_folder, files, compile_dir,
                        today, group_name, mname, cat,
                        jid, qid, mid)

                    elapsed = (datetime.now() - start).total_seconds()
                    D.finish_queue_item(qid, "SUCCESS",
                        finished_at=datetime.now().isoformat(),
                        date_folder=unc_folder,
                        duration_secs=elapsed,
                        output_files=json.dumps(new_files) if new_files else "")
                    D.add_log(jid, qid, mid, "INFO", "CAT_DONE",
                              f"{mname}: '{cat}' done in {elapsed:.1f}s")

                except Exception as e:
                    elapsed = (datetime.now() - start).total_seconds()
                    D.finish_queue_item(qid, "FAILED",
                        finished_at=datetime.now().isoformat(),
                        error_message=str(e)[:500],
                        duration_secs=elapsed)
                    D.add_log(jid, qid, mid, "ERROR", "CAT_FAIL",
                              f"{mname}: '{cat}' failed: {e}")
                    N.notify_macro_failure(jid, qid, mname,
                        m["ip_address"], excel_file, macro_name, str(e), cat)

                finally:
                    # Cleanup
                    for fp in [vbs_unc, result_unc]:
                        if fp:
                            try:
                                os.remove(fp)
                            except:
                                pass
                    if hostname:
                        try:
                            subprocess.run(
                                ["schtasks", "/delete", "/s", hostname,
                                 "/tn", task_name, "/f"],
                                capture_output=True, timeout=10)
                        except:
                            pass

        # Launch one thread per machine
        with ThreadPoolExecutor(max_workers=len(machine_ready)) as pool:
            futs = [pool.submit(machine_worker, mid, unc, loc)
                    for mid, (unc, loc) in machine_ready.items()]
            for w in as_completed(futs):
                try:
                    w.result()
                except Exception as e:
                    D.add_log(jid, level="ERROR", step="WORKER_CRASH",
                              message=str(e))

        # Finalize
        if _kill_jobs.get(jid):
            with D.db() as c:
                c.execute("UPDATE jobs SET status='KILLED',finished_at=? WHERE job_id=?",
                          (datetime.now().isoformat(), jid))
            D.add_log(jid, level="WARN", step="KILLED", message=f"Job #{jid} killed")
        else:
            D.finish_job(jid)
            D.add_log(jid, level="INFO", step="JOB_DONE", message=f"Job #{jid} completed")

        track_collected_files(jid)

        # Email summary if failures
        job = D.get_job(jid)
        if (job["failed_cats"] or 0) > 0:
            fails = []
            for q in D.get_queue(jid):
                if q["status"] == "FAILED":
                    fails.append({
                        "machine": q["machine_name"] or "?",
                        "ip": q["ip_address"] or "",
                        "step": f"CAT:{q['cat_value']}",
                        "error": q["error_message"] or ""
                    })
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
#  PREP MACHINE - copy files ONCE, return (unc_folder, local_folder)
# =====================================================================

def _prep_machine(machine, today, files, jid):
    shared = machine["shared_folder"].strip()
    remote_path = (machine["remote_path"] or "").strip()
    system_name = (machine["system_name"] or "").strip()
    username = (machine["username"] or "").strip()
    password = (machine["password"] or "").strip()
    mid = machine["machine_id"]
    mname = machine["machine_name"]

    if not remote_path:
        raise ValueError(f"{mname}: 'Remote Local Path' is empty. "
                         f"Set it to the local drive path on the remote PC "
                         f"(e.g. D:\\test if shared_folder is \\\\{mname}\\test)")

    # Auth
    if shared.startswith("\\\\") and username and password:
        _net_use(shared, username, password, jid, mid)

    # Create date folder via UNC
    unc_folder = os.path.join(shared, today)
    try:
        os.makedirs(unc_folder, exist_ok=True)
    except PermissionError:
        if system_name and username and password:
            _net_use(f"\\\\{system_name}", username, password, jid, mid)
            os.makedirs(unc_folder, exist_ok=True)
        else:
            raise

    # Local folder path on remote machine
    local_folder = os.path.join(remote_path, today)

    # Copy files in parallel
    D.add_log(jid, machine_id=mid, level="INFO", step="PREP",
              message=f"{mname}: copying {len(files)} files to {unc_folder}")

    def copy_one(f):
        dst = os.path.join(unc_folder, f["original_name"])
        shutil.copy2(f["stored_path"], dst)
        sz = os.path.getsize(dst) / 1024 / 1024
        D.add_log(jid, machine_id=mid, level="INFO", step="FILE_OK",
                  message=f"{f['original_name']} ({sz:.1f}MB)")

    with ThreadPoolExecutor(max_workers=min(len(files), 4)) as pool:
        futs = [pool.submit(copy_one, f) for f in files]
        for fut in as_completed(futs):
            fut.result()

    D.add_log(jid, machine_id=mid, level="INFO", step="PREP_OK",
              message=f"{mname}: ready (UNC={unc_folder} LOCAL={local_folder})")
    return unc_folder, local_folder


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
    except:
        pass
    time.sleep(0.5)
    r = subprocess.run(
        ["net", "use", server_share, f"/user:{username}", password,
         "/persistent:no"],
        capture_output=True, text=True, timeout=15)
    if r.returncode != 0:
        err = (r.stderr.strip() or r.stdout.strip())[:200]
        D.add_log(jid, machine_id=mid, level="WARN", step="NET_USE",
                  message=f"Failed: {err}")
    else:
        D.add_log(jid, machine_id=mid, level="INFO", step="NET_USE",
                  message=f"OK: {server_share}")


# =====================================================================
#  VBS - Simple, uses script-relative paths. No drive mapping needed
#  because schtasks runs it from LOCAL path (D:\test\...)
# =====================================================================

def _make_vbs(excel_file, macro_name, target_cell, cat_value, result_filename):
    return f'''
Dim fso, scriptFolder, excelPath, resultPath
Set fso = CreateObject("Scripting.FileSystemObject")
scriptFolder = fso.GetParentFolderName(WScript.ScriptFullName)
If Right(scriptFolder, 1) <> "\\" Then scriptFolder = scriptFolder & "\\"

excelPath = scriptFolder & "{excel_file}"
resultPath = scriptFolder & "{result_filename}"

' Mark as running
Call WriteResult(resultPath, "RUNNING")

' Check file exists
If Not fso.FileExists(excelPath) Then
    Call WriteResult(resultPath, "ERROR_OPEN:File not found: " & excelPath)
    WScript.Quit 1
End If

' Start Excel
On Error Resume Next
Dim xlApp
Set xlApp = CreateObject("Excel.Application")
If Err.Number <> 0 Then
    Call WriteResult(resultPath, "ERROR_OPEN:Cannot start Excel: " & Err.Description)
    WScript.Quit 1
End If
On Error GoTo 0

xlApp.Visible = False
xlApp.DisplayAlerts = False
xlApp.AskToUpdateLinks = False
xlApp.EnableEvents = False

' Open workbook
On Error Resume Next
Dim xlWb
Set xlWb = xlApp.Workbooks.Open(excelPath, 0, False)
If Err.Number <> 0 Then
    Call WriteResult(resultPath, "ERROR_OPEN:" & Err.Number & ":" & Err.Description & " Path=" & excelPath)
    xlApp.Quit: Set xlApp = Nothing
    WScript.Quit 1
End If
On Error GoTo 0

' Paste category
On Error Resume Next
xlWb.Sheets(1).Range("{target_cell}").Value = "{cat_value}"
If Err.Number <> 0 Then
    Call WriteResult(resultPath, "ERROR_PASTE:" & Err.Number & ":" & Err.Description)
    xlWb.Close False: xlApp.Quit: Set xlApp = Nothing
    WScript.Quit 1
End If
On Error GoTo 0

' Run macro
On Error Resume Next
xlApp.Run "{macro_name}"
If Err.Number <> 0 Then
    Call WriteResult(resultPath, "ERROR_MACRO:" & Err.Number & ":" & Err.Description)
    xlWb.Close False: xlApp.Quit: Set xlApp = Nothing
    WScript.Quit 1
End If
On Error GoTo 0

' Save and close
On Error Resume Next
xlWb.Save
xlWb.Close False
xlApp.Quit
Set xlApp = Nothing
On Error GoTo 0

Call WriteResult(resultPath, "SUCCESS")
WScript.Quit 0

Sub WriteResult(path, msg)
    Dim f
    Set f = CreateObject("Scripting.FileSystemObject").CreateTextFile(path, True)
    f.Write msg
    f.Close
End Sub
'''


# =====================================================================
#  EXECUTE VBS ON MACHINE
#  Uses LOCAL path for schtasks (runs on remote machine's drive)
# =====================================================================

def _execute_on_machine(vbs_local_path, hostname, username, password,
                        task_name, vbs_unc_path, jid, qid, mid, mname):
    """Execute VBS. Uses LOCAL path via schtasks on remote, or cscript locally."""

    # Detect if target is THIS machine
    is_local = _is_local_machine(hostname)

    if is_local or not hostname:
        # Run locally with the UNC path (will work if local machine)
        D.add_log(jid, qid, mid, "INFO", "EXEC_LOCAL",
                  f"Running locally: cscript {vbs_unc_path}")
        subprocess.Popen(
            ["cscript", "//NoLogo", vbs_unc_path.replace("/", "\\")],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
        return

    # === REMOTE MACHINE: use LOCAL path via schtasks ===
    vbs_local_win = vbs_local_path.replace("/", "\\")

    # Method 1: schtasks (built into Windows, most reliable)
    ok = _try_schtasks(vbs_local_win, hostname, username, password,
                       task_name, jid, qid, mid, mname)
    if ok:
        return

    # Method 2: psexec with LOCAL path
    psexec_path = D.get_setting("psexec_path", "psexec")
    ok = _try_psexec(vbs_local_win, hostname, username, password,
                     psexec_path, jid, qid, mid, mname)
    if ok:
        return

    # Method 3: wmic with LOCAL path
    ok = _try_wmic(vbs_local_win, hostname, username, password,
                   jid, qid, mid, mname)
    if ok:
        return

    # All remote methods failed
    D.add_log(jid, qid, mid, "ERROR", "EXEC_FAIL",
              f"Cannot execute on {hostname}. Check: "
              f"1) System Name correct? 2) Username has admin rights? "
              f"3) Firewall allows schtasks? 4) Remote Path correct?")
    raise RuntimeError(f"All remote execution methods failed for {mname} ({hostname})")


def _is_local_machine(hostname):
    if not hostname:
        return True
    import socket
    local_names = {"localhost", "127.0.0.1", "."}
    try:
        local_names.add(socket.gethostname().lower())
        local_names.add(socket.getfqdn().lower())
    except:
        pass
    return hostname.lower() in local_names


def _try_schtasks(vbs_local_path, hostname, username, password,
                  task_name, jid, qid, mid, mname):
    """Create + run scheduled task. VBS path is LOCAL on remote machine."""
    task_cmd = f'cscript //NoLogo "{vbs_local_path}"'

    create_cmd = [
        "schtasks", "/create",
        "/s", hostname,
        "/tn", task_name,
        "/tr", task_cmd,
        "/sc", "once",
        "/st", "00:00",
        "/f",
        "/rl", "highest",
    ]
    if username:
        create_cmd += ["/ru", username]
        if password:
            create_cmd += ["/rp", password]

    D.add_log(jid, qid, mid, "INFO", "SCHTASKS",
              f"Creating task on {hostname}: {task_cmd}")

    try:
        r = subprocess.run(create_cmd, capture_output=True, text=True, timeout=15)
        if r.returncode != 0:
            err = (r.stderr.strip() or r.stdout.strip())[:200]
            D.add_log(jid, qid, mid, "WARN", "SCHTASKS",
                      f"Create failed: {err}")
            return False

        run_cmd = ["schtasks", "/run", "/s", hostname, "/tn", task_name]
        r2 = subprocess.run(run_cmd, capture_output=True, text=True, timeout=15)
        if r2.returncode != 0:
            err = (r2.stderr.strip() or r2.stdout.strip())[:200]
            D.add_log(jid, qid, mid, "WARN", "SCHTASKS",
                      f"Run failed: {err}")
            return False

        D.add_log(jid, qid, mid, "INFO", "SCHTASKS",
                  f"Task running on {hostname}")
        return True

    except Exception as e:
        D.add_log(jid, qid, mid, "WARN", "SCHTASKS", f"Error: {e}")
        return False


def _try_psexec(vbs_local_path, hostname, username, password,
                psexec_path, jid, qid, mid, mname):
    cmd = [psexec_path, f"\\\\{hostname}", "-accepteula", "-d", "-i"]
    if username:
        cmd += ["-u", username]
    if password:
        cmd += ["-p", password]
    cmd += ["cscript", "//NoLogo", vbs_local_path]

    D.add_log(jid, qid, mid, "INFO", "PSEXEC", f"PsExec -> {hostname}")
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if r.returncode in (0, -1):
            D.add_log(jid, qid, mid, "INFO", "PSEXEC", f"Launched on {hostname}")
            return True
        D.add_log(jid, qid, mid, "WARN", "PSEXEC",
                  f"exit={r.returncode}: {(r.stderr or r.stdout)[:150]}")
    except FileNotFoundError:
        D.add_log(jid, qid, mid, "WARN", "PSEXEC",
                  f"Not found: '{psexec_path}'")
    except Exception as e:
        D.add_log(jid, qid, mid, "WARN", "PSEXEC", f"Error: {e}")
    return False


def _try_wmic(vbs_local_path, hostname, username, password,
              jid, qid, mid, mname):
    wmic_cmd = f'cscript //NoLogo "{vbs_local_path}"'
    cmd = ["wmic", f"/node:{hostname}"]
    if username:
        cmd.append(f"/user:{username}")
    if password:
        cmd.append(f"/password:{password}")
    cmd += ["process", "call", "create", wmic_cmd]

    D.add_log(jid, qid, mid, "INFO", "WMIC", f"wmic /node:{hostname}")
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        out = (r.stdout + r.stderr).strip()
        if "ReturnValue = 0" in out:
            D.add_log(jid, qid, mid, "INFO", "WMIC",
                      f"Process created on {hostname}")
            return True
        D.add_log(jid, qid, mid, "WARN", "WMIC", f"{out[:150]}")
    except Exception as e:
        D.add_log(jid, qid, mid, "WARN", "WMIC", f"Error: {e}")
    return False


# =====================================================================
#  POLL RESULT FILE (via UNC)
# =====================================================================

def _poll_result(result_unc_path, timeout_secs, jid, qid, mid, mname):
    start = time.time()
    last_log = 0

    while (time.time() - start) < timeout_secs:
        if _kill_jobs.get(jid):
            return False

        try:
            if os.path.exists(result_unc_path):
                with open(result_unc_path, "r", encoding="utf-8") as f:
                    content = f.read().strip()
                if content and content != "RUNNING":
                    return True
        except:
            pass

        elapsed = time.time() - start
        if elapsed - last_log >= 60:
            D.add_log(jid, qid, mid, "INFO", "WAITING",
                      f"{mname}: macro running ({int(elapsed)}s)")
            last_log = elapsed

        time.sleep(5)

    return False


# =====================================================================
#  COLLECT OUTPUT
# =====================================================================

def _collect_output(unc_folder, files, compile_dir, today, group_name,
                    mname, cat, jid, qid, mid):
    original_names = {f["original_name"] for f in files}
    skip_prefixes = ("_run_", "_result_", "_macro_", "~$")
    new_files = []

    try:
        for fname in os.listdir(unc_folder):
            if fname in original_names:
                continue
            if any(fname.startswith(p) for p in skip_prefixes):
                continue
            fpath = os.path.join(unc_folder, fname)
            if not os.path.isfile(fpath):
                continue
            new_files.append(fname)
    except Exception as e:
        D.add_log(jid, qid, mid, "WARN", "COLLECT", f"Cannot list: {e}")
        return []

    if new_files:
        out_dir = os.path.join(compile_dir, today, group_name, mname, cat)
        os.makedirs(out_dir, exist_ok=True)
        for nf in new_files:
            try:
                shutil.copy2(os.path.join(unc_folder, nf),
                             os.path.join(out_dir, nf))
                sz = os.path.getsize(os.path.join(out_dir, nf)) / 1024
                D.add_log(jid, qid, mid, "INFO", "OUTPUT",
                          f"Collected: {nf} ({sz:.1f}KB)")
            except Exception as e:
                D.add_log(jid, qid, mid, "WARN", "OUTPUT", f"{nf}: {e}")

    return new_files


# =====================================================================
#  TRACKER CSV
# =====================================================================

def track_collected_files(jid):
    job = D.get_job(jid)
    if not job:
        return
    today = datetime.now().strftime("%Y-%m-%d")
    group_name = job["group_name"]
    compile_base = D.get_setting("compile_path", "") or COMPILED_DIR
    compile_root = os.path.join(compile_base, today, group_name)
    try:
        os.makedirs(compile_root, exist_ok=True)
        tp = os.path.join(compile_root,
                          f"TRACKER_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        rows = []
        for item in D.get_queue(jid):
            d = dict(item)
            if d.get("output_files"):
                try:
                    for fn in json.loads(d["output_files"]):
                        rows.append({
                            "job_id": jid, "date": today, "group": group_name,
                            "machine": d.get("machine_name") or "?",
                            "category": d["cat_value"], "filename": fn,
                            "collected_at": datetime.now().isoformat()})
                except:
                    pass
        with open(tp, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=[
                "job_id","date","group","machine","category","filename","collected_at"])
            w.writeheader()
            w.writerows(rows)
        D.add_log(jid, level="INFO", step="TRACKER",
                  message=f"{os.path.basename(tp)} ({len(rows)} files)")
    except Exception as e:
        D.add_log(jid, level="WARN", step="TRACKER", message=f"Failed: {e}")
