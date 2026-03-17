"""
Executor v9 - Guaranteed execution via Python-side drive mapping.

PROBLEM: Excel COM cannot open files from UNC paths.
SOLUTION: Python maps UNC to a drive letter BEFORE launching cscript.
          VBS sees Z: drive (local path). Always works.

FLOW:
  PREP: authenticate, copy files, map drive letter
  PER CATEGORY: write VBS, run cscript from mapped drive, poll result, collect output
  CLEANUP: unmap drive after all categories done

  60 cats / 40 machines = 40 parallel, 20 queued.
"""
import os, json, shutil, subprocess, threading, time, csv, string
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import database as D
import notifier as N

COMPILED_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "compiled_output")
_running_jobs = {}
_kill_jobs = {}
_drive_lock = threading.Lock()  # protect drive letter allocation


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


def test_machine(mid):
    """Test connectivity to a machine. Returns list of (step, ok, message)."""
    m = D.get_machine(mid)
    if not m:
        return [("LOAD", False, "Machine not found")]
    results = []
    shared = m["shared_folder"].strip()
    hostname = (m["system_name"] or "").strip()
    username = (m["username"] or "").strip()
    password = (m["password"] or "").strip()

    # Test 1: Disconnect old connections, then map drive with creds
    if shared.startswith("\\\\"):
        _disconnect_server(shared)

    drive = None
    if shared.startswith("\\\\") and username and password:
        drive = _find_free_drive()
        if drive:
            ok = _map_drive(drive, shared, username, password)
            if ok:
                results.append(("MAP_DRIVE", True, f"Mapped {drive} to {shared} (Excel will use this)"))
            else:
                results.append(("MAP_DRIVE", False, f"Cannot map drive to {shared}. Check username/password."))
                drive = None
        else:
            results.append(("MAP_DRIVE", False, "No free drive letters (Z: through M: all in use)"))

    # Test 2: Access share (via drive or UNC)
    test_path = (drive + "\\") if drive else shared
    try:
        os.makedirs(test_path, exist_ok=True)
        results.append(("ACCESS", True, f"Can access {test_path}"))
    except Exception as e:
        results.append(("ACCESS", False, f"Cannot access {test_path}: {e}"))
        if drive:
            _unmap_drive(drive)
        return results

    # Test 3: Write/read file
    test_file = os.path.join(test_path, "_test_connectivity.txt")
    try:
        with open(test_file, "w") as f:
            f.write("test")
        with open(test_file, "r") as f:
            assert f.read() == "test"
        os.remove(test_file)
        results.append(("WRITE_FILE", True, "Can write/read files"))
    except Exception as e:
        results.append(("WRITE_FILE", False, f"Cannot write: {e}"))

    # Cleanup test drive
    if drive:
        _unmap_drive(drive)

    # Test 4: schtasks (optional - for remote execution)
    if hostname and username and password:
        try:
            cmd = ["schtasks", "/query", "/s", hostname, "/u", username, "/p", password, "/tn", "\\"]
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            if r.returncode == 0:
                results.append(("SCHTASKS", True, f"Remote execution available on {hostname}"))
            else:
                err = (r.stderr.strip() or r.stdout.strip())[:100]
                results.append(("SCHTASKS", False, f"schtasks failed: {err}. Drive mapping will be used instead (still works)."))
        except Exception as e:
            results.append(("SCHTASKS", False, f"schtasks error: {e}. Drive mapping will be used instead."))
    elif not hostname:
        results.append(("SCHTASKS", False, "No System Name set. Drive mapping will be used."))

    return results


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

        # == PHASE 1: Prep machines (auth + copy + map drive) ==
        machine_ready = {}  # mid -> (unc_folder, drive_letter, can_schtasks)
        with ThreadPoolExecutor(max_workers=min(len(machines), 8)) as pool:
            futs = {pool.submit(_prep_machine, m, today, files, jid): m for m in machines}
            for fut in as_completed(futs):
                m = futs[fut]
                try:
                    unc_folder, drive_letter, can_schtasks = fut.result()
                    machine_ready[m["machine_id"]] = (unc_folder, drive_letter, can_schtasks)
                except Exception as e:
                    D.add_log(jid, machine_id=m["machine_id"], level="ERROR",
                              step="PREP_FAIL", message=f"{m['machine_name']}: {e}")

        if not machine_ready:
            D.add_log(jid, level="ERROR", step="ABORT", message="No machines ready")
            D.finish_job(jid)
            return

        # == PHASE 2: Workers process categories ==
        def machine_worker(mid, unc_folder, drive_letter, can_schtasks):
            m = D.get_machine(mid)
            mname = m["machine_name"]
            hostname = (m["system_name"] or "").strip()
            username = (m["username"] or "").strip()
            password = (m["password"] or "").strip()
            remote_path = (m["remote_path"] or "").strip()
            # Drive maps to SHARE root, so add today for date folder
            if drive_letter:
                work_folder = os.path.join(drive_letter + "\\", today)
            else:
                work_folder = unc_folder

            while not _kill_jobs.get(jid):
                item = D.claim_next(jid, mid)
                if not item:
                    break

                qid = item["queue_id"]
                cat = item["cat_value"]
                start = datetime.now()
                task_name = f"MQ_{jid}_{qid}"

                D.add_log(jid, qid, mid, "INFO", "CAT_START",
                          f"{mname}: '{cat}' (drive={drive_letter or 'UNC'})")

                vbs_work = None
                result_unc = None

                try:
                    safe_cat = str(cat).replace('"', '""')
                    vbs_name = f"_run_{qid}.vbs"
                    result_name = f"_result_{qid}.txt"

                    # Paths
                    vbs_work = os.path.join(work_folder, vbs_name)
                    result_unc = os.path.join(unc_folder, result_name)
                    result_work = os.path.join(work_folder, result_name)

                    # Clean old result
                    for rp in [result_unc, result_work]:
                        try:
                            os.remove(rp)
                        except:
                            pass

                    # Write VBS to work folder (drive letter or UNC)
                    vbs_code = _make_vbs(excel_file, macro_name, target_cell,
                                         safe_cat, result_name)
                    with open(vbs_work, "w", encoding="utf-8") as f:
                        f.write(vbs_code)

                    D.add_log(jid, qid, mid, "INFO", "VBS_READY",
                              f"VBS at {vbs_work}")

                    # Execute
                    if can_schtasks and hostname and remote_path:
                        # REMOTE: schtasks with local path on remote PC
                        local_vbs = os.path.join(remote_path, today, vbs_name)
                        _run_via_schtasks(local_vbs, hostname, username, password,
                                          task_name, jid, qid, mid, mname)
                    else:
                        # LOCAL: cscript from mapped drive (guaranteed to work)
                        D.add_log(jid, qid, mid, "INFO", "EXEC",
                                  f"cscript {vbs_work}")
                        subprocess.Popen(
                            ["cscript", "//NoLogo", vbs_work],
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL,
                            creationflags=0x08000000  # CREATE_NO_WINDOW
                        )

                    # Poll result
                    timeout_secs = int(D.get_setting("macro_timeout", "1800"))
                    got = _poll_result(result_work, result_unc, timeout_secs,
                                       jid, qid, mid, mname)

                    if not got:
                        raise RuntimeError(f"Timeout ({timeout_secs}s) - macro did not finish")

                    # Read result
                    result_text = _read_result(result_work, result_unc)

                    if result_text.startswith("ERROR"):
                        raise RuntimeError(result_text)

                    D.add_log(jid, qid, mid, "INFO", "MACRO_DONE",
                              f"{mname}: '{cat}' -> {result_text[:80]}")

                    # Collect output
                    new_files = _collect_output(unc_folder, files, compile_dir,
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
                              f"{mname}: '{cat}' FAILED: {e}")
                    N.notify_macro_failure(jid, qid, mname,
                                           m["ip_address"], excel_file, macro_name, str(e), cat)
                finally:
                    # Cleanup VBS + result
                    for fp in [vbs_work, result_work, result_unc]:
                        if fp:
                            try:
                                os.remove(fp)
                            except:
                                pass
                    # Cleanup schtask
                    if can_schtasks and hostname:
                        try:
                            subprocess.run(["schtasks", "/delete", "/s", hostname,
                                            "/tn", task_name, "/f"],
                                           capture_output=True, timeout=10)
                        except:
                            pass

            # Worker done - unmap drive
            if drive_letter:
                _unmap_drive(drive_letter)
                D.add_log(jid, machine_id=mid, level="INFO", step="UNMAP",
                          message=f"Unmapped {drive_letter}")

        # Launch workers
        with ThreadPoolExecutor(max_workers=max(len(machine_ready), 1)) as pool:
            futs = [pool.submit(machine_worker, mid, unc, drv, sch)
                    for mid, (unc, drv, sch) in machine_ready.items()]
            for w in as_completed(futs):
                try:
                    w.result()
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

        _track(jid)

        # Email if failures
        job = D.get_job(jid)
        if (job["failed_cats"] or 0) > 0:
            fails = [{"machine": q["machine_name"] or "?", "ip": q["ip_address"] or "",
                       "step": f"CAT:{q['cat_value']}", "error": q["error_message"] or ""}
                     for q in D.get_queue(jid) if q["status"] == "FAILED"]
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
#  PREP MACHINE: auth + copy + map drive
# =====================================================================

def _prep_machine(machine, today, files, jid):
    shared = machine["shared_folder"].strip()
    hostname = (machine["system_name"] or "").strip()
    username = (machine["username"] or "").strip()
    password = (machine["password"] or "").strip()
    mid = machine["machine_id"]
    mname = machine["machine_name"]

    # STEP 1: Disconnect ALL existing connections to this server
    # (prevents "Access denied" when mapping drive)
    if shared.startswith("\\\\"):
        _disconnect_server(shared, jid, mid)

    # STEP 2: Map drive letter to the SHARE (with credentials)
    # This is the ONLY connection to the server - no conflict
    drive_letter = None
    if shared.startswith("\\\\") and username and password:
        drive_letter = _find_free_drive()
        if drive_letter:
            ok = _map_drive(drive_letter, shared, username, password, jid, mid)
            if ok:
                D.add_log(jid, machine_id=mid, level="INFO", step="DRIVE",
                          message=f"{mname}: {drive_letter} -> {shared}")
            else:
                drive_letter = None

    # STEP 3: Create date folder (via drive letter or UNC)
    if drive_letter:
        work_root = drive_letter + "\\"
        date_folder_drive = os.path.join(work_root, today)
        os.makedirs(date_folder_drive, exist_ok=True)
    else:
        # Fallback: try UNC directly (might work if same domain)
        if username and password:
            _net_use_auth(shared, username, password, jid, mid)
        unc_folder = os.path.join(shared, today)
        os.makedirs(unc_folder, exist_ok=True)

    # UNC path for server-side polling
    unc_folder = os.path.join(shared, today)

    # STEP 4: Copy files (via drive letter if available)
    copy_target = os.path.join(drive_letter + "\\", today) if drive_letter else unc_folder
    D.add_log(jid, machine_id=mid, level="INFO", step="COPY",
              message=f"{mname}: copying {len(files)} files to {copy_target}")

    def cp(f):
        dst = os.path.join(copy_target, f["original_name"])
        shutil.copy2(f["stored_path"], dst)
        sz = os.path.getsize(dst) / 1024 / 1024
        D.add_log(jid, machine_id=mid, level="INFO", step="COPY_OK",
                  message=f"{f['original_name']} ({sz:.1f}MB)")

    with ThreadPoolExecutor(max_workers=min(len(files), 4)) as pool:
        futs = [pool.submit(cp, f) for f in files]
        for fut in as_completed(futs):
            fut.result()

    # STEP 5: Check schtasks
    can_schtasks = False
    remote_path = (machine["remote_path"] or "").strip()
    if hostname and remote_path and username and password:
        try:
            r = subprocess.run(
                ["schtasks", "/query", "/s", hostname, "/u", username, "/p", password,
                 "/tn", "\\"],
                capture_output=True, text=True, timeout=10)
            can_schtasks = (r.returncode == 0)
            if can_schtasks:
                D.add_log(jid, machine_id=mid, level="INFO", step="SCHTASKS",
                          message=f"{mname}: remote execution available")
        except:
            pass

    method = "schtasks" if can_schtasks else (f"drive:{drive_letter}" if drive_letter else "UNC")
    D.add_log(jid, machine_id=mid, level="INFO", step="PREP_OK",
              message=f"{mname}: ready (method={method})")

    return unc_folder, drive_letter, can_schtasks


# =====================================================================
#  DRIVE MAPPING (Python-side, not VBS)
# =====================================================================

def _find_free_drive():
    """Find a free drive letter Z..M."""
    with _drive_lock:
        for letter in "ZYXWVUTSRQPONM":
            drive = f"{letter}:"
            if not os.path.exists(drive + "\\"):
                return drive
    return None


def _disconnect_server(unc_path, jid=None, mid=None):
    """Disconnect ALL existing connections to a server (share + drive letters).
    This prevents 'Access denied' when mapping a new drive letter."""
    clean = unc_path.replace("/", "\\").rstrip("\\")
    parts = [p for p in clean.split("\\") if p]
    if not parts:
        return

    server_share = f"\\\\{parts[0]}\\{parts[1]}" if len(parts) >= 2 else f"\\\\{parts[0]}"

    # Delete the share connection
    try:
        subprocess.run(["net", "use", server_share, "/delete", "/y"],
                       capture_output=True, timeout=10)
    except:
        pass

    # Also try deleting with just the server name (catches wildcard connections)
    try:
        subprocess.run(["net", "use", f"\\\\{parts[0]}", "/delete", "/y"],
                       capture_output=True, timeout=10)
    except:
        pass

    time.sleep(0.5)

    if jid:
        D.add_log(jid, machine_id=mid, level="INFO", step="DISCONNECT",
                  message=f"Cleared connections to {server_share}")


def _map_drive(drive, unc_path, username="", password="", jid=None, mid=None):
    """Map drive letter to UNC path with credentials. Returns True if OK."""
    # Disconnect this drive letter if already mapped
    try:
        subprocess.run(["net", "use", drive, "/delete", "/y"],
                       capture_output=True, timeout=10)
    except:
        pass
    time.sleep(0.3)

    # Map with credentials
    cmd = ["net", "use", drive, unc_path, "/persistent:no"]
    if username and password:
        cmd += [f"/user:{username}", password]

    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        if r.returncode == 0:
            return True
        err = (r.stderr.strip() or r.stdout.strip())[:150]
        if jid:
            D.add_log(jid, machine_id=mid, level="WARN", step="MAP",
                      message=f"net use {drive} {unc_path} failed: {err}")
    except Exception as e:
        if jid:
            D.add_log(jid, machine_id=mid, level="WARN", step="MAP",
                      message=f"Map error: {e}")
    return False


def _unmap_drive(drive):
    """Unmap a drive letter."""
    try:
        subprocess.run(["net", "use", drive, "/delete", "/y"],
                       capture_output=True, timeout=10)
    except:
        pass


def _net_use_auth(unc_path, username, password, jid=None, mid=None):
    """Authenticate to share WITHOUT mapping a drive. Fallback only."""
    clean = unc_path.replace("/", "\\").rstrip("\\")
    parts = [p for p in clean.split("\\") if p]
    if len(parts) >= 2:
        share = f"\\\\{parts[0]}\\{parts[1]}"
    else:
        return

    r = subprocess.run(
        ["net", "use", share, f"/user:{username}", password, "/persistent:no"],
        capture_output=True, text=True, timeout=15)
    if jid:
        if r.returncode == 0:
            D.add_log(jid, machine_id=mid, level="INFO", step="AUTH",
                      message=f"Authenticated to {share}")
        else:
            err = (r.stderr.strip() or r.stdout.strip())[:150]
            D.add_log(jid, machine_id=mid, level="WARN", step="AUTH",
                      message=f"Auth failed: {err}")


# =====================================================================
#  VBS - DEAD SIMPLE. No drive mapping. Just opens from scriptFolder.
# =====================================================================

def _make_vbs(excel_file, macro_name, target_cell, cat_value, result_filename):
    """
    VBS uses WScript.ScriptFullName to find its folder.
    When cscript runs from Z:\\_run_42.vbs, scriptFolder = Z:\\
    Excel opens Z:\\file.xlsb = drive letter = WORKS.
    """
    return f'''Dim fso, scriptFolder, excelPath, resultPath
Dim xlApp, xlWb

Set fso = CreateObject("Scripting.FileSystemObject")
scriptFolder = fso.GetParentFolderName(WScript.ScriptFullName)
If Right(scriptFolder, 1) <> "\\" Then scriptFolder = scriptFolder & "\\"

excelPath = scriptFolder & "{excel_file}"
resultPath = scriptFolder & "{result_filename}"

Call WriteR(resultPath, "RUNNING")

If Not fso.FileExists(excelPath) Then
    Call WriteR(resultPath, "ERROR_OPEN:File not found: " & excelPath)
    WScript.Quit 1
End If

On Error Resume Next
Set xlApp = CreateObject("Excel.Application")
If Err.Number <> 0 Then
    Call WriteR(resultPath, "ERROR_OPEN:Cannot start Excel: " & Err.Description)
    WScript.Quit 1
End If
On Error GoTo 0

xlApp.Visible = False
xlApp.DisplayAlerts = False
xlApp.AskToUpdateLinks = False
xlApp.EnableEvents = False

On Error Resume Next
Set xlWb = xlApp.Workbooks.Open(excelPath, 0, False)
If Err.Number <> 0 Then
    Call WriteR(resultPath, "ERROR_OPEN:" & Err.Number & ":" & Err.Description & " Path=" & excelPath)
    xlApp.Quit: Set xlApp = Nothing
    WScript.Quit 1
End If
On Error GoTo 0

On Error Resume Next
xlWb.Sheets(1).Range("{target_cell}").Value = "{cat_value}"
If Err.Number <> 0 Then
    Call WriteR(resultPath, "ERROR_PASTE:" & Err.Number & ":" & Err.Description)
    xlWb.Close False: xlApp.Quit: Set xlApp = Nothing
    WScript.Quit 1
End If
On Error GoTo 0

On Error Resume Next
xlApp.Run "{macro_name}"
If Err.Number <> 0 Then
    Call WriteR(resultPath, "ERROR_MACRO:" & Err.Number & ":" & Err.Description)
    xlWb.Close False: xlApp.Quit: Set xlApp = Nothing
    WScript.Quit 1
End If
On Error GoTo 0

On Error Resume Next
xlWb.Save
xlWb.Close False
xlApp.Quit
Set xlApp = Nothing
On Error GoTo 0

Call WriteR(resultPath, "SUCCESS")
WScript.Quit 0

Sub WriteR(p, m)
    Dim f
    Set f = CreateObject("Scripting.FileSystemObject").CreateTextFile(p, True)
    f.Write m
    f.Close
End Sub
'''


# =====================================================================
#  REMOTE EXECUTION via schtasks
# =====================================================================

def _run_via_schtasks(vbs_local_path, hostname, username, password,
                      task_name, jid, qid, mid, mname):
    """Create + run scheduled task on remote machine using LOCAL path."""
    task_cmd = f'cscript //NoLogo "{vbs_local_path}"'

    create = ["schtasks", "/create", "/s", hostname,
              "/tn", task_name, "/tr", task_cmd,
              "/sc", "once", "/st", "00:00", "/f", "/rl", "highest"]
    if username:
        create += ["/ru", username]
    if password:
        create += ["/rp", password]

    D.add_log(jid, qid, mid, "INFO", "SCHTASKS",
              f"Creating task on {hostname}")

    try:
        r = subprocess.run(create, capture_output=True, text=True, timeout=15)
        if r.returncode != 0:
            err = (r.stderr.strip() or r.stdout.strip())[:150]
            D.add_log(jid, qid, mid, "WARN", "SCHTASKS", f"Create failed: {err}")
            return False

        run_cmd = ["schtasks", "/run", "/s", hostname, "/tn", task_name]
        r2 = subprocess.run(run_cmd, capture_output=True, text=True, timeout=15)
        if r2.returncode != 0:
            err = (r2.stderr.strip() or r2.stdout.strip())[:150]
            D.add_log(jid, qid, mid, "WARN", "SCHTASKS", f"Run failed: {err}")
            return False

        D.add_log(jid, qid, mid, "INFO", "SCHTASKS", f"Running on {hostname}")
        return True
    except Exception as e:
        D.add_log(jid, qid, mid, "WARN", "SCHTASKS", f"Error: {e}")
        return False


# =====================================================================
#  POLL + READ RESULT
# =====================================================================

def _poll_result(path1, path2, timeout_secs, jid, qid, mid, mname):
    """Poll result file. Tries both paths (drive and UNC)."""
    start = time.time()
    last_log = 0

    while (time.time() - start) < timeout_secs:
        if _kill_jobs.get(jid):
            return False

        for p in [path1, path2]:
            try:
                if p and os.path.exists(p):
                    with open(p, "r", encoding="utf-8") as f:
                        c = f.read().strip()
                    if c and c != "RUNNING":
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


def _read_result(path1, path2):
    """Read result from first available path."""
    for p in [path1, path2]:
        try:
            if p and os.path.exists(p):
                with open(p, "r", encoding="utf-8") as f:
                    return f.read().strip()
        except:
            pass
    return "UNKNOWN"


# =====================================================================
#  COLLECT OUTPUT
# =====================================================================

def _collect_output(unc_folder, files, compile_dir, today, group_name,
                    mname, cat, jid, qid, mid):
    original = {f["original_name"] for f in files}
    skip = ("_run_", "_result_", "_macro_", "~$")
    found = []

    try:
        for fn in os.listdir(unc_folder):
            if fn in original or any(fn.startswith(s) for s in skip):
                continue
            if os.path.isfile(os.path.join(unc_folder, fn)):
                found.append(fn)
    except Exception as e:
        D.add_log(jid, qid, mid, "WARN", "COLLECT", f"Cannot list: {e}")
        return []

    if found:
        out = os.path.join(compile_dir, today, group_name, mname, cat)
        os.makedirs(out, exist_ok=True)
        for fn in found:
            try:
                shutil.copy2(os.path.join(unc_folder, fn), os.path.join(out, fn))
                sz = os.path.getsize(os.path.join(out, fn)) / 1024
                D.add_log(jid, qid, mid, "INFO", "OUTPUT", f"{fn} ({sz:.1f}KB)")
            except Exception as e:
                D.add_log(jid, qid, mid, "WARN", "OUTPUT", f"{fn}: {e}")
    return found


# =====================================================================
#  TRACKER CSV
# =====================================================================

def _track(jid):
    job = D.get_job(jid)
    if not job:
        return
    today = datetime.now().strftime("%Y-%m-%d")
    gn = job["group_name"]
    base = D.get_setting("compile_path", "") or COMPILED_DIR
    root = os.path.join(base, today, gn)
    try:
        os.makedirs(root, exist_ok=True)
        tp = os.path.join(root, f"TRACKER_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        rows = []
        for item in D.get_queue(jid):
            d = dict(item)
            if d.get("output_files"):
                try:
                    for fn in json.loads(d["output_files"]):
                        rows.append({"job_id": jid, "date": today, "group": gn,
                                     "machine": d.get("machine_name") or "?",
                                     "category": d["cat_value"], "filename": fn})
                except:
                    pass
        with open(tp, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["job_id", "date", "group", "machine", "category", "filename"])
            w.writeheader()
            w.writerows(rows)
        D.add_log(jid, level="INFO", step="TRACKER",
                  message=f"{os.path.basename(tp)} ({len(rows)} files)")
    except Exception as e:
        D.add_log(jid, level="WARN", step="TRACKER", message=f"Failed: {e}")
