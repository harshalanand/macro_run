"""
Executor v7 - True remote execution via schtasks.

HOW IT WORKS:
  1. PREP: Copy files to each remote machine's shared folder ONCE
  2. PER CATEGORY:
     a. Write VBS to remote share (maps drive letter, opens Excel, runs macro)
     b. Create scheduled task on remote machine via schtasks
     c. Run the task (VBS executes ON the remote machine's Excel)
     d. Poll _result file via UNC path
     e. Collect output files
     f. Delete scheduled task
     g. Pick next QUEUED category

  60 cats / 40 machines = first 40 run in parallel, rest queue automatically.
  Each machine runs ONE category at a time.

EXECUTION CHAIN: schtasks -> psexec -> local cscript
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
#  MAIN JOB RUNNER
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
        machine_ready = {}
        with ThreadPoolExecutor(max_workers=min(len(machines), 8)) as pool:
            futs = {pool.submit(_prep_machine, m, today, files, jid): m for m in machines}
            for fut in as_completed(futs):
                m = futs[fut]
                try:
                    remote_folder = fut.result()
                    machine_ready[m["machine_id"]] = remote_folder
                except Exception as e:
                    D.add_log(jid, machine_id=m["machine_id"], level="ERROR",
                              step="PREP_FAIL", message=f"{m['machine_name']}: {e}")
                    N.notify_copy_failure(jid, None, m["machine_name"],
                                          m["ip_address"], m["shared_folder"], str(e), "PREP")

        if not machine_ready:
            D.add_log(jid, level="ERROR", step="ABORT", message="No machines ready")
            D.finish_job(jid)
            return

        # == PHASE 2: Each machine processes categories from queue ==
        def machine_worker(mid, remote_folder):
            m = D.get_machine(mid)
            mname = m["machine_name"]
            hostname = (m["system_name"] or m["ip_address"] or "").strip()
            username = (m["username"] or "").strip()
            password = (m["password"] or "").strip()

            while not _kill_jobs.get(jid):
                item = D.claim_next(jid, mid)
                if not item:
                    break  # No more categories in queue

                qid = item["queue_id"]
                cat = item["cat_value"]
                start = datetime.now()
                D.add_log(jid, qid, mid, "INFO", "CAT_START",
                          f"{mname}: processing '{cat}'")

                vbs_path = None
                result_path = None
                task_name = f"MacroQ_{jid}_{qid}"

                try:
                    safe_cat = str(cat).replace('"', '""')
                    vbs_name = f"_run_{qid}.vbs"
                    result_name = f"_result_{qid}.txt"
                    vbs_path = os.path.join(remote_folder, vbs_name)
                    result_path = os.path.join(remote_folder, result_name)

                    # Clean old result file
                    try:
                        os.remove(result_path)
                    except:
                        pass

                    # Write VBS to remote share
                    vbs_code = _make_vbs(excel_file, macro_name, target_cell,
                                         safe_cat, result_name)
                    with open(vbs_path, "w", encoding="utf-8") as f:
                        f.write(vbs_code)

                    D.add_log(jid, qid, mid, "INFO", "VBS_READY",
                              f"VBS: {vbs_path}")

                    # Execute VBS on the remote machine
                    _execute_on_remote(
                        vbs_path=vbs_path,
                        hostname=hostname,
                        username=username,
                        password=password,
                        task_name=task_name,
                        jid=jid, qid=qid, mid=mid, mname=mname
                    )

                    # Poll result file
                    timeout_secs = int(D.get_setting("macro_timeout", "1800"))
                    got_result = _poll_result(result_path, timeout_secs,
                                              jid, qid, mid, mname)

                    if not got_result:
                        raise RuntimeError(
                            f"Timeout ({timeout_secs}s) waiting for macro on {mname}")

                    # Read result
                    result_text = ""
                    try:
                        with open(result_path, "r", encoding="utf-8") as rf:
                            result_text = rf.read().strip()
                    except:
                        result_text = "UNKNOWN"

                    if result_text.startswith("ERROR"):
                        raise RuntimeError(result_text)

                    D.add_log(jid, qid, mid, "INFO", "MACRO_DONE",
                              f"{mname}: '{cat}' -> {result_text[:100]}")

                    # Collect output files
                    new_files = _collect_output(
                        remote_folder, files, compile_dir,
                        today, group_name, mname, cat,
                        jid, qid, mid)

                    elapsed = (datetime.now() - start).total_seconds()
                    D.finish_queue_item(qid, "SUCCESS",
                        finished_at=datetime.now().isoformat(),
                        date_folder=remote_folder,
                        duration_secs=elapsed,
                        output_files=json.dumps(new_files) if new_files else "")
                    D.add_log(jid, qid, mid, "INFO", "CAT_DONE",
                              f"{mname}: '{cat}' done in {elapsed:.1f}s "
                              f"({len(new_files)} outputs)")

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
                    # Cleanup VBS + result + scheduled task
                    for fp in [vbs_path, result_path]:
                        if fp:
                            try:
                                os.remove(fp)
                            except:
                                pass
                    # Delete remote scheduled task (fire and forget)
                    if hostname:
                        try:
                            subprocess.run(
                                ["schtasks", "/delete", "/s", hostname,
                                 "/tn", task_name, "/f"],
                                capture_output=True, timeout=10)
                        except:
                            pass

        # Launch one worker thread per machine
        with ThreadPoolExecutor(max_workers=len(machine_ready)) as pool:
            futs = [pool.submit(machine_worker, mid, rf)
                    for mid, rf in machine_ready.items()]
            for w in as_completed(futs):
                try:
                    w.result()
                except Exception as e:
                    D.add_log(jid, level="ERROR", step="WORKER_CRASH",
                              message=str(e))

        # Finalize job
        if _kill_jobs.get(jid):
            with D.db() as c:
                c.execute(
                    "UPDATE jobs SET status='KILLED',finished_at=? WHERE job_id=?",
                    (datetime.now().isoformat(), jid))
            D.add_log(jid, level="WARN", step="KILLED",
                      message=f"Job #{jid} killed")
        else:
            D.finish_job(jid)
            D.add_log(jid, level="INFO", step="JOB_DONE",
                      message=f"Job #{jid} completed")

        # Tracker CSV
        track_collected_files(jid)

        # Summary email if failures
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
            c.execute(
                "UPDATE jobs SET status='CRASHED',finished_at=? WHERE job_id=?",
                (datetime.now().isoformat(), jid))
    finally:
        _running_jobs.pop(jid, None)
        _kill_jobs.pop(jid, None)


# =====================================================================
#  PREP MACHINE (auth + create folder + copy files ONCE)
# =====================================================================

def _prep_machine(machine, today, files, jid):
    shared = machine["shared_folder"].strip()
    system_name = (machine["system_name"] or "").strip()
    username = (machine["username"] or "").strip()
    password = (machine["password"] or "").strip()
    mid = machine["machine_id"]
    mname = machine["machine_name"]

    # Authenticate to share
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

    # Copy files in parallel
    D.add_log(jid, machine_id=mid, level="INFO", step="PREP_COPY",
              message=f"{mname}: copying {len(files)} files")

    def copy_one(f):
        dst = os.path.join(date_folder, f["original_name"])
        shutil.copy2(f["stored_path"], dst)
        sz = os.path.getsize(dst) / 1024 / 1024
        D.add_log(jid, machine_id=mid, level="INFO", step="FILE_OK",
                  message=f"{f['original_name']} ({sz:.1f}MB)")

    with ThreadPoolExecutor(max_workers=min(len(files), 4)) as pool:
        futs = [pool.submit(copy_one, f) for f in files]
        for fut in as_completed(futs):
            fut.result()

    D.add_log(jid, machine_id=mid, level="INFO", step="PREP_OK",
              message=f"{mname}: ready ({date_folder})")
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
                  message=f"net use failed: {err}")
    else:
        D.add_log(jid, machine_id=mid, level="INFO", step="NET_USE",
                  message=f"OK: {server_share}")


# =====================================================================
#  VBS GENERATION - BULLETPROOF
#  No blanket On Error Resume Next. Targeted error handling only.
#  Maps drive letter if UNC path. ALWAYS writes result file.
# =====================================================================

def _make_vbs(excel_file, macro_name, target_cell, cat_value, result_filename):
    return f'''
Dim fso, scriptFolder, workFolder, excelPath, resultPath, mappedDrive
Set fso = CreateObject("Scripting.FileSystemObject")

' Get script's own folder
scriptFolder = fso.GetParentFolderName(WScript.ScriptFullName)
If Right(scriptFolder, 1) <> "\\" Then scriptFolder = scriptFolder & "\\"

' Result file is ALWAYS written to script folder (server polls this via UNC)
resultPath = scriptFolder & "{result_filename}"

' Write RUNNING marker
Call WriteResult("RUNNING")

' === STEP 1: Map drive letter if UNC path ===
workFolder = scriptFolder
mappedDrive = ""

If Left(scriptFolder, 2) = "\\\\" Then
    Dim letters, dl, wshShell
    letters = Array("Z","Y","X","W","V","U","T","S","R","Q","P","O","N","M")
    Set wshShell = CreateObject("WScript.Shell")

    For Each dl In letters
        If Not fso.DriveExists(dl & ":") Then
            Dim mapResult
            mapResult = wshShell.Run("cmd /c net use " & dl & ": """ & Left(scriptFolder, Len(scriptFolder)-1) & """ /persistent:no", 0, True)
            WScript.Sleep 1000
            If fso.DriveExists(dl & ":") Then
                mappedDrive = dl & ":"
                workFolder = mappedDrive & "\\"
                Exit For
            End If
        End If
    Next

    If mappedDrive = "" Then
        Call WriteResult("ERROR_DRIVE:Could not map any drive letter for UNC path")
        WScript.Quit 1
    End If
End If

excelPath = workFolder & "{excel_file}"

' === STEP 2: Verify Excel file exists ===
If Not fso.FileExists(excelPath) Then
    Call WriteResult("ERROR_OPEN:File not found: " & excelPath)
    Call UnmapDrive
    WScript.Quit 1
End If

' === STEP 3: Start Excel ===
On Error Resume Next
Dim xlApp
Set xlApp = CreateObject("Excel.Application")
If Err.Number <> 0 Then
    Call WriteResult("ERROR_OPEN:Cannot start Excel: " & Err.Description)
    On Error GoTo 0
    Call UnmapDrive
    WScript.Quit 1
End If
On Error GoTo 0

xlApp.Visible = False
xlApp.DisplayAlerts = False
xlApp.AskToUpdateLinks = False
xlApp.EnableEvents = False

' === STEP 4: Open workbook ===
On Error Resume Next
Dim xlWb
Set xlWb = xlApp.Workbooks.Open(excelPath, 0, False)
If Err.Number <> 0 Then
    Dim openErr
    openErr = "ERROR_OPEN:" & Err.Number & ":" & Err.Description & " [" & excelPath & "]"
    On Error GoTo 0
    Call WriteResult(openErr)
    xlApp.Quit: Set xlApp = Nothing
    Call UnmapDrive
    WScript.Quit 1
End If
On Error GoTo 0

' === STEP 5: Paste category value ===
On Error Resume Next
xlWb.Sheets(1).Range("{target_cell}").Value = "{cat_value}"
If Err.Number <> 0 Then
    Dim pasteErr
    pasteErr = "ERROR_PASTE:" & Err.Number & ":" & Err.Description
    On Error GoTo 0
    Call WriteResult(pasteErr)
    xlWb.Close False: xlApp.Quit: Set xlApp = Nothing
    Call UnmapDrive
    WScript.Quit 1
End If
On Error GoTo 0

' === STEP 6: Run macro ===
On Error Resume Next
xlApp.Run "{macro_name}"
If Err.Number <> 0 Then
    Dim macroErr
    macroErr = "ERROR_MACRO:" & Err.Number & ":" & Err.Description
    On Error GoTo 0
    Call WriteResult(macroErr)
    xlWb.Close False: xlApp.Quit: Set xlApp = Nothing
    Call UnmapDrive
    WScript.Quit 1
End If
On Error GoTo 0

' === STEP 7: Save and close ===
On Error Resume Next
xlWb.Save
xlWb.Close False
xlApp.Quit
Set xlApp = Nothing
On Error GoTo 0

' === DONE ===
Call WriteResult("SUCCESS")
Call UnmapDrive
WScript.Quit 0


' ---- Helper: Write result file ----
Sub WriteResult(msg)
    Dim f
    Set f = fso.CreateTextFile(resultPath, True)
    f.Write msg
    f.Close
End Sub

' ---- Helper: Unmap drive ----
Sub UnmapDrive()
    If mappedDrive <> "" Then
        Dim sh
        Set sh = CreateObject("WScript.Shell")
        sh.Run "cmd /c net use " & mappedDrive & " /delete /y", 0, True
    End If
End Sub
'''


# =====================================================================
#  EXECUTE VBS ON REMOTE MACHINE
#  Priority: schtasks -> psexec -> local cscript
# =====================================================================

def _execute_on_remote(vbs_path, hostname, username, password, task_name,
                       jid, qid, mid, mname):
    """Execute VBS on the target machine."""
    vbs_win = vbs_path.replace("/", "\\")

    # Detect if target is the local machine
    is_local = _is_local_machine(hostname)

    if is_local or not hostname:
        D.add_log(jid, qid, mid, "INFO", "EXEC_LOCAL",
                  f"Local: cscript {vbs_win}")
        subprocess.Popen(
            ["cscript", "//NoLogo", vbs_win],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
        return

    # === REMOTE MACHINE ===

    # Method 1: schtasks (built into every Windows, no extra tools)
    ok = _try_schtasks(vbs_win, hostname, username, password, task_name,
                       jid, qid, mid, mname)
    if ok:
        return

    # Method 2: psexec (if installed)
    psexec_path = D.get_setting("psexec_path", "psexec")
    ok = _try_psexec(vbs_win, hostname, username, password, psexec_path,
                     jid, qid, mid, mname)
    if ok:
        return

    # Method 3: wmic
    ok = _try_wmic(vbs_win, hostname, username, password,
                   jid, qid, mid, mname)
    if ok:
        return

    # Fallback: local cscript (macro runs on server, not ideal)
    D.add_log(jid, qid, mid, "WARN", "EXEC_FALLBACK",
              f"All remote methods failed. Running locally on server.")
    subprocess.Popen(
        ["cscript", "//NoLogo", vbs_win],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))


def _is_local_machine(hostname):
    """Check if hostname refers to this machine."""
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


def _try_schtasks(vbs_path, hostname, username, password, task_name,
                  jid, qid, mid, mname):
    """
    Create + run a scheduled task on the remote machine.
    Task runs cscript with the VBS. This ensures VBS runs ON the remote PC.
    """
    task_cmd = f'cscript //NoLogo "{vbs_path}"'

    # Create task on remote machine
    create_cmd = [
        "schtasks", "/create",
        "/s", hostname,
        "/tn", task_name,
        "/tr", task_cmd,
        "/sc", "once",
        "/st", "00:00",
        "/f",  # force overwrite
        "/rl", "highest",  # run with highest privileges
    ]
    if username:
        create_cmd += ["/ru", username]
        if password:
            create_cmd += ["/rp", password]

    D.add_log(jid, qid, mid, "INFO", "EXEC_SCHTASKS",
              f"Creating task '{task_name}' on {hostname}")

    try:
        r = subprocess.run(create_cmd, capture_output=True, text=True, timeout=15)
        if r.returncode != 0:
            err = (r.stderr.strip() or r.stdout.strip())[:200]
            D.add_log(jid, qid, mid, "WARN", "EXEC_SCHTASKS",
                      f"Create failed: {err}")
            return False

        # Run the task
        run_cmd = ["schtasks", "/run", "/s", hostname, "/tn", task_name]
        r2 = subprocess.run(run_cmd, capture_output=True, text=True, timeout=15)
        if r2.returncode != 0:
            err = (r2.stderr.strip() or r2.stdout.strip())[:200]
            D.add_log(jid, qid, mid, "WARN", "EXEC_SCHTASKS",
                      f"Run failed: {err}")
            return False

        D.add_log(jid, qid, mid, "INFO", "EXEC_SCHTASKS",
                  f"Task running on {hostname}")
        return True

    except Exception as e:
        D.add_log(jid, qid, mid, "WARN", "EXEC_SCHTASKS", f"Error: {e}")
        return False


def _try_psexec(vbs_path, hostname, username, password, psexec_path,
                jid, qid, mid, mname):
    """Execute via PsExec with -i (interactive session for Excel)."""
    cmd = [psexec_path, f"\\\\{hostname}", "-accepteula", "-d", "-i"]
    if username:
        cmd += ["-u", username]
    if password:
        cmd += ["-p", password]
    cmd += ["cscript", "//NoLogo", vbs_path]

    D.add_log(jid, qid, mid, "INFO", "EXEC_PSEXEC",
              f"PsExec -> {hostname}")
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if r.returncode in (0, -1):
            D.add_log(jid, qid, mid, "INFO", "EXEC_PSEXEC",
                      f"Launched on {hostname}")
            return True
        D.add_log(jid, qid, mid, "WARN", "EXEC_PSEXEC",
                  f"exit={r.returncode}: {(r.stderr or r.stdout)[:150]}")
    except FileNotFoundError:
        D.add_log(jid, qid, mid, "WARN", "EXEC_PSEXEC",
                  f"psexec not found at '{psexec_path}'")
    except Exception as e:
        D.add_log(jid, qid, mid, "WARN", "EXEC_PSEXEC", f"Error: {e}")
    return False


def _try_wmic(vbs_path, hostname, username, password,
              jid, qid, mid, mname):
    """Execute via wmic process call create."""
    wmic_cmd = f'cscript //NoLogo "{vbs_path}"'
    cmd = ["wmic", f"/node:{hostname}"]
    if username:
        cmd.append(f"/user:{username}")
    if password:
        cmd.append(f"/password:{password}")
    cmd += ["process", "call", "create", wmic_cmd]

    D.add_log(jid, qid, mid, "INFO", "EXEC_WMIC",
              f"wmic /node:{hostname} ...")
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        out = (r.stdout + r.stderr).strip()
        if "ReturnValue = 0" in out:
            D.add_log(jid, qid, mid, "INFO", "EXEC_WMIC",
                      f"Process created on {hostname}")
            return True
        D.add_log(jid, qid, mid, "WARN", "EXEC_WMIC",
                  f"wmic: {out[:150]}")
    except Exception as e:
        D.add_log(jid, qid, mid, "WARN", "EXEC_WMIC", f"Error: {e}")
    return False


# =====================================================================
#  POLL RESULT FILE
# =====================================================================

def _poll_result(result_path, timeout_secs, jid, qid, mid, mname):
    """Poll _result file every 5s. Returns True when done."""
    start = time.time()
    last_log = 0

    while (time.time() - start) < timeout_secs:
        if _kill_jobs.get(jid):
            return False

        try:
            if os.path.exists(result_path):
                with open(result_path, "r", encoding="utf-8") as f:
                    content = f.read().strip()
                # Done when result is not empty and not "RUNNING"
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
#  COLLECT OUTPUT FILES
# =====================================================================

def _collect_output(remote_folder, files, compile_dir, today, group_name,
                    mname, cat, jid, qid, mid):
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
        D.add_log(jid, qid, mid, "WARN", "COLLECT",
                  f"Cannot list folder: {e}")
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
                D.add_log(jid, qid, mid, "INFO", "OUTPUT",
                          f"Collected: {nf} ({sz:.1f}KB)")
            except Exception as e:
                D.add_log(jid, qid, mid, "WARN", "OUTPUT",
                          f"Failed: {nf}: {e}")

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
        tracker_path = os.path.join(
            compile_root,
            f"TRACKER_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")

        collected = []
        for item in D.get_queue(jid):
            d = dict(item)
            if d.get("output_files"):
                try:
                    for fname in json.loads(d["output_files"]):
                        collected.append({
                            "job_id": jid, "date": today,
                            "group": group_name,
                            "machine": d.get("machine_name") or "?",
                            "category": d["cat_value"],
                            "filename": fname,
                            "collected_at": datetime.now().isoformat()
                        })
                except:
                    pass

        with open(tracker_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=[
                "job_id", "date", "group", "machine",
                "category", "filename", "collected_at"])
            w.writeheader()
            w.writerows(collected)

        D.add_log(jid, level="INFO", step="TRACKER",
                  message=f"{os.path.basename(tracker_path)} "
                          f"({len(collected)} files)")
    except Exception as e:
        D.add_log(jid, level="WARN", step="TRACKER",
                  message=f"Tracker failed: {e}")
