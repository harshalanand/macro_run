"""
Executor v10 - Remote execution via schtasks ONLY.

Files copy via UNC (server -> remote share).
Macro runs via schtasks on the remote PC using its local drive path.
No drive mapping. No local execution. No fallback.

Each machine MUST have: system_name, remote_path, username, password.
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
    remote_path = (m["remote_path"] or "").strip()

    # Block local machine
    if hostname and _is_local(hostname):
        results.append(("BLOCKED", False, f"{hostname} is THIS server. Macros only run on REMOTE PCs."))
        return results

    # Check required fields
    missing = []
    if not hostname: missing.append("System Name")
    if not remote_path: missing.append("Remote Path")
    if not username: missing.append("Username")
    if not password: missing.append("Password")
    if not shared: missing.append("Shared Folder")
    if missing:
        results.append(("CONFIG", False, f"Missing: {', '.join(missing)}"))
        return results

    # Test 1: Authenticate to UNC share
    if shared.startswith("\\\\") and username and password:
        ok = _net_use_auth(shared, username, password)
        if ok:
            results.append(("AUTH", True, f"Authenticated to share"))
        else:
            results.append(("AUTH", False, f"Authentication failed for {shared}. Check username/password."))
            return results

    # Test 2: Can we access the share?
    try:
        os.makedirs(shared, exist_ok=True)
        results.append(("ACCESS", True, f"Can access {shared}"))
    except Exception as e:
        results.append(("ACCESS", False, f"Cannot access {shared}: {e}"))
        return results

    # Test 2: Write/read file
    test_file = os.path.join(shared, "_test.txt")
    try:
        with open(test_file, "w") as f:
            f.write("ok")
        with open(test_file, "r") as f:
            assert f.read() == "ok"
        os.remove(test_file)
        results.append(("WRITE", True, "Can write/read files on share"))
    except Exception as e:
        results.append(("WRITE", False, f"Cannot write: {e}"))

    # Test 3: schtasks (the ONLY execution method)
    try:
        cmd = ["schtasks", "/query", "/s", hostname, "/u", username,
               "/p", password, "/fo", "list"]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        if r.returncode == 0:
            results.append(("SCHTASKS", True,
                f"schtasks works on {hostname}"))
            results.append(("READY", True,
                f"Macro will run on {hostname} at {remote_path}"))
        else:
            err = (r.stderr.strip() or r.stdout.strip())[:120]
            results.append(("SCHTASKS", False,
                f"schtasks failed: {err}. Run setup_remote.bat on {hostname} as admin."))
    except Exception as e:
        results.append(("SCHTASKS", False, f"schtasks error: {e}"))

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
        job_folder = f"job_{jid}"
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

        # == VALIDATE compile_dir is writable before starting ==
        try:
            os.makedirs(compile_dir, exist_ok=True)
            test_f = os.path.join(compile_dir, f"._write_test_{jid}")
            with open(test_f, "w") as tf:
                tf.write("ok")
            os.remove(test_f)
            D.add_log(jid, level="INFO", step="COMPILE_PATH",
                      message=f"Output folder OK: {compile_dir}")
        except Exception as e:
            D.add_log(jid, level="ERROR", step="COMPILE_PATH",
                      message=f"Cannot write to compile_path '{compile_dir}': {e}. "
                              f"Fix path in Settings or check folder permissions — all output will be LOST.")
            # Don't abort — still run macros, just warn loudly

        # == PHASE 1: Prep machines (auth + copy files) ==
        machine_ready = {}  # mid -> unc_folder
        with ThreadPoolExecutor(max_workers=min(len(machines), 8)) as pool:
            futs = {pool.submit(_prep_machine, m, job_folder, files, jid): m for m in machines}
            for fut in as_completed(futs):
                m = futs[fut]
                try:
                    unc_folder = fut.result()
                    machine_ready[m["machine_id"]] = unc_folder
                except Exception as e:
                    D.add_log(jid, machine_id=m["machine_id"], level="ERROR",
                              step="PREP_FAIL", message=f"{m['machine_name']}: {e}")

        if not machine_ready:
            D.add_log(jid, level="ERROR", step="ABORT", message="No machines ready")
            D.finish_job(jid)
            return

        # == PHASE 2: Each machine processes categories via schtasks ==
        def machine_worker(mid, unc_folder):
            m = D.get_machine(mid)
            mname = m["machine_name"]
            hostname = (m["system_name"] or "").strip()
            username = (m["username"] or "").strip()
            password = (m["password"] or "").strip()
            remote_path = (m["remote_path"] or "").strip()

            while not _kill_jobs.get(jid):
                item = D.claim_next(jid, mid)
                if not item:
                    break

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

                    vbs_unc = os.path.join(unc_folder, vbs_name)
                    result_unc = os.path.join(unc_folder, result_name)

                    try:
                        os.remove(result_unc)
                    except:
                        pass

                    # Write VBS to remote share via UNC
                    excel_visible = D.get_setting("excel_visible", "1") == "1"
                    vbs_code = _make_vbs(excel_file, macro_name, target_cell,
                                         safe_cat, result_name, visible=excel_visible)
                    with open(vbs_unc, "w", encoding="utf-8") as f:
                        f.write(vbs_code)

                    # Build LOCAL path for schtasks (runs on remote PC)
                    local_vbs = os.path.join(remote_path, job_folder, vbs_name)
                    D.add_log(jid, qid, mid, "INFO", "VBS_READY",
                              f"Remote: {local_vbs}")

                    # Snapshot folder BEFORE macro runs — so we only copy NEW output
                    pre_snapshot = _snapshot_folder(unc_folder)

                    # Execute via schtasks on target machine
                    ok = _run_via_schtasks(local_vbs, hostname, username, password,
                                           task_name, jid, qid, mid, mname,
                                           interactive=excel_visible)
                    if not ok:
                        raise RuntimeError(f"schtasks failed on {hostname}")

                    # Poll result via UNC
                    timeout_secs = int(D.get_setting("macro_timeout", "1800"))
                    got = _poll_result(result_unc, timeout_secs,
                                       jid, qid, mid, mname)
                    if not got:
                        raise RuntimeError(f"Timeout ({timeout_secs}s) waiting for macro on {hostname}")

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

                    # Collect output — only pending/new files since snapshot
                    new_files = _collect_output(unc_folder, files, compile_dir,
                                                job_folder, group_name, mname, cat,
                                                jid, qid, mid, pre_snapshot)

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
                    for fp in [vbs_unc, result_unc]:
                        if fp:
                            try:
                                os.remove(fp)
                            except:
                                pass
                    try:
                        del_cmd = ["schtasks", "/delete", "/tn", task_name, "/f"]
                        if not _is_local(hostname):
                            del_cmd[2:2] = ["/s", hostname]
                            if username:
                                del_cmd += ["/u", username]
                            if password:
                                del_cmd += ["/p", password]
                        subprocess.run(del_cmd, capture_output=True, timeout=10)
                    except:
                        pass

        # Launch workers
        with ThreadPoolExecutor(max_workers=max(len(machine_ready), 1)) as pool:
            futs = [pool.submit(machine_worker, mid, unc)
                    for mid, unc in machine_ready.items()]
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

def _prep_machine(machine, job_folder, files, jid):
    shared = machine["shared_folder"].strip()
    hostname = (machine["system_name"] or "").strip()
    username = (machine["username"] or "").strip()
    password = (machine["password"] or "").strip()
    remote_path = (machine["remote_path"] or "").strip()
    mid = machine["machine_id"]
    mname = machine["machine_name"]
    unc_folder = os.path.join(shared, job_folder)

    # BLOCK: Server cannot run macros on itself
    if _is_local(hostname):
        D.add_log(jid, machine_id=mid, level="ERROR", step="BLOCKED",
                  message=f"{mname}: {hostname} is THIS server. Macros only run on REMOTE PCs. Remove this machine or change System Name.")
        raise ValueError(f"{mname}: cannot run macros on server itself ({hostname})")

    # Validate all fields
    missing = []
    if not hostname: missing.append("System Name")
    if not remote_path: missing.append("Remote Path")
    if not username: missing.append("Username")
    if not password: missing.append("Password")
    if missing:
        D.add_log(jid, machine_id=mid, level="ERROR", step="CONFIG",
                  message=f"{mname}: missing {', '.join(missing)}")
        raise ValueError(f"{mname}: fill all fields: {', '.join(missing)}")

    D.add_log(jid, machine_id=mid, level="INFO", step="METHOD",
              message=f"{mname}: schtasks -> {hostname} (remote_path={remote_path})")

    # Authenticate to share first (required for non-domain PCs)
    if shared.startswith("\\\\"):
        _net_use_auth(shared, username, password, jid, mid)

    # Create date folder
    os.makedirs(unc_folder, exist_ok=True)
    copy_target = unc_folder

    # STEP 3: Copy files
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

    D.add_log(jid, machine_id=mid, level="INFO", step="PREP_OK",
              message=f"{mname}: ready (schtasks -> {hostname})")

    return unc_folder


# =====================================================================
#  DRIVE MAPPING (Python-side, not VBS)
# =====================================================================

def _net_use_auth(unc_path, username, password, jid=None, mid=None):
    """Authenticate to share for file copy via UNC. Tries access first."""
    clean = unc_path.replace("/", "\\").rstrip("\\")
    parts = [p for p in clean.split("\\") if p]
    if len(parts) < 2:
        return True
    server = parts[0]
    share = f"\\\\{server}\\{parts[1]}"

    # Step 1: Check if already accessible (don't break working connections)
    try:
        os.listdir(share)
        if jid:
            D.add_log(jid, machine_id=mid, level="INFO", step="AUTH",
                      message=f"Share already accessible: {share}")
        return True
    except:
        pass

    # Step 2: Need to authenticate. Try multiple username formats.
    # Extract plain username
    plain_user = username
    if "\\" in plain_user:
        plain_user = plain_user.split("\\", 1)[1]

    # Try in order: server\user, plain user, original input
    attempts = [
        f"{server}\\{plain_user}",
        plain_user,
        username,
    ]
    # Deduplicate while keeping order
    seen = set()
    unique = []
    for a in attempts:
        if a not in seen:
            seen.add(a)
            unique.append(a)

    for auth_user in unique:
        try:
            r = subprocess.run(
                ["net", "use", share, f"/user:{auth_user}", password, "/persistent:no"],
                capture_output=True, text=True, timeout=15)
            if r.returncode == 0:
                if jid:
                    D.add_log(jid, machine_id=mid, level="INFO", step="AUTH",
                              message=f"Authenticated to {share} as {auth_user}")
                return True
        except:
            pass

    # All attempts failed
    if jid:
        D.add_log(jid, machine_id=mid, level="WARN", step="AUTH",
                  message=f"All auth attempts failed for {share}. Tried: {unique}")
    return False


# =====================================================================
#  VBS - Runs on remote PC. Opens Excel from local drive path.
# =====================================================================

def _make_vbs(excel_file, macro_name, target_cell, cat_value, result_filename, visible=True):
    """
    VBS runs ON the remote PC via schtasks.
    It uses WScript.ScriptFullName to find its folder (local drive path).
    Excel opens from local path = always works.
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

xlApp.Visible = {("True" if visible else "False")}
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

def _is_local(hostname):
    """Check if hostname is the local machine (by name or IP)."""
    import socket
    h = hostname.upper().strip()
    local_names = {
        socket.gethostname().upper(),
        "LOCALHOST",
        "127.0.0.1",
        ".",
    }
    if h in local_names:
        return True
    # Also check if it's one of our own IPs
    try:
        local_ips = set()
        for info in socket.getaddrinfo(socket.gethostname(), None):
            local_ips.add(info[4][0])
        if h in local_ips:
            return True
    except:
        pass
    return False


def _get_active_session_user(hostname, admin_username, admin_password, jid=None, qid=None, mid=None):
    """
    Query the currently logged-in interactive user on a remote machine.
    Uses 'query user /server:hostname' (requires prior net use auth).
    Returns the plain username string, or None if not determinable.
    """
    try:
        cmd = ["query", "user", f"/server:{hostname}"]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        if r.returncode == 0:
            for line in r.stdout.splitlines()[1:]:   # skip header row
                line = line.strip()
                if not line:
                    continue
                # Mark active session (line starts with ">")
                # Format: >username  sessionname  id  Active  idletime  logontime
                parts = line.lstrip(">").split()
                if len(parts) >= 4 and parts[3].lower() == "active":
                    logged_user = parts[0].strip()
                    if jid:
                        D.add_log(jid, qid, mid, "INFO", "SESSION",
                                  f"Detected logged-in user on {hostname}: {logged_user}")
                    return logged_user
    except Exception as e:
        if jid:
            D.add_log(jid, qid, mid, "INFO", "SESSION",
                      f"Could not query session on {hostname}: {e}")
    return None



def _run_via_schtasks(vbs_local_path, hostname, username, password,
                      task_name, jid, qid, mid, mname, interactive=True):
    """Create + run scheduled task on the remote machine.

    User resolution strategy:
      - ALWAYS try to detect the ACTIVE logged-in user via 'query user'.
      - Run the task AS that active user (/ru <user>, NO /rp — not needed
        when the user has an active interactive session on the machine).
      - The configured admin credentials (/u /p) are used ONLY to
        authenticate the schtasks command to the remote machine — they
        are NOT used as the run-as user unless no session is detected.

    Fallback chain if no active session is detected:
      1. Configured user + /it        (admin is the logged-in user)
      2. Configured user, no /it      (headless, admin not logged in)
      ERROR — do NOT fall back to SYSTEM (macro needs Excel/UI).

    Fallback chain if active session IS detected:
      1. Detected user + /it          (normal — user has desktop session)
      2. Detected user, no /it        (session exists but /it rejected)
    """
    task_cmd = f'cscript //NoLogo "{vbs_local_path}"'
    local = _is_local(hostname)

    # Strip domain prefix from configured admin username (used to CONNECT, not run-as)
    configured_ru = username
    if "\\" in configured_ru:
        configured_ru = configured_ru.split("\\", 1)[1]

    mode = "LOCAL" if local else "REMOTE"

    # ── Step 1: Detect the active session user on the remote machine ──
    ru_user = None
    use_password = False      # active session user needs no /rp

    if not local:
        detected = _get_active_session_user(hostname, username, password, jid, qid, mid)
        if detected:
            ru_user = detected.split("\\")[-1].strip()   # strip domain prefix
            use_password = False   # /it with active session = no password needed
            D.add_log(jid, qid, mid, "INFO", "SCHTASKS",
                      f"{hostname}: active session user = '{ru_user}' "
                      f"{'(same as configured)' if ru_user.lower() == configured_ru.lower() else f'(configured={configured_ru})'} "
                      f"— task will run AS '{ru_user}'")
        else:
            # No active session detected — fall back to configured admin
            ru_user = configured_ru
            use_password = True
            D.add_log(jid, qid, mid, "WARN", "SCHTASKS",
                      f"{hostname}: no active session detected — falling back to configured user '{ru_user}'. "
                      f"Macro may not interact with Excel unless someone is logged in.")
    else:
        # Local: run as configured user
        ru_user = configured_ru
        use_password = True

    def _build_create(with_interactive, with_password):
        cmd = ["schtasks", "/create"]
        if not local:
            # /u /p = admin credentials to AUTHENTICATE the command to remote machine
            cmd += ["/s", hostname, "/u", username, "/p", password]
        cmd += ["/tn", task_name, "/tr", task_cmd,
                "/sc", "once", "/st", "00:00", "/f", "/rl", "highest"]
        # /ru = which user's session the task RUNS IN on the target machine
        cmd += ["/ru", ru_user]
        if with_password:
            cmd += ["/rp", password]
        if with_interactive:
            cmd.append("/it")
        return cmd

    D.add_log(jid, qid, mid, "INFO", "SCHTASKS",
              f"Scheduling task on {hostname} ({mode}) ru='{ru_user}'")

    # ── Step 2: Try creating the task (with /it first, then without) ──
    attempts = [
        (True,  use_password, "interactive /it"),
        (False, use_password, "headless (no /it)"),
    ]

    task_created = False
    for with_it, with_pw, label in attempts:
        create = _build_create(with_interactive=with_it, with_password=with_pw)
        r = subprocess.run(create, capture_output=True, text=True, timeout=30)
        if r.returncode == 0:
            D.add_log(jid, qid, mid, "INFO", "SCHTASKS",
                      f"Task created [{label}] on {hostname}")
            task_created = True
            break
        err = (r.stderr.strip() or r.stdout.strip())[:200]
        D.add_log(jid, qid, mid, "WARN", "SCHTASKS",
                  f"Create [{label}] failed: {err}")

    if not task_created:
        D.add_log(jid, qid, mid, "ERROR", "SCHTASKS",
                  f"{hostname}: could not create task as '{ru_user}'. "
                  f"Ensure the machine has an active user session and admin credentials are correct.")
        return False

    # ── Step 3: Trigger the task ──
    try:
        run_cmd = ["schtasks", "/run"]
        if not local:
            run_cmd += ["/s", hostname, "/u", username, "/p", password]
        run_cmd += ["/tn", task_name]

        r2 = subprocess.run(run_cmd, capture_output=True, text=True, timeout=15)
        if r2.returncode != 0:
            err = (r2.stderr.strip() or r2.stdout.strip())[:200]
            D.add_log(jid, qid, mid, "WARN", "SCHTASKS", f"Trigger failed: {err}")
            return False

        D.add_log(jid, qid, mid, "INFO", "SCHTASKS",
                  f"Task triggered on {hostname} — running as '{ru_user}'")
        return True
    except Exception as e:
        D.add_log(jid, qid, mid, "WARN", "SCHTASKS", f"Error triggering task: {e}")
        return False




# =====================================================================
#  POLL + READ RESULT
# =====================================================================

def _poll_result(result_path, timeout_secs, jid, qid, mid, mname):
    """Poll result file via UNC until SUCCESS/ERROR or timeout."""
    start = time.time()
    last_log = 0

    while (time.time() - start) < timeout_secs:
        if _kill_jobs.get(jid):
            return False

        try:
            if os.path.exists(result_path):
                with open(result_path, "r", encoding="utf-8") as f:
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


# =====================================================================
#  COLLECT OUTPUT
# =====================================================================

def _snapshot_folder(folder):
    """Return dict of {filename: mtime} for all files currently in folder."""
    snap = {}
    try:
        for fn in os.listdir(folder):
            fp = os.path.join(folder, fn)
            if os.path.isfile(fp):
                snap[fn] = os.path.getmtime(fp)
    except:
        pass
    return snap


def _collect_output(unc_folder, files, compile_dir, job_folder, group_name,
                    mname, cat, jid, qid, mid, pre_snapshot=None):
    """
    Collect only NEW output files produced by the macro:
      - Must NOT be in pre_snapshot (brand-new file after macro ran)
      - OR existed before but was MODIFIED (mtime increased > 1s)
      - ALWAYS skip original/uploaded files — they are input templates,
        never output, even if the macro saved back into them.
      - ALWAYS skip temp VBS/result/lock files.
      - Output filenames are kept exactly as produced — NO category prefix.
    """
    if pre_snapshot is None:
        pre_snapshot = {}

    # Build set of original filenames (case-insensitive for Windows shares)
    original_lower = {f["original_name"].lower() for f in files}
    skip_prefixes = ("_run_", "_result_", "_macro_", "~$")
    found = []

    try:
        for fn in os.listdir(unc_folder):
            fp = os.path.join(unc_folder, fn)
            if not os.path.isfile(fp):
                continue
            # Always skip temp files
            if any(fn.startswith(s) for s in skip_prefixes):
                continue
            # Always skip original/uploaded files — never treat them as output
            if fn.lower() in original_lower:
                continue

            cur_mtime = os.path.getmtime(fp)
            prev_mtime = pre_snapshot.get(fn)

            if prev_mtime is None:
                # Brand-new file produced by macro → collect
                found.append(fn)
            elif cur_mtime > prev_mtime + 1:
                # Existed before but modified by macro → collect
                found.append(fn)
            # else: unchanged → skip

    except Exception as e:
        D.add_log(jid, qid, mid, "WARN", "COLLECT", f"Cannot list folder: {e}")
        return []

    if not found:
        D.add_log(jid, qid, mid, "INFO", "COLLECT",
                  f"{mname}: no new output files for '{cat}'")
        return []

    out = os.path.join(compile_dir, job_folder, group_name)
    os.makedirs(out, exist_ok=True)

    copied = []
    for fn in found:
        try:
            src = os.path.join(unc_folder, fn)
            dst = os.path.join(out, fn)          # keep original filename, no prefix
            # If same filename from different cat already exists, append cat suffix
            if os.path.exists(dst):
                name, ext = os.path.splitext(fn)
                safe_cat = cat.replace("/", "_").replace("\\", "_").replace(" ", "_")
                dst = os.path.join(out, f"{name}_{safe_cat}{ext}")
            shutil.copy2(src, dst)
            out_name = os.path.basename(dst)
            sz = os.path.getsize(dst) / 1024
            D.add_log(jid, qid, mid, "INFO", "OUTPUT",
                      f"Copied: {out_name} ({sz:.1f}KB) -> {out}")
            copied.append(out_name)
        except Exception as e:
            D.add_log(jid, qid, mid, "WARN", "OUTPUT", f"Failed to copy {fn}: {e}")

    return copied


# =====================================================================
#  TRACKER CSV
# =====================================================================

def _track(jid):
    job = D.get_job(jid)
    if not job:
        return
    job_folder = f"job_{jid}"
    gn = job["group_name"]
    base = D.get_setting("compile_path", "") or COMPILED_DIR
    root = os.path.join(base, job_folder, gn)
    try:
        os.makedirs(root, exist_ok=True)
        tp = os.path.join(root, f"TRACKER_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        rows = []
        for item in D.get_queue(jid):
            d = dict(item)
            if d.get("output_files"):
                try:
                    for fn in json.loads(d["output_files"]):
                        rows.append({"job_id": jid, "job_folder": job_folder, "group": gn,
                                     "machine": d.get("machine_name") or "?",
                                     "category": d["cat_value"], "filename": fn})
                except:
                    pass
        with open(tp, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["job_id", "job_folder", "group", "machine", "category", "filename"])
            w.writeheader()
            w.writerows(rows)
        D.add_log(jid, level="INFO", step="TRACKER",
                  message=f"{os.path.basename(tp)} ({len(rows)} files) -> {root}")
    except Exception as e:
        D.add_log(jid, level="WARN", step="TRACKER", message=f"Failed: {e}")
