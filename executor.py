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
_job_executors = {}   # jid -> {executor, machine_worker, active, lock, done}
_job_context = {}     # jid -> {excel_file, macro_name, ...}


def is_running(jid):
    """Return True if job jid is currently executing in memory."""
    return bool(_running_jobs.get(jid))


def run_async(jid):
    _running_jobs[jid] = True
    _kill_jobs.pop(jid, None)
    threading.Thread(target=_run_job, args=(jid,), daemon=True).start()


def kill_job(jid):
    _kill_jobs[jid] = True
    D.add_log(jid, level="WARN", step="KILL", message=f"Kill signal for job #{jid}")
    with D.db() as c:
        c.execute("UPDATE job_queue SET status='CANCELLED' WHERE job_id=? AND status='QUEUED'", (jid,))


def test_machine(mid):
    """Test connectivity to a group machine. Returns list of (step, ok, message)."""
    m = D.get_machine(mid)
    if not m:
        return [("LOAD", False, "Machine not found")]
    results = test_machine_dict(dict(m))
    ok_count = sum(1 for r in results if r[1])
    fail_count = sum(1 for r in results if not r[1])
    if ok_count and not fail_count:
        status = "OK"
    elif ok_count and fail_count:
        status = "PARTIAL"
    else:
        status = "FAIL"
    detail = " | ".join(f"{'OK' if r[1] else 'FAIL'} {r[0]}: {r[2]}" for r in results)
    D.sync_health_to_master(m["machine_name"], status, detail)
    return results



# Test addon - will be appended to executor.py

_test_job_results = {}


def test_job_async(gid):
    """Run a full pipeline validation for every machine in the group.
    Tests: config, auth, folder create, VBS write, user detection, schtasks create+trigger, result polling.
    Does NOT open Excel or run any macro."""
    _test_job_results[gid] = []
    threading.Thread(target=_run_test_job, args=(gid,), daemon=True).start()


def get_test_results(gid):
    return _test_job_results.get(gid, None)


def clear_test_results(gid):
    _test_job_results.pop(gid, None)


def _make_test_vbs(result_filename):
    """Minimal VBS that just writes SUCCESS — proves schtasks can execute scripts."""
    return (
        'Dim fso, scriptFolder, resultPath\n'
        'Set fso = CreateObject("Scripting.FileSystemObject")\n'
        'scriptFolder = fso.GetParentFolderName(WScript.ScriptFullName)\n'
        'If Right(scriptFolder, 1) <> "\\\\" Then scriptFolder = scriptFolder & "\\\\"\n'
        'resultPath = scriptFolder & "' + result_filename + '"\n'
        'Call WriteR(resultPath, "SUCCESS:TEST_OK")\n'
        'WScript.Quit 0\n'
        '\n'
        'Sub WriteR(p, m)\n'
        '    Dim f\n'
        '    Set f = CreateObject("Scripting.FileSystemObject").CreateTextFile(p, True)\n'
        '    f.Write m\n'
        '    f.Close\n'
        'End Sub\n'
    )


def _cleanup_test(unc_folder, task_name=None, hostname=None, username=None, password=None):
    if task_name and hostname:
        try:
            subprocess.run(
                ["schtasks", "/delete", "/s", hostname, "/u", username,
                 "/p", password, "/tn", task_name, "/f"],
                capture_output=True, timeout=10)
        except Exception:
            pass
    try:
        if unc_folder and os.path.exists(unc_folder):
            shutil.rmtree(unc_folder, ignore_errors=True)
    except Exception:
        pass


def _run_test_job(gid):
    results = _test_job_results[gid]

    def log(mid, mname, step, ok, msg):
        results.append({
            "mid": mid, "machine": mname, "step": step,
            "ok": ok, "msg": msg,
            "ts": datetime.now().strftime("%H:%M:%S")
        })

    try:
        machines = [dict(m) for m in D.get_machines(gid) if m["is_active"]]
        g = D.get_group(gid)
        if not machines:
            log(0, "—", "INIT", False, "No active machines in group")
            results.append({"done": True})
            return

        job_folder = "test_{}_{}".format(gid, int(datetime.now().timestamp()))
        log(0, "ALL", "START", True,
            "Testing {} machine(s) in group '{}'".format(len(machines), g["group_name"]))

        def test_one(m):
            mid = m["machine_id"]
            mname = m["machine_name"]
            hostname = (m.get("system_name") or "").strip()
            username = (m.get("username") or "").strip()
            password = (m.get("password") or "").strip()
            remote_path = (m.get("remote_path") or "").strip()
            shared = (m.get("shared_folder") or "").strip()

            # Step 1: Config
            missing = [k for k, v in [
                ("System Name", hostname), ("Remote Path", remote_path),
                ("Username", username), ("Password", password), ("Shared Folder", shared)
            ] if not v]
            if missing:
                log(mid, mname, "CONFIG", False, "Missing fields: " + ", ".join(missing))
                return
            log(mid, mname, "CONFIG", True,
                "hostname={}, remote_path={}".format(hostname, remote_path))

            # Step 2: Auth
            unc_folder = os.path.join(shared, job_folder)
            auth_ok = _net_use_auth(shared, username, password)
            if not auth_ok:
                log(mid, mname, "AUTH", False,
                    "Cannot authenticate to {} — check username/password".format(shared))
                return
            log(mid, mname, "AUTH", True, "Authenticated to {}".format(shared))

            # Step 3: Create folder
            try:
                os.makedirs(unc_folder, exist_ok=True)
                log(mid, mname, "MKDIR", True, "Folder created: {}".format(unc_folder))
            except Exception as e:
                log(mid, mname, "MKDIR", False,
                    "Cannot create folder: {} — fix share write permissions".format(e))
                return

            # Step 4: Write test VBS
            vbs_name = "_test_run.vbs"
            result_name = "_test_result.txt"
            vbs_unc = os.path.join(unc_folder, vbs_name)
            result_unc = os.path.join(unc_folder, result_name)
            local_vbs = os.path.join(remote_path, job_folder, vbs_name)
            try:
                with open(vbs_unc, "w", encoding="utf-8") as f:
                    f.write(_make_test_vbs(result_name))
                log(mid, mname, "VBS_WRITE", True, "Test VBS written to share")
            except Exception as e:
                log(mid, mname, "VBS_WRITE", False, "Cannot write VBS: {}".format(e))
                _cleanup_test(unc_folder)
                return

            # Step 5: Detect active user
            detected = _get_active_session_user(hostname, username, password)
            if detected:
                ru_user = detected.split("\\")[-1].strip()
                log(mid, mname, "SESSION", True,
                    "Active user: '{}' — schtasks will run in their session".format(ru_user))
            else:
                ru_user = username.split("\\")[-1] if "\\" in username else username
                log(mid, mname, "SESSION", False,
                    "No active session detected — will try as '{}'. "
                    "Enable Remote Desktop Services + Remote Registry on {}.".format(ru_user, hostname))

            # Step 6: Create + trigger schtask
            task_name = "MQ_TEST_{}_{}".format(gid, mid)
            task_cmd = 'cscript //NoLogo "{}"'.format(local_vbs)
            plain_ru = ru_user

            # Key rule: only use /it if we actually detected an active session.
            # /it with no session = task created OK, triggered OK, but silently never runs.
            attempts = []
            if detected:
                attempts.append((True,  False, "interactive /it"))   # user session exists
            attempts.append((False, not bool(detected), "headless")) # always include as fallback

            created = False
            used_mode = ""
            last_err = ""
            for use_it, use_rp, mode_label in attempts:
                create_cmd = [
                    "schtasks", "/create", "/s", hostname,
                    "/u", username, "/p", password,
                    "/tn", task_name, "/tr", task_cmd,
                    "/sc", "once", "/st", "00:00", "/f", "/rl", "highest",
                    "/ru", plain_ru
                ]
                if use_rp:
                    create_cmd += ["/rp", password]
                if use_it:
                    create_cmd.append("/it")
                r = subprocess.run(create_cmd, capture_output=True, text=True, timeout=30)
                if r.returncode == 0:
                    log(mid, mname, "SCHTASKS", True,
                        "Task created [{}] on {} as '{}'".format(mode_label, hostname, plain_ru))
                    created = True
                    used_mode = mode_label
                    break
                last_err = (r.stderr.strip() or r.stdout.strip())[:200]

            if not created:
                log(mid, mname, "SCHTASKS", False,
                    "Cannot create task on {}: {}".format(hostname, last_err))
                _cleanup_test(unc_folder, task_name, hostname, username, password)
                return

            # Trigger
            run_cmd = ["schtasks", "/run", "/s", hostname,
                       "/u", username, "/p", password, "/tn", task_name]
            r2 = subprocess.run(run_cmd, capture_output=True, text=True, timeout=15)
            if r2.returncode != 0:
                err = (r2.stderr.strip() or r2.stdout.strip())[:200]
                log(mid, mname, "SCHTASKS", False, "Trigger failed: {}".format(err))
                _cleanup_test(unc_folder, task_name, hostname, username, password)
                return

            # Verify task is actually Running (not stuck in Ready)
            # schtasks /run returns 0 even when /it has no active session
            time.sleep(3)
            try:
                qcmd = ["schtasks", "/query", "/s", hostname, "/u", username, "/p", password,
                        "/tn", task_name, "/fo", "csv", "/nh"]
                qr = subprocess.run(qcmd, capture_output=True, text=True, timeout=15)
                task_actually_running = qr.returncode == 0 and "Running" in qr.stdout
            except Exception:
                task_actually_running = True  # can't query, assume OK

            if not task_actually_running and used_mode == "interactive /it":
                # Stuck in Ready — /it has no session. Delete and retry headless.
                log(mid, mname, "SCHTASKS", False,
                    "Task stuck in Ready on {} — '{}' has no interactive session. "
                    "Retrying headless (no /it)…".format(hostname, plain_ru))
                try:
                    subprocess.run(
                        ["schtasks", "/delete", "/s", hostname, "/u", username, "/p", password,
                         "/tn", task_name, "/f"],
                        capture_output=True, timeout=10)
                except Exception:
                    pass

                # Headless retry
                create_hl = [
                    "schtasks", "/create", "/s", hostname,
                    "/u", username, "/p", password,
                    "/tn", task_name, "/tr", task_cmd,
                    "/sc", "once", "/st", "00:00", "/f", "/rl", "highest",
                    "/ru", plain_ru, "/rp", password
                ]
                rhl = subprocess.run(create_hl, capture_output=True, text=True, timeout=30)
                if rhl.returncode != 0:
                    err = (rhl.stderr.strip() or rhl.stdout.strip())[:200]
                    log(mid, mname, "SCHTASKS", False,
                        "Headless retry also failed: {}. "
                        "Check firewall: allow 'Remote Scheduled Tasks Management' on {}.".format(err, hostname))
                    _cleanup_test(unc_folder, task_name, hostname, username, password)
                    return

                r3 = subprocess.run(run_cmd, capture_output=True, text=True, timeout=15)
                if r3.returncode != 0:
                    err = (r3.stderr.strip() or r3.stdout.strip())[:200]
                    log(mid, mname, "SCHTASKS", False, "Headless trigger failed: {}".format(err))
                    _cleanup_test(unc_folder, task_name, hostname, username, password)
                    return
                log(mid, mname, "SCHTASKS", True,
                    "Task now running [headless] on {} as '{}'".format(hostname, plain_ru))
            else:
                log(mid, mname, "SCHTASKS", True,
                    "Task confirmed running on {} [{}]".format(hostname, used_mode))

            # Step 7: Poll result (max 60s)
            log(mid, mname, "WAITING", True, "Task triggered — polling result file (max 60s)…")
            deadline = time.time() + 60
            result_text = None
            while time.time() < deadline:
                time.sleep(2)
                try:
                    if os.path.exists(result_unc):
                        t = open(result_unc, "r", encoding="utf-8").read().strip()
                        if t:
                            result_text = t
                            break
                except Exception:
                    pass

            _cleanup_test(unc_folder, task_name, hostname, username, password)

            if result_text and result_text.startswith("SUCCESS"):
                log(mid, mname, "RESULT", True,
                    "ALL STEPS PASSED — full pipeline works on {}. "
                    "Real macro jobs will execute correctly on this machine.".format(hostname))
            elif result_text:
                log(mid, mname, "RESULT", False,
                    "Script ran but returned: {}".format(result_text))
            else:
                log(mid, mname, "RESULT", False,
                    "TIMEOUT — VBS was written and task triggered but no result in 60s. "
                    "Check: is remote_path '{}' correct on {}? "
                    "Is schtasks actually running the VBS?".format(remote_path, hostname))

        with ThreadPoolExecutor(max_workers=min(len(machines), 8)) as pool:
            futs = [pool.submit(test_one, m) for m in machines]
            for fut in as_completed(futs):
                try:
                    fut.result()
                except Exception as e:
                    log(0, "?", "ERROR", False, str(e))

    except Exception as e:
        results.append({
            "mid": 0, "machine": "—", "step": "CRASH", "ok": False,
            "msg": "Test crashed: {}".format(e), "ts": datetime.now().strftime("%H:%M:%S")
        })
    finally:
        results.append({"done": True})


def add_machine_to_job(jid, mid):
    """Hot-add a machine to a running job. Preps it and starts a new worker thread."""
    ctx = _job_context.get(jid)
    ex_info = _job_executors.get(jid)
    if not ctx or not ex_info:
        return False, "Job not running or context unavailable"
    m = D.get_machine(mid)
    if not m:
        return False, "Machine not found"

    def _do_add():
        try:
            D.add_log(jid, machine_id=mid, level="INFO", step="ADD_MACHINE",
                      message=f"Hot-adding {m['machine_name']} to running job #{jid}")
            unc_folder = _prep_machine(dict(m), ctx["job_folder"], ctx["files"], jid)
            with ex_info["lock"]:
                ex_info["active"][0] += 1
                ex_info["done"].clear()
            mw = ex_info["machine_worker"]
            def _tracked():
                try:
                    mw(mid, unc_folder)
                finally:
                    with ex_info["lock"]:
                        ex_info["active"][0] -= 1
                        if ex_info["active"][0] <= 0:
                            ex_info["done"].set()
            ex_info["executor"].submit(_tracked)
            D.add_log(jid, machine_id=mid, level="INFO", step="ADD_MACHINE",
                      message=f"{m['machine_name']} worker started — picking up QUEUED categories")
        except Exception as e:
            D.add_log(jid, machine_id=mid, level="ERROR", step="ADD_MACHINE",
                      message=f"Failed to add {m['machine_name']}: {e}")

    import threading as _t
    _t.Thread(target=_do_add, daemon=True).start()
    return True, f"Adding {m['machine_name']} to job — prep in progress"


def test_machine_dict(m):
    """Test a machine dict (works for both group machines and master machines).

    Step classification:
      CONFIG  — missing required fields (always fatal, stops here)
      BLOCKED — machine is the server itself (always fatal)
      AUTH    — net use authentication (fatal if fails)
      ACCESS  — can list the share directory
      WRITE   — can write/read/delete a test file (non-fatal warning)
      SCHTASKS— remote schtasks query (key test — required for execution)
      READY   — all critical tests passed
    """
    results = []
    shared = (m.get("shared_folder") or "").strip()
    hostname = (m.get("system_name") or "").strip()
    username = (m.get("username") or "").strip()
    password = (m.get("password") or "").strip()
    remote_path = (m.get("remote_path") or "").strip()

    # Block local machine
    if hostname and _is_local(hostname):
        results.append(("BLOCKED", False, f"{hostname} is THIS server — macros only run on REMOTE PCs"))
        return results

    # Check required fields
    missing = []
    if not hostname:    missing.append("System Name")
    if not remote_path: missing.append("Remote Path")
    if not username:    missing.append("Username")
    if not password:    missing.append("Password")
    if not shared:      missing.append("Shared Folder")
    if missing:
        results.append(("CONFIG", False, f"Missing required fields: {', '.join(missing)}"))
        return results

    # AUTH: Authenticate to UNC share
    if shared.startswith("\\\\"):
        auth_ok = _net_use_auth(shared, username, password)
        if auth_ok:
            results.append(("AUTH", True, f"Authenticated to {shared}"))
        else:
            results.append(("AUTH", False, f"Authentication failed for {shared} — check username/password"))
            return results
    else:
        results.append(("AUTH", True, f"Local/non-UNC path, skipping auth"))

    # ACCESS: Can we list the share?
    try:
        os.makedirs(shared, exist_ok=True)
        entries = os.listdir(shared)
        results.append(("ACCESS", True, f"Share accessible ({len(entries)} items)"))
    except Exception as e:
        results.append(("ACCESS", False, f"Cannot access {shared}: {e}"))
        return results

    # WRITE: Non-fatal — warn but continue to schtasks test
    test_file = os.path.join(shared, "_healthcheck_.txt")
    try:
        with open(test_file, "w") as f: f.write("healthcheck")
        with open(test_file, "r") as f: assert f.read() == "healthcheck"
        os.remove(test_file)
        results.append(("WRITE", True, "Read/write test passed"))
    except Exception as e:
        results.append(("WRITE", False, f"Write test failed (non-fatal): {e}"))
        # Don't return — continue to schtasks which is the critical test

    # SCHTASKS: Remote execution capability — this is the key test
    try:
        cmd = ["schtasks", "/query", "/s", hostname, "/u", username, "/p", password, "/fo", "list"]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
        if r.returncode == 0:
            results.append(("SCHTASKS", True, f"Remote schtasks OK on {hostname}"))
            results.append(("READY", True, f"Machine ready — macros will run at {remote_path}"))
        else:
            err = (r.stderr.strip() or r.stdout.strip())[:200]
            results.append(("SCHTASKS", False,
                f"schtasks failed on {hostname}: {err} — run setup_remote.bat as admin on that machine"))
    except Exception as e:
        results.append(("SCHTASKS", False, f"schtasks error on {hostname}: {e}"))

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

        # Store job context so hot-added machines can access it
        _job_context[jid] = {
            "excel_file": excel_file, "macro_name": macro_name,
            "target_cell": target_cell, "job_folder": job_folder,
            "group_name": group_name, "compile_dir": compile_dir,
            "files": files, "gid": gid,
        }

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
        # Use a persistent executor stored globally so hot-added machines can join
        executor = ThreadPoolExecutor(max_workers=32)  # large enough for any additions
        _job_executors[jid] = executor
        def machine_worker(mid, unc_folder):
            m = dict(D.get_machine(mid))   # convert sqlite3.Row → dict so .get() works
            mname = m["machine_name"]
            hostname = (m.get("system_name") or "").strip()
            username = (m.get("username") or "").strip()
            password = (m.get("password") or "").strip()
            remote_path = (m.get("remote_path") or "").strip()

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

                    # Determine visible/hidden mode — per-machine overrides global setting
                    global_visible = D.get_setting("excel_visible", "1") == "1"
                    machine_run_mode = (m.get("run_mode") or "global").strip().lower()
                    if machine_run_mode == "hidden":
                        excel_visible = False
                    elif machine_run_mode == "visible":
                        excel_visible = True
                    else:
                        excel_visible = global_visible  # use global setting

                    D.add_log(jid, qid, mid, "INFO", "MODE",
                              f"{mname}: Excel={'VISIBLE' if excel_visible else 'HIDDEN'} "
                              f"(machine={machine_run_mode}, global={'visible' if global_visible else 'hidden'})")

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

                    # Execute on remote PC — try WMI first (no admin session needed),
                    # fall back to schtasks if WMI unavailable
                    wmi_ok, wmi_msg = _run_via_wmi(
                        local_vbs, hostname, username, password, jid, qid, mid, mname)

                    if wmi_ok:
                        D.add_log(jid, qid, mid, "INFO", "EXEC",
                                  f"{mname}: running via WMI (no admin session required)")
                    else:
                        D.add_log(jid, qid, mid, "INFO", "EXEC",
                                  f"{mname}: WMI unavailable ({wmi_msg[:80]}), falling back to schtasks")
                        ok = _run_via_schtasks(local_vbs, hostname, username, password,
                                               task_name, jid, qid, mid, mname,
                                               interactive=excel_visible)
                        if not ok:
                            raise RuntimeError(f"Both WMI and schtasks failed on {hostname}")

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

        # Track active workers with a counter protected by a lock
        import threading as _threading
        _active_workers = [len(machine_ready)]
        _workers_lock = _threading.Lock()
        _all_done = _threading.Event()
        _job_executors[jid] = {
            "executor": executor,
            "machine_worker": machine_worker,
            "active": _active_workers,
            "lock": _workers_lock,
            "done": _all_done,
        }

        def tracked_worker(mid, unc):
            try:
                machine_worker(mid, unc)
            finally:
                with _workers_lock:
                    _active_workers[0] -= 1
                    if _active_workers[0] <= 0:
                        _all_done.set()

        # Launch initial workers
        for mid, unc in machine_ready.items():
            executor.submit(tracked_worker, mid, unc)

        # Wait for all workers (including any hot-added ones)
        _all_done.wait()

        # Finalize
        executor.shutdown(wait=False)
        _job_executors.pop(jid, None)
        _job_context.pop(jid, None)
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
        _job_executors.pop(jid, None)
        _job_context.pop(jid, None)


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
        auth_ok = _net_use_auth(shared, username, password, jid, mid)
        if not auth_ok:
            raise ValueError(f"{mname}: authentication failed for {shared} — check username/password")

    # Create job folder on the UNC share
    # On some Windows shares, makedirs fails with [WinError 5] if:
    #   a) The auth session expired between auth and mkdir
    #   b) The share root is writable but subfolder creation needs a refresh
    # Fix: retry once with explicit net use re-auth if first attempt fails
    try:
        os.makedirs(unc_folder, exist_ok=True)
    except OSError as e:
        if "5" in str(e) or "denied" in str(e).lower() or "access" in str(e).lower():
            D.add_log(jid, machine_id=mid, level="WARN", step="MKDIR",
                      message=f"{mname}: folder create failed ({e}), retrying after re-auth...")
            # Force a fresh net use connection and retry
            _net_use_auth(shared, username, password, jid, mid, force=True)
            try:
                os.makedirs(unc_folder, exist_ok=True)
            except OSError as e2:
                raise ValueError(
                    f"{mname}: Cannot create folder '{unc_folder}': {e2}. "
                    f"Check that the shared folder '{shared}' allows write access for user '{username}'. "
                    f"On the remote PC, right-click the shared folder → Properties → Sharing → ensure '{username}' has Read/Write permission."
                ) from e2
        else:
            raise
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

def _net_use_auth(unc_path, username, password, jid=None, mid=None, force=False):
    """Authenticate to share for file copy via UNC.
    Verifies WRITE access (not just read) since makedirs needs write permission.
    force=True disconnects existing session and reconnects fresh.
    """
    clean = unc_path.replace("/", "\\").rstrip("\\")
    parts = [p for p in clean.split("\\") if p]
    if len(parts) < 2:
        return True
    server = parts[0]
    share = f"\\\\{server}\\{parts[1]}"

    # Extract plain username (strip domain prefix)
    plain_user = username
    if "\\" in plain_user:
        plain_user = plain_user.split("\\", 1)[1]

    # On force: disconnect existing session first so we get a fresh writable one
    if force:
        try:
            subprocess.run(["net", "use", share, "/delete", "/yes"],
                           capture_output=True, timeout=10)
        except:
            pass

    # If not forcing, check if already accessible AND writable
    if not force:
        try:
            os.listdir(share)
            # Quick write test
            test_f = os.path.join(share, "_writecheck_.tmp")
            try:
                with open(test_f, "w") as f:
                    f.write("ok")
                os.remove(test_f)
                if jid:
                    D.add_log(jid, machine_id=mid, level="INFO", step="AUTH",
                              message=f"Share accessible+writable: {share}")
                return True
            except OSError:
                # Readable but not writable — fall through to re-auth
                pass
        except:
            pass

    # Build auth attempts: server\user, plain user, original
    attempts = list(dict.fromkeys([
        f"{server}\\{plain_user}",
        plain_user,
        username,
    ]))

    for auth_user in attempts:
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

    if jid:
        D.add_log(jid, machine_id=mid, level="WARN", step="AUTH",
                  message=f"All auth attempts failed for {share}. Tried: {attempts}")
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
    Detect the currently logged-in interactive user on a remote machine.
    Tries multiple methods in order until one succeeds.

    Method 1: query user /server:hostname  (fastest, needs Remote Desktop Services)
    Method 2: wmic /node:hostname computersystem get username  (works on most Windows)
    Method 3: tasklist explorer.exe (finds user via their explorer.exe process)
    """
    plain_user = admin_username.split("\\")[-1] if "\\" in admin_username else admin_username

    # Method 1: query user
    try:
        r = subprocess.run(["query", "user", f"/server:{hostname}"],
                           capture_output=True, text=True, timeout=15)
        if r.returncode == 0:
            for line in r.stdout.splitlines()[1:]:
                line = line.strip()
                if not line:
                    continue
                parts = line.lstrip(">").split()
                if len(parts) >= 4 and parts[3].lower() == "active":
                    logged_user = parts[0].strip()
                    if jid:
                        D.add_log(jid, qid, mid, "INFO", "SESSION",
                                  f"{hostname}: active user = '{logged_user}' (via query user)")
                    return logged_user
    except Exception as e:
        if jid:
            D.add_log(jid, qid, mid, "INFO", "SESSION",
                      f"{hostname}: query user failed ({e}), trying WMI...")

    # Method 2: wmic computersystem get username
    try:
        r = subprocess.run(
            ["wmic", f"/node:{hostname}", f"/user:{admin_username}",
             f"/password:{admin_password}", "computersystem", "get", "username", "/format:value"],
            capture_output=True, text=True, timeout=15)
        if r.returncode == 0:
            for line in r.stdout.splitlines():
                line = line.strip()
                if line.lower().startswith("username=") and "=" in line:
                    val = line.split("=", 1)[1].strip()
                    if val:
                        # val may be DOMAIN\user or just user
                        logged_user = val.split("\\")[-1].strip()
                        if logged_user and logged_user.lower() not in ("", "username"):
                            if jid:
                                D.add_log(jid, qid, mid, "INFO", "SESSION",
                                          f"{hostname}: active user = '{logged_user}' (via WMI)")
                            return logged_user
    except Exception as e:
        if jid:
            D.add_log(jid, qid, mid, "INFO", "SESSION",
                      f"{hostname}: WMI failed ({e}), trying tasklist...")

    # Method 3: find explorer.exe owner via tasklist
    try:
        r = subprocess.run(
            ["tasklist", "/s", hostname, "/u", admin_username, "/p", admin_password,
             "/fi", "imagename eq explorer.exe", "/fo", "csv", "/nh"],
            capture_output=True, text=True, timeout=15)
        if r.returncode == 0 and "explorer.exe" in r.stdout.lower():
            # CSV format: "Image","PID","Session","Num","Mem"  — no username column here
            # But if explorer.exe is running, the logged user is the configured user most likely
            # Try with /v for verbose which includes username
            r2 = subprocess.run(
                ["tasklist", "/s", hostname, "/u", admin_username, "/p", admin_password,
                 "/fi", "imagename eq explorer.exe", "/fo", "csv", "/v", "/nh"],
                capture_output=True, text=True, timeout=15)
            if r2.returncode == 0:
                for line in r2.stdout.splitlines():
                    if "explorer.exe" in line.lower():
                        parts = [p.strip('"') for p in line.split('","')]
                        # Verbose CSV: Image,PID,Session,Num,Mem,Status,Username,CPU,Window
                        if len(parts) >= 7 and parts[6] and "\\" in parts[6]:
                            logged_user = parts[6].split("\\")[-1].strip()
                            if logged_user:
                                if jid:
                                    D.add_log(jid, qid, mid, "INFO", "SESSION",
                                              f"{hostname}: active user = '{logged_user}' (via tasklist)")
                                return logged_user
    except Exception as e:
        if jid:
            D.add_log(jid, qid, mid, "INFO", "SESSION",
                      f"{hostname}: tasklist failed ({e})")

    if jid:
        D.add_log(jid, qid, mid, "WARN", "SESSION",
                  f"{hostname}: could not detect logged-in user via any method. "
                  f"Will try running task as configured user '{plain_user}' with /it. "
                  f"If macro fails, ensure the user is logged in and 'Remote Registry' + "
                  f"'Remote Desktop Services' are enabled on {hostname}.")
    return None



def _run_via_wmi(vbs_local_path, hostname, username, password, jid, qid, mid, mname):
    """
    Execute VBS via WMI Win32_Process.Create on the remote machine.
    This is the PREFERRED method when available:
    - Runs in the currently logged-in user's session automatically
    - Does NOT require the user to be Administrator
    - Does NOT need /it flag or session detection
    - Requires WMI to be accessible (port 135 + dynamic RPC)

    Returns (ok: bool, message: str)
    """
    try:
        cmd = [
            "wmic", f"/node:{hostname}",
            f"/user:{username}", f"/password:{password}",
            "process", "call", "create",
            f'cscript //NoLogo "{vbs_local_path}"'
        ]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if r.returncode == 0 and ("ReturnValue = 0" in r.stdout or "ProcessId" in r.stdout):
            # Extract PID if available
            pid = ""
            for line in r.stdout.splitlines():
                if "ProcessId" in line:
                    pid = line.strip()
                    break
            D.add_log(jid, qid, mid, "INFO", "WMI",
                      f"{hostname}: process started via WMI {pid}")
            return True, "WMI"
        err = (r.stderr.strip() or r.stdout.strip())[:200]
        return False, err
    except Exception as e:
        return False, str(e)


def _run_via_schtasks(vbs_local_path, hostname, username, password,
                      task_name, jid, qid, mid, mname, interactive=True):
    """
    Execute VBS via Windows Task Scheduler on the remote machine.
    Used as fallback when WMI is not available.

    Key fix: schtasks /run returns 0 even when an /it task has no interactive
    session — the task stays in 'Ready' state and never fires. We verify the
    task is actually RUNNING after triggering, and retry without /it if stuck.
    """
    task_cmd = f'cscript //NoLogo "{vbs_local_path}"'
    local = _is_local(hostname)

    configured_ru = username
    if "\\" in configured_ru:
        configured_ru = configured_ru.split("\\", 1)[1]

    mode = "LOCAL" if local else "REMOTE"

    # ── Step 1: Detect active session user ──
    ru_user = None
    use_password = False

    if not local:
        detected = _get_active_session_user(hostname, username, password, jid, qid, mid)
        if detected:
            ru_user = detected.split("\\")[-1].strip()
            use_password = False
            D.add_log(jid, qid, mid, "INFO", "SESSION",
                      f"{hostname}: active user = '{ru_user}' "
                      f"{'(same as configured)' if ru_user.lower() == configured_ru.lower() else f'[configured={configured_ru}]'}"
                      f" — task runs AS {ru_user}")
            D.update_machine_active_user(mid, ru_user)
            D.update_master_active_user(hostname, ru_user)
        else:
            ru_user = configured_ru
            use_password = True
            D.add_log(jid, qid, mid, "WARN", "SESSION",
                      f"{hostname}: no active session detected — falling back to '{ru_user}'.")
            D.update_machine_active_user(mid, "")
    else:
        ru_user = configured_ru
        use_password = True

    def _schtasks_base():
        """Base schtasks args for /s authentication."""
        if local:
            return []
        return ["/s", hostname, "/u", username, "/p", password]

    def _build_create(with_interactive, with_password, run_as_system=False):
        cmd = ["schtasks", "/create"] + _schtasks_base()
        cmd += ["/tn", task_name, "/tr", task_cmd,
                "/sc", "once", "/st", "00:00", "/f", "/rl", "highest"]
        if run_as_system:
            cmd += ["/ru", "SYSTEM"]          # always fires, no session needed
        else:
            cmd += ["/ru", ru_user]
            if with_password:
                cmd += ["/rp", password]
            if with_interactive:
                cmd.append("/it")
        return cmd

    def _build_run():
        return ["schtasks", "/run"] + _schtasks_base() + ["/tn", task_name]

    def _build_delete():
        return ["schtasks", "/delete"] + _schtasks_base() + ["/tn", task_name, "/f"]

    def _task_is_running():
        """Return True if the task is in Running state (not Ready/stuck)."""
        try:
            cmd = ["schtasks", "/query"] + _schtasks_base() + \
                  ["/tn", task_name, "/fo", "csv", "/nh"]
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
            if r.returncode == 0:
                status_line = r.stdout.strip()
                # CSV format: "TaskName","Next Run","Status"
                # Status values: Running, Ready, Disabled, etc.
                return "Running" in status_line
        except:
            pass
        return False

    D.add_log(jid, qid, mid, "INFO", "SCHTASKS",
              f"Scheduling task on {hostname} ({mode}) ru='{ru_user}'")

    # ── Step 2: Attempt order ──
    # 1. Detected/configured user + /it   (interactive, preferred — Excel visible)
    # 2. Detected/configured user, no /it  (headless — runs even without interactive session)
    # NOTE: SYSTEM fallback REMOVED — SYSTEM has no desktop and cannot open Excel.
    create_attempts = [
        (True,  use_password, False, "interactive /it"),
        (False, use_password, False, "headless (no /it)"),
    ]

    task_created = False
    used_label = ""
    for with_it, with_pw, as_system, label in create_attempts:
        create = _build_create(with_it, with_pw, as_system)
        r = subprocess.run(create, capture_output=True, text=True, timeout=30)
        if r.returncode == 0:
            task_created = True
            used_label = label
            D.add_log(jid, qid, mid, "INFO", "SCHTASKS",
                      f"Task created [{label}] on {hostname}")
            break
        err = (r.stderr.strip() or r.stdout.strip())[:200]
        D.add_log(jid, qid, mid, "WARN", "SCHTASKS",
                  f"Create [{label}] failed: {err}")

    if not task_created:
        D.add_log(jid, qid, mid, "ERROR", "SCHTASKS",
                  f"{hostname}: all create attempts failed.")
        return False

    # ── Step 3: Trigger the task ──
    try:
        r2 = subprocess.run(_build_run(), capture_output=True, text=True, timeout=15)
        if r2.returncode != 0:
            err = (r2.stderr.strip() or r2.stdout.strip())[:200]
            D.add_log(jid, qid, mid, "WARN", "SCHTASKS", f"Trigger failed: {err}")
            return False
    except Exception as e:
        D.add_log(jid, qid, mid, "WARN", "SCHTASKS", f"Error triggering task: {e}")
        return False

    # ── Step 4: Verify task actually started ──
    # schtasks /run returns 0 even when /it has no interactive session.
    # Task stays in 'Ready' and never fires. Detect this and retry.
    # SYSTEM fallback REMOVED: SYSTEM has no desktop and cannot open Excel.
    time.sleep(3)
    if not _task_is_running():
        D.add_log(jid, qid, mid, "WARN", "SCHTASKS",
                  f"Task stuck in Ready on {hostname} "
                  f"(created as [{used_label}], no interactive session for '{ru_user}'). "
                  f"Retrying headless...")
        try:
            subprocess.run(_build_delete(), capture_output=True, timeout=10)
        except:
            pass

        # Only retry headless if we haven't already tried it
        if used_label == "interactive /it":
            create = _build_create(False, use_password, False)
            r = subprocess.run(create, capture_output=True, text=True, timeout=30)
            if r.returncode == 0:
                r2 = subprocess.run(_build_run(), capture_output=True, text=True, timeout=15)
                if r2.returncode == 0:
                    time.sleep(3)
                    if _task_is_running():
                        D.add_log(jid, qid, mid, "INFO", "SCHTASKS",
                                  f"Task running [headless retry] on {hostname}")
                        return True
                    D.add_log(jid, qid, mid, "WARN", "SCHTASKS",
                              f"[headless retry] also stuck in Ready on {hostname}")
            else:
                err = (r.stderr.strip() or r.stdout.strip())[:200]
                D.add_log(jid, qid, mid, "WARN", "SCHTASKS", f"Headless retry create failed: {err}")

        D.add_log(jid, qid, mid, "ERROR", "SCHTASKS",
                  f"{hostname}: task not running after all attempts. "
                  f"Excel requires an interactive user session — SYSTEM cannot open Excel. "
                  f"FIX: ensure a user is logged in on {hostname}. "
                  f"Enable Windows services: Remote Desktop Services, Remote Registry, Task Scheduler. "
                  f"Firewall: allow Remote Scheduled Tasks Management.")
        return False

    D.add_log(jid, qid, mid, "INFO", "SCHTASKS",
              f"Task confirmed running on {hostname} as '{ru_user}' [{used_label}]")
    return True





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
