"""
Queue-based executor v4:
- Copy files to REMOTE machine (date folder)
- For macro: copy excel to LOCAL temp dir -> paste category -> run macro -> copy output back
- This solves Excel COM not being able to open files from UNC paths
- Queue: N machines process M categories, auto-redistribute when free
"""
import os, json, shutil, subprocess, threading, tempfile
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
                   message=f"Job #{jid}: {len(queue_items)} categories across {len(machines)} machines")

        # PHASE 1: Prep all machines in parallel (auth + folder + copy)
        machine_ready = {}
        with ThreadPoolExecutor(max_workers=min(len(machines), 8)) as pool:
            futs = {pool.submit(_prep_machine, m, today, files, jid): m for m in machines}
            for fut in as_completed(futs):
                m = futs[fut]
                try:
                    date_folder = fut.result()
                    machine_ready[m["machine_id"]] = date_folder
                    D.add_log(jid, machine_id=m["machine_id"], level="INFO", step="PREP_OK",
                              message=f"{m['machine_name']}: ready at {date_folder}")
                except Exception as e:
                    D.add_log(jid, machine_id=m["machine_id"], level="ERROR", step="PREP_FAIL",
                              message=f"{m['machine_name']}: {e}")
                    N.notify_copy_failure(jid, None, m["machine_name"],
                                          m["ip_address"], m["shared_folder"], str(e), "PREP")

        if not machine_ready:
            D.add_log(jid, level="ERROR", step="ABORT", message="No machines available")
            D.finish_job(jid); return

        # PHASE 2: Workers process queue
        def machine_worker(mid, remote_date_folder):
            m = D.get_machine(mid)
            mname = m["machine_name"]

            while True:
                item = D.claim_next(jid, mid)
                if not item:
                    break

                qid = item["queue_id"]
                cat = item["cat_value"]
                start = datetime.now()
                local_work = None
                D.add_log(jid, qid, mid, "INFO", "CAT_START", f"{mname}: category '{cat}'")

                try:
                    # Step 1: Copy ALL files from remote to LOCAL temp folder
                    local_work = tempfile.mkdtemp(prefix=f"macro_{mname[:10]}_{cat[:10]}_")
                    D.add_log(jid, qid, mid, "INFO", "LOCAL_COPY",
                              f"Copying to local temp: {local_work}")

                    for fname in os.listdir(remote_date_folder):
                        src = os.path.join(remote_date_folder, fname)
                        if os.path.isfile(src) and not fname.startswith("_"):
                            shutil.copy2(src, os.path.join(local_work, fname))

                    local_excel = os.path.join(local_work, excel_file)
                    if not os.path.exists(local_excel):
                        raise FileNotFoundError(f"Macro file not in local temp: {excel_file}")

                    D.add_log(jid, qid, mid, "INFO", "MACRO_RUN",
                              f"Running macro on LOCAL: {local_excel} cell={target_cell}='{cat}'")

                    # Step 2: Paste category + run macro on LOCAL file
                    vbs_out = _paste_and_run(local_excel, macro_name, target_cell, cat)
                    D.add_log(jid, qid, mid, "INFO", "MACRO_DONE",
                              f"{mname}: '{cat}' macro OK | {vbs_out[:150]}")

                    # Step 3: Find new output files
                    original_names = {f["original_name"] for f in files}
                    local_files = set(os.listdir(local_work))
                    new_files = [f for f in (local_files - original_names)
                                 if not f.startswith("_macro_") and not f.startswith("~$")
                                 and os.path.isfile(os.path.join(local_work, f))]

                    # Step 4: Copy outputs back to remote + compile dir
                    if new_files:
                        for nf in new_files:
                            try:
                                shutil.copy2(os.path.join(local_work, nf),
                                             os.path.join(remote_date_folder, nf))
                            except Exception as ce:
                                D.add_log(jid, qid, mid, "WARN", "COPYBACK",
                                          f"Failed copying {nf} to remote: {ce}")

                        out_dir = os.path.join(compile_dir, today, group_name, mname, cat)
                        os.makedirs(out_dir, exist_ok=True)
                        for nf in new_files:
                            shutil.copy2(os.path.join(local_work, nf),
                                         os.path.join(out_dir, nf))
                            sz = os.path.getsize(os.path.join(out_dir, nf)) / 1024
                            D.add_log(jid, qid, mid, "INFO", "OUTPUT",
                                      f"Collected: {nf} ({sz:.1f}KB)")
                        D.finish_queue_item(qid, "RUNNING", output_files=json.dumps(new_files))
                    else:
                        # Copy updated macro file back to remote
                        try:
                            shutil.copy2(local_excel,
                                         os.path.join(remote_date_folder, excel_file))
                        except: pass
                        D.add_log(jid, qid, mid, "INFO", "NO_NEW_OUTPUT",
                                  "No new output files (macro may update the file itself)")

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
                finally:
                    if local_work:
                        try: shutil.rmtree(local_work, ignore_errors=True)
                        except: pass

        # Launch workers
        with ThreadPoolExecutor(max_workers=len(machine_ready)) as pool:
            futs = [pool.submit(machine_worker, mid, df)
                    for mid, df in machine_ready.items()]
            for w in as_completed(futs):
                try: w.result()
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


def _prep_machine(machine, today, files, jid):
    """Auth + create date folder + copy files. Returns remote date_folder."""
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


def _net_use(unc_path, username, password, jid=None, mid=None):
    """Authenticate via net use. Extracts \\\\server\\share correctly."""
    clean = unc_path.replace("/", "\\").rstrip("\\")
    parts = [p for p in clean.split("\\") if p]

    if len(parts) >= 2:
        server_share = f"\\\\{parts[0]}\\{parts[1]}"
    elif len(parts) == 1:
        server_share = f"\\\\{parts[0]}"
    else:
        D.add_log(jid, machine_id=mid, level="WARN", step="NET_USE",
                  message=f"Bad UNC path: {unc_path}")
        return

    D.add_log(jid, machine_id=mid, level="INFO", step="NET_USE",
              message=f"Running: net use {server_share} /user:{username}")

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
                  message=f"OK: authenticated to {server_share}")


def _paste_and_run(local_excel_path, macro_name, target_cell, cat_value, timeout=600):
    """Open LOCAL excel, paste category, run macro, save, close."""
    excel_path = os.path.abspath(local_excel_path).replace("/", "\\")
    vbs_path = os.path.join(os.path.dirname(excel_path), f"_macro_{os.getpid()}.vbs")
    safe_cat = str(cat_value).replace('"', '""')

    vbs = f'''
Dim fso
Set fso = CreateObject("Scripting.FileSystemObject")
If Not fso.FileExists("{excel_path}") Then
    WScript.Echo "ERROR_OPEN:File not found: {excel_path}"
    WScript.Quit 1
End If
Set fso = Nothing

On Error Resume Next
Dim xlApp
Set xlApp = CreateObject("Excel.Application")
If Err.Number <> 0 Then
    WScript.Echo "ERROR_OPEN:Cannot start Excel: " & Err.Description
    WScript.Quit 1
End If

xlApp.Visible = False
xlApp.DisplayAlerts = False
xlApp.AskToUpdateLinks = False
Err.Clear

WScript.Echo "OPENING: {excel_path}"
Dim xlWb
Set xlWb = xlApp.Workbooks.Open("{excel_path}")
If Err.Number <> 0 Then
    WScript.Echo "ERROR_OPEN:" & Err.Description
    xlApp.Quit: Set xlApp = Nothing: WScript.Quit 1
End If
Err.Clear

WScript.Echo "PASTING: {safe_cat} into {target_cell}"
Dim xlWs
Set xlWs = xlWb.Sheets(1)
xlWs.Range("{target_cell}").Value = "{safe_cat}"
If Err.Number <> 0 Then
    WScript.Echo "ERROR_PASTE:" & Err.Description
    xlWb.Close False: xlApp.Quit: Set xlApp = Nothing: WScript.Quit 3
End If
Err.Clear

WScript.Echo "RUNNING: {macro_name}"
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
        if r.returncode != 0 or "ERROR_" in out:
            raise RuntimeError(f"exit={r.returncode}: {out} {err}")
        return out
    finally:
        try: os.remove(vbs_path)
        except: pass
