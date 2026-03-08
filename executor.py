"""
Queue-based executor v5:
- PREP:    Copy files to remote machine date folder (unchanged)
- EXEC:    Write VBS to remote share → execute ON the remote machine via wmic
           VBS writes output to a log file; Python polls the log via UNC share
- COLLECT: New output files appear in remote date folder → copy to compile_dir
- FALLBACK: If system_name is absent / wmic unavailable, fall back to local execution
"""
import os, json, shutil, subprocess, threading, tempfile, time, csv
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

        # PHASE 1: Copy files to all machines in parallel
        machine_ready = {}
        with ThreadPoolExecutor(max_workers=min(len(machines), 8)) as pool:
            futs = {pool.submit(_prep_machine, m, today, files, jid): m for m in machines}
            for fut in as_completed(futs):
                m = futs[fut]
                try:
                    unc_folder = fut.result()
                    machine_ready[m["machine_id"]] = unc_folder
                    D.add_log(jid, machine_id=m["machine_id"], level="INFO", step="PREP_OK",
                              message=f"{m['machine_name']}: files copied to {unc_folder}")
                except Exception as e:
                    D.add_log(jid, machine_id=m["machine_id"], level="ERROR", step="PREP_FAIL",
                              message=f"{m['machine_name']}: {e}")
                    N.notify_copy_failure(jid, None, m["machine_name"],
                                          m["ip_address"], m["shared_folder"], str(e), "PREP")

        if not machine_ready:
            D.add_log(jid, level="ERROR", step="ABORT", message="No machines available")
            D.finish_job(jid); return

        # PHASE 2: Workers process queue - SIMPLIFIED: Copy → Execute Locally → Collect
        def machine_worker(mid, remote_date_folder, can_run_remote):
            m = D.get_machine(mid)
            mname = m["machine_name"]

            while True:
                item = D.claim_next(jid, mid)
                if not item:
                    break

                qid = item["queue_id"]
                cat = item["cat_value"]
                start = datetime.now()
                new_files = []
                D.add_log(jid, qid, mid, "INFO", "CAT_START", f"{mname}: category '{cat}'")

                try:
                    # ── SIMPLE: Copy → Execute Locally → Collect ──
                    # Copy shared folder contents to local temp
                    local_work = tempfile.mkdtemp(prefix=f"macro_{mname[:10]}_{cat[:10]}_")
                    try:
                        D.add_log(jid, qid, mid, "INFO", "LOCAL_COPY",
                                  f"Copying to local: {local_work}")
                        
                        # List all files to copy
                        available_files = []
                        for fname in os.listdir(remote_date_folder):
                            src = os.path.join(remote_date_folder, fname)
                            if os.path.isfile(src) and not fname.startswith("_") and not fname.startswith("~$"):
                                try:
                                    size = os.path.getsize(src)
                                    available_files.append((fname, src, size))
                                except:
                                    pass  # Skip if can't stat
                        
                        D.add_log(jid, qid, mid, "INFO", "LOCAL_COPY_LIST",
                                  f"Found {len(available_files)} files to copy")
                        
                        # Copy each file with retry and verification
                        copy_errors = []
                        for fname, src, expected_size in available_files:
                            dst = os.path.join(local_work, fname)
                            success = False
                            
                            for attempt in range(3):
                                try:
                                    # Add small delay before copy to avoid locking
                                    if attempt > 0:
                                        time.sleep(2)
                                    
                                    # Copy file
                                    shutil.copy2(src, dst)
                                    time.sleep(0.5)  # Let filesystem catch up
                                    
                                    # Verify copy
                                    if not os.path.exists(dst):
                                        raise OSError(f"Copy verification failed: {dst} not found")
                                    
                                    actual_size = os.path.getsize(dst)
                                    if actual_size != expected_size:
                                        raise OSError(f"Size mismatch: {actual_size} != {expected_size}")
                                    
                                    D.add_log(jid, qid, mid, "INFO", "LOCAL_COPY_OK",
                                              f"{fname} ({actual_size/1024/1024:.1f}MB)")
                                    success = True
                                    break
                                    
                                except Exception as e:
                                    if attempt < 2:
                                        D.add_log(jid, qid, mid, "WARN", "LOCAL_COPY_RETRY",
                                                  f"{fname}: attempt {attempt+1}/3: {str(e)[:80]}")
                                    else:
                                        msg = f"{fname}: {str(e)[:100]}"
                                        copy_errors.append(msg)
                                        D.add_log(jid, qid, mid, "ERROR", "LOCAL_COPY_FAIL", msg)
                            
                            if not success and fname == excel_file:
                                # If we can't copy the main Excel file, fail immediately
                                raise RuntimeError(f"Cannot copy required file: {excel_file}")

                        if copy_errors:
                            raise RuntimeError(f"Copy failures: {'; '.join(copy_errors)}")

                        # Verify all required files are present before execution
                        local_excel = os.path.join(local_work, excel_file)
                        if not os.path.exists(local_excel):
                            local_files = os.listdir(local_work)
                            raise FileNotFoundError(f"Excel file '{excel_file}' not in workspace. Found: {local_files}")

                        # Execute macro locally
                        D.add_log(jid, qid, mid, "INFO", "MACRO_RUN",
                                  f"Executing: {excel_file} cell={target_cell}='{cat}'")
                        vbs_out = _paste_and_run(local_excel, macro_name, target_cell, cat)
                        D.add_log(jid, qid, mid, "INFO", "MACRO_DONE",
                                  f"Macro executed successfully")

                        # Find new output files
                        original_names = {f["original_name"] for f in files}
                        local_files = set(os.listdir(local_work))
                        new_files = [
                            f for f in (local_files - original_names)
                            if not f.startswith("_") and not f.startswith("~$")
                            and os.path.isfile(os.path.join(local_work, f))
                        ]

                        # Copy output files back to remote shared folder
                        for nf in new_files:
                            try:
                                shutil.copy2(os.path.join(local_work, nf),
                                             os.path.join(remote_date_folder, nf))
                                D.add_log(jid, qid, mid, "INFO", "OUTPUT_COPY",
                                         f"Copied back: {nf}")
                            except Exception as e:
                                D.add_log(jid, qid, mid, "WARN", "OUTPUT_COPY_FAIL",
                                         f"Failed to copy {nf}: {e}")
                    finally:
                        try: shutil.rmtree(local_work, ignore_errors=True)
                        except: pass

                    # ── Collect outputs to compile dir ──────────────────
                    if new_files:
                        out_dir = os.path.join(compile_dir, today, group_name, mname, cat)
                        os.makedirs(out_dir, exist_ok=True)
                        for nf in new_files:
                            shutil.copy2(os.path.join(remote_date_folder, nf),
                                         os.path.join(out_dir, nf))
                            sz = os.path.getsize(os.path.join(out_dir, nf)) / 1024
                            D.add_log(jid, qid, mid, "INFO", "OUTPUT",
                                      f"Collected: {nf} ({sz:.1f}KB)")
                        D.finish_queue_item(qid, "RUNNING", output_files=json.dumps(new_files))
                    else:
                        D.add_log(jid, qid, mid, "INFO", "NO_NEW_OUTPUT",
                                  "No new output files created")

                    elapsed = (datetime.now() - start).total_seconds()
                    D.finish_queue_item(qid, "SUCCESS",
                        finished_at=datetime.now().isoformat(),
                        date_folder=remote_date_folder, duration_secs=elapsed)
                    D.add_log(jid, qid, mid, "INFO", "CAT_DONE",
                              f"Category '{cat}' done in {elapsed:.1f}s")

                except Exception as e:
                    elapsed = (datetime.now() - start).total_seconds()
                    D.finish_queue_item(qid, "FAILED",
                        finished_at=datetime.now().isoformat(),
                        error_message=str(e)[:500], duration_secs=elapsed)
                    D.add_log(jid, qid, mid, "ERROR", "CAT_FAIL",
                              f"Category '{cat}' failed: {e}")
                    N.notify_macro_failure(jid, qid, mname,
                        m["ip_address"], excel_file, macro_name, str(e), cat)

        # Launch workers - SIMPLE: Copy → Execute → Collect
        with ThreadPoolExecutor(max_workers=len(machine_ready)) as pool:
            futs = [pool.submit(machine_worker, mid, unc_folder, True)
                    for mid, unc_folder in machine_ready.items()]
            for w in as_completed(futs):
                try: w.result()
                except Exception as e:
                    D.add_log(jid, level="ERROR", step="WORKER_CRASH", message=str(e))

        D.finish_job(jid)
        D.add_log(jid, level="INFO", step="JOB_DONE", message=f"Job #{jid} completed")
        
        # Track all collected output files
        success, msg = track_collected_files(jid)
        if success:
            D.add_log(jid, level="INFO", step="TRACKER_CREATED", message=msg)
        else:
            D.add_log(jid, level="WARN", step="TRACKER_SKIP", message=f"Tracker creation skipped: {msg}")

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
    """Copy files to shared folder date folder.
    Returns unc_date_folder path.
    """
    shared = machine["shared_folder"].strip()
    username = (machine["username"] or "").strip()
    password = (machine["password"] or "").strip()
    mid = machine["machine_id"]

    if shared.startswith("\\\\") and username and password:
        _net_use(shared, username, password, jid, mid)

    date_folder = os.path.join(shared, today)
    try:
        os.makedirs(date_folder, exist_ok=True)
    except PermissionError:
        if username and password:
            _net_use(f"\\\\{shared.split(chr(92))[2]}", username, password, jid, mid)
            os.makedirs(date_folder, exist_ok=True)
        else:
            raise

    def copy_one(f):
        dst = os.path.join(date_folder, f["original_name"])
        fname = f["original_name"]
        src = f["stored_path"]
        last_err = None
        
        for attempt in range(3):
            try:
                # Log each file copy attempt
                if attempt == 0:
                    D.add_log(jid, machine_id=mid, level="INFO", step="FILE_COPY",
                              message=f"Copying: {fname}")
                else:
                    D.add_log(jid, machine_id=mid, level="INFO", step="FILE_COPY_RETRY",
                              message=f"Retry {attempt}: {fname}")
                
                # Check if source exists
                if not os.path.exists(src):
                    raise FileNotFoundError(f"Source not found: {src}")
                
                # Copy with timeout using subprocess for large files
                try:
                    # For .xlsb files, use robocopy for more reliable transfer
                    if fname.endswith('.xlsb'):
                        r = subprocess.run(
                            ["robocopy", os.path.dirname(src), date_folder, fname, "/Z", "/R:2", "/W:2"],
                            capture_output=True, text=True, timeout=300
                        )
                        if r.returncode > 7:  # robocopy returns 0-7 for success
                            raise OSError(f"robocopy failed: {r.stderr}")
                    else:
                        # For other files, use shutil with a manual timeout check
                        shutil.copy2(src, dst)
                    
                    # Verify copy completed
                    if not os.path.exists(dst):
                        raise OSError(f"Copy verification failed: {dst} not found after copy")
                    
                    src_size = os.path.getsize(src)
                    dst_size = os.path.getsize(dst)
                    if src_size != dst_size:
                        raise OSError(f"Size mismatch: {src_size} vs {dst_size}")
                    
                    D.add_log(jid, machine_id=mid, level="INFO", step="FILE_COPY_OK",
                              message=f"Copied OK: {fname} ({dst_size/1024/1024:.1f}MB)")
                    return
                    
                except subprocess.TimeoutExpired:
                    last_err = TimeoutError(f"Copy timeout after 300s: {fname}")
                    
            except Exception as e:
                last_err = e
                if attempt < 2:
                    time.sleep(3)  # Wait 3 seconds before retry
                    
        # All retries exhausted
        D.add_log(jid, machine_id=mid, level="ERROR", step="FILE_COPY_FAIL",
                  message=f"Failed to copy {fname}: {last_err}")
        raise last_err

    # Copy all files with sequential or parallel approach
    failed = []
    max_workers = min(len(files), 2)  # Reduce parallel workers for large files
    D.add_log(jid, machine_id=mid, level="INFO", step="PREP_START",
              message=f"Starting copy of {len(files)} files with {max_workers} workers")
    
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futs = {pool.submit(copy_one, f): f["original_name"] for f in files}
        for fut in as_completed(futs):
            fname = futs[fut]
            try:
                fut.result(timeout=300)  # 5 minute timeout per file
            except Exception as e:
                failed.append((fname, str(e)))
                D.add_log(jid, machine_id=mid, level="ERROR", step="FILE_FAIL",
                          message=f"{fname}: {str(e)[:150]}")

    if failed:
        msg = "; ".join([f"{f[0]}: {f[1][:50]}" for f in failed])
        raise Exception(f"Failed to copy {len(failed)} files: {msg}")
    
    D.add_log(jid, machine_id=mid, level="INFO", step="FILES_COPIED",
              message=f"✓ All {len(files)} files copied to {date_folder}")

    return date_folder


def _net_use(unc_path, username, password, jid=None, mid=None):
    """Authenticate via net use. Extracts \\\\server\\share correctly."""
    clean = unc_path.replace("/", "\\").rstrip("\\")
    parts = [p for p in clean.split("\\") if p]

    if len(parts) >= 2:
        server_share = f"\\\\{parts[0]}\\{parts[1]}"
        server_only  = f"\\\\{parts[0]}"
    elif len(parts) == 1:
        server_share = f"\\\\{parts[0]}"
        server_only  = server_share
    else:
        D.add_log(jid, machine_id=mid, level="WARN", step="NET_USE",
                  message=f"Bad UNC path: {unc_path}")
        return

    # Skip auth if username and password not provided
    if not username or not password:
        D.add_log(jid, machine_id=mid, level="INFO", step="NET_USE",
                  message=f"Skipping auth (no credentials): {server_share}")
        return

    D.add_log(jid, machine_id=mid, level="INFO", step="NET_USE",
              message=f"Running: net use {server_share} /user:{username}")

    # Aggressively disconnect ALL sessions to prevent error 1219
    for path in [server_only, server_share]:
        for _ in range(2):  # Try twice to be sure
            try:
                subprocess.run(["net", "use", path, "/delete", "/y"],
                              capture_output=True, timeout=10)
                time.sleep(0.5)
            except:
                pass

    # Add modest delay to ensure cleanup is complete
    time.sleep(1)

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
    
    # Check file exists and is readable before VBS execution
    if not os.path.exists(excel_path):
        raise FileNotFoundError(f"Excel file not found: {excel_path}")
    
    try:
        # Try to open file to verify it's not locked and readable
        with open(excel_path, 'rb') as f:
            header = f.read(4)
            if header[:2] != b'PK':  # .xlsb files are ZIP format
                raise ValueError(f"File appears corrupted (not valid .xlsb): {excel_path}")
    except Exception as e:
        raise RuntimeError(f"Cannot read Excel file: {e}")
    
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
Set xlWb = xlApp.Workbooks.Open("{excel_path}", 0)
If Err.Number <> 0 Then
    WScript.Echo "ERROR_OPEN:" & Err.Number & ":" & Err.Description
    xlApp.Quit: Set xlApp = Nothing: WScript.Sleep 1000: WScript.Quit 1
End If
Err.Clear

WScript.Echo "PASTING: {safe_cat} into {target_cell}"
Dim xlWs
Set xlWs = xlWb.Sheets(1)
xlWs.Range("{target_cell}").Value = "{safe_cat}"
If Err.Number <> 0 Then
    WScript.Echo "ERROR_PASTE:" & Err.Number & ":" & Err.Description
    xlWb.Close False: xlApp.Quit: Set xlApp = Nothing: WScript.Sleep 1000: WScript.Quit 3
End If
Err.Clear

WScript.Echo "RUNNING: {macro_name}"
xlApp.Run "{macro_name}"
If Err.Number <> 0 Then
    WScript.Echo "ERROR_MACRO:" & Err.Number & ":" & Err.Description
    xlWb.Close False: xlApp.Quit: Set xlApp = Nothing: WScript.Sleep 1000: WScript.Quit 2
End If

xlWb.Save
xlWb.Close False
xlApp.Quit
Set xlApp = Nothing
WScript.Sleep 1000
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

def track_collected_files(jid):
    """
    Create a tracker file listing all collected output files.
    Writes to: compiled_output/{date}/{group}/TRACKER_{timestamp}.csv
    Format: job_id,date,group,machine,category,filename,filepath,collected_at
    """
    job = D.get_job(jid)
    if not job:
        return False, "Job not found"
    
    today = datetime.now().strftime("%Y-%m-%d")
    group_name = job["group_name"]
    compile_base = D.get_setting("compile_path", "") or COMPILED_DIR
    compile_root = os.path.join(compile_base, today, group_name)
    
    try:
        # Create directory if it doesn't exist
        os.makedirs(compile_root, exist_ok=True)
        
        tracker_path = os.path.join(compile_root, 
                                   f"TRACKER_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        
        collected_files = []
        
        # Get all queue items with outputs
        queue_items = D.get_queue(jid)
        for item in queue_items:
            item_dict = dict(item)  # Convert sqlite3.Row to dict
            if item_dict.get("output_files"):
                try:
                    files = json.loads(item_dict["output_files"])
                    for fname in files:
                        output_path = os.path.join(
                            item_dict["date_folder"] or "",
                            fname
                        )
                        collected_files.append({
                            "job_id": jid,
                            "date": today,
                            "group": group_name,
                            "machine": item_dict["machine_name"] or "Unknown",
                            "category": item_dict["cat_value"],
                            "filename": fname,
                            "filepath": output_path,
                            "collected_at": datetime.now().isoformat()
                        })
                except:
                    pass
        
        # Write tracker CSV regardless of whether files exist (audit trail)
        with open(tracker_path, "w", newline="", encoding="utf-8") as f:
            fieldnames = ["job_id", "date", "group", "machine", "category", 
                         "filename", "filepath", "collected_at"]
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(collected_files)
        
        if collected_files:
            D.add_log(jid, level="INFO", step="TRACKER_CREATED",
                    message=f"Created tracker: {os.path.basename(tracker_path)} ({len(collected_files)} files)")
            return True, f"Tracked {len(collected_files)} files in {os.path.basename(tracker_path)}"
        else:
            D.add_log(jid, level="INFO", step="TRACKER_CREATED",
                    message=f"Created tracker: {os.path.basename(tracker_path)} (no output files)")
            return True, f"Tracker created (0 files collected - all tasks may have failed)"
    
    except Exception as e:
        D.add_log(jid, level="ERROR", step="TRACKER_FAIL", message=str(e))
        return False, str(e)
