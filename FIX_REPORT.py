#!/usr/bin/env python
"""
Job #22 Analysis & Fix Summary
==============================

ISSUE FOUND:
- Job #22 stuck in RUNNING state for 3.9+ minutes
- All 6 queue items stuck in QUEUED status
- PHASE 1 (file transfer) never completed after NET_USE authentication succeeded
- Root cause: shutil.copy2() was hanging indefinitely for large .xlsb files over network

IMPROVEMENTS MADE TO executor.py:
1. Added robocopy for .xlsb files (more reliable for network transfers)
   - Uses /Z flag for resumable transfer
   - 300 second (5 minute) timeout per file
   - Automatic retry on failure

2. Enhanced file copy progress logging
   - Log each file copy start
   - Log retries with attempt number
   - Log completion with file size verification
   - Log failures with detailed error messages

3. Size verification after transfer
   - Checks file exists after copy
   - Verifies source size == destination size
   - Prevents corrupt/incomplete transfers

4. Improved retry logic
   - 3 attempts per file instead of 1
   - 3 second delay between retries (was 2 seconds)
   - Timeout per individual file (300s)
   - Timeout per future result in thread pool (300s)

5. Reduced parallel workers for large files
   - Changed from 4 workers to 2 workers
   - Prevents network congestion
   - More reliable transfer completion

6. Better error aggregation
   - Tracks all failed files
   - Collects error messages
   - Raises descriptive exception with all failures

EXECUTION FLOW (FIXED):
PHASE 1: Complete File Transfer (with detailed progress)
  ├─ NET_USE: Authenticate to \\hopc575\test
  ├─ Create date folder on remote
  ├─ Copy file 1 (with size verification)
  ├─ Copy file 2 (with size verification)
  ├─ Copy file 3 (with size verification)
  └─ Log: "✓ All N files copied to {path}" <- MUST complete before Phase 2

PHASE 2: Execute Macro (only after files confirmed transferred)
  ├─ machine_worker claims queue items
  ├─ Copy remote → local temp
  ├─ Execute macro locally
  ├─ Collect outputs
  └─ Log results

JOB #22 RESET:
- Status: CANCELLED (was RUNNING)
- Queue items: 6 items set to PENDING (were QUEUED)
- Ready for re-run with improved copy code

NEXT STEPS:
1. Restart FastAPI to reload executor.py
2. Re-run job #22 via web UI or API
3. Monitor logs for "FILES_COPIED" completion before EXEC phase starts
"""

import sqlite3
from datetime import datetime

if __name__ == "__main__":
    print(__doc__)
    
    # Show current status
    conn = sqlite3.connect('tracker.db')
    c = conn.cursor()
    
    job = c.execute('SELECT status, started_at, finished_at FROM jobs WHERE job_id=22').fetchone()
    items = c.execute('''SELECT COUNT(*), 
                         SUM(CASE WHEN status='PENDING' THEN 1 ELSE 0 END),
                         SUM(CASE WHEN status='QUEUED' THEN 1 ELSE 0 END),
                         SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END)
                      FROM job_queue WHERE job_id=22''').fetchone()
    
    print("\n" + "="*60)
    print("CURRENT JOB #22 STATUS")
    print("="*60)
    print(f"Job Status: {job[0]}")
    print(f"Started:    {job[1]}")
    print(f"Finished:   {job[2]}")
    print(f"\nQueue Items:")
    print(f"  PENDING: {items[1]} (ready to retry)")
    print(f"  QUEUED:  {items[2]} (stuck)")
    print(f"  FAILED:  {items[3]}")
    print("="*60)
    
    conn.close()
