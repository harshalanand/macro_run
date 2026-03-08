#!/usr/bin/env python
import sqlite3

conn = sqlite3.connect('tracker.db')
conn.row_factory = sqlite3.Row
c = conn.cursor()

# Get job 22 info
print('=== JOB 22 ===')
job = c.execute('SELECT * FROM jobs WHERE job_id=22').fetchone()
if job:
    for key in job.keys():
        print(f'{key}: {job[key]}')
else:
    print('Job 22 not found')
    conn.close()
    exit(1)

print('\n=== QUEUE ITEMS ===')
queue = c.execute('''SELECT q.*, m.machine_name FROM job_queue q 
                     LEFT JOIN machines m ON q.machine_id=m.machine_id 
                     WHERE q.job_id=22''').fetchall()
for item in queue:
    status = item['status']
    error = item['error_message'] or 'None'
    cat = item['cat_value']
    machine = item['machine_name'] or 'Unknown'
    duration = item['duration_secs'] or 0
    print(f'  {cat:20} | {machine:15} | {status:10} | Duration: {duration:.1f}s | Error: {error[:80]}')

print('\n=== LOGS ===')
logs = c.execute('''SELECT log_level, step, message, created_at 
                    FROM run_logs WHERE job_id=22 
                    ORDER BY created_at''').fetchall()
for log in logs:
    print(f'{log["created_at"][-8:]} {log["log_level"]:5} {log["step"]:15} {log["message"][:100]}')

conn.close()
