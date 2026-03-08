# Macro Orchestrator v3 — Queue-Based Execution with Central Compilation

FastAPI web app to distribute Excel macro runs across multiple Windows machines with category queuing, email alerts, full audit logging, and **centralized output compilation**.

## Key Concept

**Groups** → each has its own machines, files, and category list
**Categories** → list of Maj/Cat numbers to process (e.g., 25 items)
**Machines** → pool of Windows PCs (e.g., 10 machines)
**Queue** → 25 categories run across 10 machines. When a machine finishes one category, it automatically picks the next from the queue.
**Compilation** → After execution, all output files are automatically merged into a central consolidated workbook

## Execution Flow

```
For each category in the queue:
  1. Pick a free machine from the pool
  2. Open the Excel macro file on that machine (via remote execution or local fallback)
  3. Paste the category value into the target cell (e.g., A1)
  4. Run the macro
  5. Collect output files back to compiled folder
  6. Machine becomes free → picks next category

After all categories complete:
  7. **Compile outputs**: Merge all Excel sheets from all machines/categories
  8. Create CONSOLIDATED_OUTPUT_{timestamp}.xlsx in group folder
  9. Send summary email with results
```

## Quick Start

```bash
cd macro_orchestrator
pip install -r requirements.txt
python -m uvicorn main:app --host 0.0.0.0 --port 8000

# Or on Windows:
run.bat
```

Open **http://localhost:8000**

## Setup Workflow

### 1. Create Group
Groups > + New Group (e.g., "Monthly Reports")

### 2. Configure Macro
On the group page, set:
- **Excel File to Open**: `Report.xlsm` (filename that will be opened)
- **Macro Name**: `Module1.RunReport` (VBA macro to execute)
- **Paste Cell**: `A1` (cell where category value is pasted before macro runs)

### 3. Upload Files
Upload all Excel files needed (support files + the macro file). Click "Set as Macro" on the file that contains the macro.

### 4. Add Categories
Add Maj/Cat numbers one by one, or **Import CSV** with a list.
Example CSV:
```
cat_value
MAJ001
MAJ002
CAT-100
CAT-200
```

### 5. Add Machines / Remote Systems
Add manually or **Import CSV**. Download the template for correct format.
CSV columns: `name, system_name, ip, shared_folder, username, password, department, location`

**Remote Execution via WMIC**:
- If `system_name` is provided (e.g., `PCSAL01` or IP), the macro runs **on the remote machine itself**
- Excel process spawned via `wmic` (Windows Management Instrumentation Command-line)
- VBS script executes on remote machine, accesses network files via UNC paths
- Output files created on remote machine → pulled back to central folder

**Local Fallback**:
- If `system_name` is empty or wmic fails, files copy to local temp folder
- Macro runs locally, results copy back to remote share
- Useful for test/debug scenarios

**System access**: If the shared folder is a UNC path (\\server\share):
1. Try direct access first
2. If access denied and username/password provided → authenticate via `net use`
3. System name can be `\\HOSTNAME\share` instead of IP

Example machines:

| Name | System Name | IP | Shared Folder | User | Notes |
|------|-------------|----|----|------|-------|
| SALES-PC-01 | PCSAL01 | 192.168.1.101 | \\\\PCSAL01\\Reports\\Macro | domain\admin | Remote execution via wmic |
| SALES-PC-02 | PCSAL02 | 192.168.1.102 | \\\\192.168.1.102\\macro | admin | Remote via IP |
| LOCAL-TEST | (empty) | localhost | C:\\Macro\\Temp | (empty) | Local fallback (no remote exec) |

### 6. Configure Compilation
In **Settings** tab:
- **Compile Path**: Where consolidated outputs are saved
  - Default: `./compiled_output`
  - Example: `D:\\Reports\\Compiled` or `\\\\server\\reports\\Final`

### 7. Run Job
Click **Run Job**:
1. System copies all files to each remote machine's date-stamped folder (parallel)
2. Queues all categories for processing
3. Each machine claims categories from queue, runs macro, collects outputs
4. **Auto-consolidation**: When all categories done, system merges Excel sheets
5. Creates `CONSOLIDATED_OUTPUT_{timestamp}.xlsx` in compile folder
6. Sends summary email

## Remote Execution Architecture

### VBS Over WMIC Flow

```
Python Controller
    ↓
writes VBS + opens Excel file on remote machine via wmic
    ↓
Remote Machine (PCSAL01)
    ├─ cscript runs VBS via UNC: \\server\share\_macro_xxx.vbs
    └─ VBS opens Excel via UNC: \\server\share\Report.xlsm
        ├─ Pastes category into A1
        ├─ Runs macro Module1.RunReport
        ├─ Creates output: \\server\share\OUTPUT_MAJ001.xlsx
        └─ Writes log: \\server\share\_log_xxx.txt (_DONE_ sentinel)
    ↓
Python polls log via UNC until _DONE_ appears
    ↓
Python detects new files, copies to compiled_output/{date}/{group}/{machine}/{category}/
```

### Why This Works

- **No RDP/Login**: Uses SMB credentials or domain authentication
- **Shared Storage**: Central folder accessible from both controller and all machines
- **Scale**: Many machines, many categories, parallel execution
- **Audit**: Every step logged; VBS output captured

## File Collection & Compilation

### During Execution
- Each machine's output files → `compiled_output/{date}/{group}/{machine}/{category}/`
- Files organized by: date → group → machine → category

### Compilation Phase (Auto-triggered)
1. Scans all output folders for machine/category combinations
2. For each Excel file (`.xlsx`, `.xlsm`):
   - Opens and reads all worksheets
   - Creates new sheet in master workbook: `{machine_name}_{category}`
   - Copies all data (values + formatting)
3. Saves: `CONSOLIDATED_OUTPUT_{HH:MM:SS}.xlsx` in group compile folder
4. Logs all collected files and merge status

### Manual Compilation
- POST `/api/jobs/{jid}/compile` triggers compilation manually
- Useful if auto-compilation was skipped or you need to re-run

## Email Alerts

Configure in **Settings**:
- SMTP host/port/username/password
- Recipient emails (comma-separated)
- **Test Email** button to verify configuration

Alerts sent for:
- Machine prep failure (can't create folder, can't copy files)
- Macro execution failure (per category)
- File collection/compilation errors
- Job summary (if any failures occurred)

Email body shows:
- Machine name, IP, shared folder
- Category (for macro failures)
- Detailed error message
- Timestamp

## Settings

| Setting | Purpose | Example |
|---------|---------|---------|
| Compile Path | Where consolidated outputs saved + final workbook | `D:\\Reports\\Compiled` |
| SMTP Host | Email server | `smtp.gmail.com` |
| SMTP Port | SMTP port | `587` |
| SMTP Username | Login (often email) | `reports@company.com` |
| SMTP Password | Encrypted in DB | (hidden) |
| SMTP From | Sender address | `orchestrator@company.com` |
| Recipients | Alert emails | `team@company.com,boss@company.com` |
| Email Enabled | Toggle alerts on/off | ✓ |

## Output Structure

```
compiled_output/
  2026-02-27/
    Monthly Reports/                          ← group name
      PC-SALES-01/                            ← machine 1
        MAJ001/                               ← category
          OUTPUT_MAJ001.xlsx                  ← output file
        MAJ002/
          OUTPUT_MAJ002.xlsx
      PC-SALES-02/                            ← machine 2
        MAJ003/
          OUTPUT_MAJ003.xlsx
        MAJ004/
          OUTPUT_MAJ004.xlsx
      CONSOLIDATED_OUTPUT_145930.xlsx         ← merged workbook (auto-created)
                                              (contains all sheets from all machines/categories)
```

### Consolidated Workbook Structure
Each sheet named: `{MACHINE_NAME}_{CATEGORY}` (e.g., `PC-SALES-01_MAJ001`)
- All data from corresponding output files merged
- Ready for distribution/analysis
- Timestamp ensures no overwrites

## Database Tables

| Table | Purpose |
|-------|---------|
| machine_groups | Groups with macro config (file, macro, cell) |
| machines | Per-group with system_name, credentials, IP |
| group_files | Uploaded Excel files per group |
| categories | Maj/Cat list per group |
| jobs | Execution runs with status timestamps |
| job_queue | Category queue with machine assignment + outputs |
| run_logs | Detailed audit trail (PREP, EXEC, COMPILE, etc.) |
| email_log | All notifications sent with status |
| settings | SMTP config, compile path |

Log steps:
- `PREP_OK` / `PREP_FAIL` : File copy to remote
- `MACRO_RUN` / `MACRO_DONE` : VBS execution on remote
- `OUTPUT` : File collected to compile folder
- `COMPILE_SUCCESS` / `COMPILE_FAIL` / `COMPILE_SKIP` : Consolidation status
- `JOB_DONE` : All categories + compilation complete

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Dashboard |
| GET | `/groups` | Group list |
| GET | `/groups/{id}` | Group detail with config |
| POST | `/api/groups` | Create group |
| POST | `/api/groups/{id}/update` | Update macro config |
| POST | `/api/groups/{id}/delete` | Delete group + all data |
| POST | `/api/groups/{id}/machines` | Add machine |
| POST | `/api/groups/{id}/import-machines` | CSV import |
| POST | `/api/groups/{id}/upload` | Upload files |
| POST | `/api/groups/{id}/categories` | Add category |
| POST | `/api/groups/{id}/import-categories` | CSV import |
| POST | `/api/groups/{id}/run` | Start job |
| GET | `/api/jobs/{id}/status` | Live status (JSON) |
| POST | `/api/jobs/{id}/compile` | Manual compilation trigger |
| GET | `/jobs/{id}` | Job detail page |
| GET | `/logs` | Full audit log |
| GET | `/settings` | Settings page |
| POST | `/api/settings` | Save settings |
| POST | `/api/settings/test-email` | Send test email |
| GET | `/api/templates/machines.csv` | Download template |
| GET | `/api/templates/categories.csv` | Download template |

## Troubleshooting

### "No machines available after PREP"
- Check network connectivity to shared folders
- Verify username/password for `net use` authentication
- Check folder permissions (shared folder must be readable/writable)
- Review PREP_FAIL logs in job detail

### "Remote VBS did not write _DONE_ to log"
- Excel may not be installed on remote machine
- Macro has an error (review VBS error output in logs)
- Network latency; increase timeout in settings
- `wmic` may be disabled on remote (enable via Group Policy)

### "Could not merge {file}: ..."
- Excel file is corrupted or incompatible format
- System will skip that file and continue
- Check logs for specific error message

### Output files not collected
- Check shared folder accessibility from controller
- Verify macro actually creates output files (not just updating input)
- Review logs for "NO_NEW_OUTPUT" messages

### Emails not sending
- Click **Test Email** in Settings to verify SMTP
- Check SMTP credentials (Gmail needs app passwords, not account password)
- Verify recipients list is not empty
- Review email_log table for failures

## Installing Requirements

```bash
pip install -r requirements.txt
```

Key packages:
- `fastapi` - Web framework
- `uvicorn` - ASGI server
- `openpyxl` - Excel file manipulation (compilation)
- `jinja2` - HTML templating
- `python-multipart` - File upload support

## License

Internal use only.
