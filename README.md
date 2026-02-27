# Macro Orchestrator v3 — Queue-Based Execution

FastAPI web app to distribute Excel macro runs across multiple Windows machines with category queuing, email alerts, and full audit logging.

## Key Concept

**Groups** → each has its own machines, files, and category list
**Categories** → list of Maj/Cat numbers to process (e.g., 25 items)
**Machines** → pool of Windows PCs (e.g., 10 machines)
**Queue** → 25 categories run across 10 machines. When a machine finishes one category, it automatically picks the next from the queue.

### Execution Flow

```
For each category in the queue:
  1. Pick a free machine from the pool
  2. Open the Excel macro file on that machine
  3. Paste the category value into the target cell (e.g., A1)
  4. Run the macro
  5. Collect output files back to compiled folder
  6. Machine becomes free → picks next category
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

### 5. Add Machines
Add manually or **Import CSV**. Download the template for correct format.
CSV columns: `name, system_name, ip, shared_folder, username, password, department, location`

**System access**: If the shared folder is a UNC path (\\server\share), the system will:
1. Try direct access first
2. If access denied and username/password provided → authenticate via `net use`
3. System name can be used: `\\HOSTNAME\share` instead of IP

### 6. Run Job
Click **Run Job**. The system:
- Copies all files to each machine's date folder (in parallel)
- Distributes categories across machines from the queue
- Each machine: open Excel → paste category → run macro → collect output
- When done, picks next queued category
- Emails sent on any failure
- All steps logged in DB

## Machine Authentication

For remote machines that need login:

| Field | Example | Purpose |
|-------|---------|---------|
| System Name | `PCSAL01` | Windows hostname for UNC path |
| Shared Folder | `\\PCSAL01\Share\Macro` | Network path (UNC or local) |
| Username | `domain\admin` | For `net use` authentication |
| Password | `pass123` | Stored in DB (not encrypted) |

The executor runs `net use \\server\share /user:username password` before accessing the folder.

## Email Alerts

Configure in **Settings**:
- SMTP host/port/username/password
- Recipient emails
- **Test Email** button to verify configuration

Alerts sent for:
- Machine prep failure (can't create folder, can't copy files)
- Macro execution failure (per category)
- Job summary (if any failures occurred)

## Settings

| Setting | Purpose |
|---------|---------|
| Compile Path | Where output files are collected (default: ./compiled_output) |
| SMTP Config | Email server settings |
| Email Recipients | Comma-separated list |
| Test Email | Verify SMTP works |

## Output Structure

```
compiled_output/
  2026-02-27/
    Monthly Reports/        ← group name
      PC-SALES-01/          ← machine name
        MAJ001/             ← category
          output_report.xlsx
        MAJ002/
          output_report.xlsx
      PC-SALES-02/
        MAJ003/
          output_report.xlsx
```

## Database Tables

| Table | Purpose |
|-------|---------|
| machine_groups | Groups with macro config (file, macro, cell) |
| machines | Per-group with system_name, credentials |
| group_files | Uploaded Excel files per group |
| categories | Maj/Cat list per group |
| jobs | Execution runs |
| job_queue | Category queue with machine assignment |
| run_logs | Detailed audit trail |
| email_log | All notifications sent |
| settings | SMTP config, compile path |

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
| POST | `/api/settings` | Save settings |
| POST | `/api/settings/test-email` | Send test |
| GET | `/api/templates/machines.csv` | Download template |
| GET | `/api/templates/categories.csv` | Download template |
