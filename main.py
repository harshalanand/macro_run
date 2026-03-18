"""
FastAPI -- Macro Orchestrator with queue-based execution, CSV templates, test mail.
"""
import os, io, csv, json, shutil
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, Request, UploadFile, File, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

import database as D
import executor as E
import notifier as N

BASE = os.path.dirname(os.path.abspath(__file__))
UPLOAD = os.path.join(BASE, "uploads")
os.makedirs(UPLOAD, exist_ok=True)

app = FastAPI(title="Macro Orchestrator")
app.mount("/static", StaticFiles(directory=os.path.join(BASE, "static")), name="static")
tpl = Jinja2Templates(directory=os.path.join(BASE, "templates"))
D.init_db()

import sys
if "--reload" in sys.argv or any("--reload" in str(a) for a in sys.argv):
    print("\n" + "="*60)
    print("  WARNING: --reload WILL CRASH running jobs!")
    print("  Every DB write triggers a server restart.")
    print("  Use: uvicorn main:app --host 0.0.0.0 --port 8000")
    print("  Or:  run.bat")
    print("="*60 + "\n")

# ── PAGES ───────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def dashboard(r: Request):
    return tpl.TemplateResponse("app.html", {"request": r, "page": "dash", "s": D.get_dashboard()})

@app.get("/machines", response_class=HTMLResponse)
async def machines_page(r: Request, tag: str = "", imported: str = "", errors: str = ""):
    machines = D.get_master_machines(tag or None)
    tags = D.get_master_tags()
    groups = D.get_groups()
    import_msg = ""
    if imported:
        import_msg = f"✅ Imported {imported} machines" + (f" ({errors} errors)" if errors and errors != "0" else "")
    return tpl.TemplateResponse("app.html", {
        "request": r, "page": "machines",
        "machines": machines, "tags": tags,
        "groups": groups, "active_tag": tag,
        "import_msg": import_msg
    })

@app.post("/api/master-machines/import")
async def import_master_machines(file: UploadFile = File(...)):
    import io, csv as _csv
    content = (await file.read()).decode("utf-8-sig")
    reader = _csv.DictReader(io.StringIO(content))
    added = 0
    errors = []
    for i, row in enumerate(reader, 1):
        name = (row.get("machine_name") or row.get("name") or "").strip()
        if not name:
            errors.append(f"Row {i}: missing machine_name")
            continue
        try:
            D.create_master_machine(
                name=name,
                system_name=(row.get("system_name") or row.get("hostname") or "").strip(),
                ip=(row.get("ip_address") or row.get("ip") or "").strip(),
                shared_folder=(row.get("shared_folder") or row.get("folder") or "").strip(),
                remote_path=(row.get("remote_path") or row.get("local_path") or "").strip(),
                username=(row.get("username") or row.get("user") or "").strip(),
                password=(row.get("password") or "").strip(),
                department=(row.get("department") or row.get("dept") or "").strip(),
                location=(row.get("location") or "").strip(),
                tags=(row.get("tags") or "").strip(),
            )
            added += 1
        except Exception as e:
            errors.append(f"Row {i} ({name}): {e}")
    return RedirectResponse(f"/machines?imported={added}&errors={len(errors)}", 303)

@app.get("/api/templates/master-machines.csv")
async def tpl_master_machines():
    import io, csv as _csv
    buf = io.StringIO()
    w = _csv.writer(buf)
    w.writerow(["machine_name","system_name","ip_address","shared_folder","remote_path","username","password","department","location","tags"])
    w.writerow(["PC-SALES-01","PCSAL01","192.168.1.101",r"\\PCSAL01\Share\Macro",r"D:\Share\Macro","administrator","pass123","Sales","Floor 2","sales,floor2"])
    w.writerow(["PC-FIN-01","PCFIN01","192.168.1.110",r"\\PCFIN01\Reports\Macro",r"E:\Reports\Macro","administrator","pass123","Finance","Floor 3","finance,floor3"])
    buf.seek(0)
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=master_machines_template.csv"})

@app.post("/api/master-machines")
async def create_master_machine(
    machine_name: str = Form(...), system_name: str = Form(""),
    ip_address: str = Form(""), shared_folder: str = Form(""),
    remote_path: str = Form(""), username: str = Form(""),
    password: str = Form(""), department: str = Form(""),
    location: str = Form(""), tags: str = Form("")):
    D.create_master_machine(machine_name, system_name, ip_address, shared_folder,
                            remote_path, username, password, department, location, tags)
    return RedirectResponse("/machines", 303)

@app.get("/api/master-machines/{mid}")
async def get_master_machine_json(mid: int):
    m = D.get_master_machine(mid)
    if not m: raise HTTPException(404)
    return JSONResponse({k: m[k] for k in m.keys()})

@app.post("/api/master-machines/{mid}/edit")
async def edit_master_machine(mid: int,
    machine_name: str = Form(...), system_name: str = Form(""),
    ip_address: str = Form(""), shared_folder: str = Form(""),
    remote_path: str = Form(""), username: str = Form(""),
    password: str = Form(""), department: str = Form(""),
    location: str = Form(""), tags: str = Form("")):
    kw = dict(machine_name=machine_name, system_name=system_name,
              ip_address=ip_address, shared_folder=shared_folder,
              remote_path=remote_path, department=department,
              location=location, tags=tags)
    if password.strip():
        kw["password"] = password
    D.update_master_machine(mid, **kw)
    return RedirectResponse("/machines", 303)

@app.post("/api/master-machines/{mid}/delete")
async def delete_master_machine(mid: int):
    D.delete_master_machine(mid)
    return RedirectResponse("/machines", 303)

@app.post("/api/master-machines/{mid}/toggle")
async def toggle_master_machine(mid: int):
    m = D.get_master_machine(mid)
    if m:
        D.update_master_machine(mid, is_active=0 if m["is_active"] else 1)
    return RedirectResponse("/machines", 303)

@app.post("/api/master-machines/{mid}/health")
async def check_master_health(mid: int):
    m = D.get_master_machine(mid)
    if not m: raise HTTPException(404)
    results = E.test_machine_dict(dict(m))
    ok = all(r[1] for r in results)
    detail = " | ".join(f"{'OK' if r[1] else 'FAIL'} {r[0]}: {r[2]}" for r in results)
    status = "OK" if ok else ("PARTIAL" if any(r[1] for r in results) else "FAIL")
    D.update_master_health(mid, status, detail)
    return JSONResponse({"status": status, "results": [{"step": r[0], "ok": r[1], "msg": r[2]} for r in results]})

@app.post("/api/master-machines/health-all")
async def check_all_health():
    machines = D.get_master_machines()
    results = {}
    for m in machines:
        res = E.test_machine_dict(dict(m))
        ok = all(r[1] for r in res)
        detail = " | ".join(f"{'OK' if r[1] else 'FAIL'} {r[0]}: {r[2]}" for r in res)
        status = "OK" if ok else ("PARTIAL" if any(r[1] for r in res) else "FAIL")
        D.update_master_health(m["master_id"], status, detail)
        results[m["master_id"]] = {"status": status, "name": m["machine_name"]}
    return JSONResponse(results)

@app.post("/api/groups/{gid}/add-from-master")
async def add_from_master(gid: int, master_ids: str = Form(...)):
    added = 0
    for mid_str in master_ids.split(","):
        mid_str = mid_str.strip()
        if mid_str.isdigit():
            D.add_master_to_group(gid, int(mid_str))
            added += 1
    return RedirectResponse(f"/groups/{gid}", 303)


async def groups_page(r: Request):
    return tpl.TemplateResponse("app.html", {"request": r, "page": "groups", "groups": D.get_groups()})

@app.get("/groups", response_class=HTMLResponse)
async def groups_page(r: Request):
    return tpl.TemplateResponse("app.html", {"request": r, "page": "groups", "groups": D.get_groups()})

@app.get("/groups/{gid}", response_class=HTMLResponse)
async def group_detail(r: Request, gid: int):
    g = D.get_group(gid)
    if not g: raise HTTPException(404)
    return tpl.TemplateResponse("app.html", {"request": r, "page": "gdetail",
        "g": g, "machines": D.get_machines(gid), "files": D.get_files(gid),
        "cats": D.get_categories(gid), "jobs": D.get_jobs(20, gid),
        "master_machines_all": D.get_master_machines()})

@app.get("/jobs", response_class=HTMLResponse)
async def jobs_page(r: Request):
    return tpl.TemplateResponse("app.html", {"request": r, "page": "jobs", "jobs": D.get_jobs(50)})

@app.get("/jobs/{jid}", response_class=HTMLResponse)
async def job_detail(r: Request, jid: int):
    j = D.get_job(jid)
    if not j: raise HTTPException(404)
    return tpl.TemplateResponse("app.html", {"request": r, "page": "jdetail",
        "j": j, "queue": D.get_queue(jid), "logs": D.get_logs(jid, 500)})

@app.get("/logs", response_class=HTMLResponse)
async def logs_page(r: Request):
    return tpl.TemplateResponse("app.html", {"request": r, "page": "logs", "logs": D.get_logs(limit=500)})

@app.post("/api/logs/clear")
async def clear_logs():
    D.clear_logs()
    return RedirectResponse("/logs", 303)

@app.post("/api/logs/clear/{jid}")
async def clear_job_logs(jid: int):
    D.clear_logs(jid)
    return RedirectResponse(f"/jobs/{jid}", 303)

@app.get("/emails", response_class=HTMLResponse)
async def emails_page(r: Request):
    return tpl.TemplateResponse("app.html", {"request": r, "page": "emails", "emails": D.get_email_logs()})

@app.post("/api/emails/clear")
async def clear_emails():
    D.clear_email_logs()
    return RedirectResponse("/emails", 303)

@app.get("/settings", response_class=HTMLResponse)
async def settings_page(r: Request):
    return tpl.TemplateResponse("app.html", {"request": r, "page": "settings", "cfg": D.get_all_settings()})

# ── GROUPS ──────────────────────────────────────────────────────────────────

@app.post("/api/groups")
async def create_group(name: str = Form(...), description: str = Form("")):
    gid = D.create_group(name, description)
    return RedirectResponse(f"/groups/{gid}", 303)

@app.post("/api/groups/{gid}/update")
async def update_group(gid: int, group_name: str=Form(""), description: str=Form(""),
                        excel_file_name: str=Form(""), macro_name: str=Form(""), target_cell: str=Form("A1")):
    D.update_group(gid, group_name=group_name, description=description,
                   excel_file_name=excel_file_name, macro_name=macro_name, target_cell=target_cell)
    return RedirectResponse(f"/groups/{gid}", 303)

@app.post("/api/groups/{gid}/delete")
async def delete_group(gid: int):
    D.delete_group(gid)
    return RedirectResponse("/groups", 303)

# ── MACHINES ────────────────────────────────────────────────────────────────

@app.post("/api/groups/{gid}/machines")
async def add_machine(gid: int, machine_name:str=Form(...), system_name:str=Form(""),
    ip_address:str=Form(""), shared_folder:str=Form(...), remote_path:str=Form(""),
    username:str=Form(""), password:str=Form(""), department:str=Form(""), location:str=Form("")):
    D.add_machine(gid, machine_name, system_name, ip_address, shared_folder, remote_path, username, password, department, location)
    return RedirectResponse(f"/groups/{gid}", 303)

@app.post("/api/machines/{mid}/delete")
async def del_machine(mid: int):
    m = D.get_machine(mid)
    D.delete_machine(mid)
    return RedirectResponse(f"/groups/{m['group_id']}" if m else "/groups", 303)

@app.post("/api/machines/{mid}/toggle")
async def toggle_machine(mid: int):
    m = D.get_machine(mid)
    D.toggle_machine(mid)
    return RedirectResponse(f"/groups/{m['group_id']}" if m else "/groups", 303)

@app.post("/api/machines/{mid}/edit")
async def edit_machine(mid: int, machine_name:str=Form(...), system_name:str=Form(""),
    ip_address:str=Form(""), shared_folder:str=Form(...), remote_path:str=Form(""),
    username:str=Form(""), password:str=Form(""), department:str=Form(""), location:str=Form("")):
    m = D.get_machine(mid)
    D.update_machine(mid, machine_name=machine_name, system_name=system_name,
        ip_address=ip_address, shared_folder=shared_folder, remote_path=remote_path,
        username=username, password=password, department=department, location=location)
    return RedirectResponse(f"/groups/{m['group_id']}" if m else "/groups", 303)

@app.post("/api/groups/{gid}/import-machines")
async def import_machines(gid: int, file: UploadFile = File(...)):
    content = (await file.read()).decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(content))
    rows = []
    for row in reader:
        rows.append({
            "name": (row.get("name") or row.get("machine_name","")).strip(),
            "system_name": (row.get("system_name") or row.get("hostname","")).strip(),
            "ip": (row.get("ip") or row.get("ip_address","")).strip(),
            "shared_folder": (row.get("shared_folder") or row.get("folder","")).strip(),
            "remote_path": (row.get("remote_path") or row.get("local_path","")).strip(),
            "username": (row.get("username") or row.get("user","")).strip(),
            "password": (row.get("password") or "").strip(),
            "department": (row.get("department") or row.get("dept","")).strip(),
            "location": (row.get("location") or "").strip(),
        })
    rows = [r for r in rows if r["name"] and r["shared_folder"]]
    added = D.bulk_import_machines(gid, rows)
    return RedirectResponse(f"/groups/{gid}", 303)

# ── FILES ───────────────────────────────────────────────────────────────────

@app.post("/api/groups/{gid}/upload")
async def upload_files(gid: int, files: list[UploadFile] = File(...)):
    gdir = os.path.join(UPLOAD, str(gid))
    os.makedirs(gdir, exist_ok=True)
    for f in files:
        path = os.path.join(gdir, f.filename)
        with open(path, "wb") as out:
            shutil.copyfileobj(f.file, out)
        D.add_file(gid, f.filename, path, os.path.getsize(path)/1024)
    return RedirectResponse(f"/groups/{gid}", 303)

@app.post("/api/files/{fid}/delete")
async def del_file(fid: int):
    with D.db() as c:
        r = c.execute("SELECT group_id FROM group_files WHERE file_id=?", (fid,)).fetchone()
    D.delete_file(fid)
    return RedirectResponse(f"/groups/{r['group_id']}" if r else "/groups", 303)

@app.post("/api/files/{fid}/set-macro")
async def set_macro(fid: int):
    D.set_macro_file(fid)
    with D.db() as c:
        r = c.execute("SELECT group_id FROM group_files WHERE file_id=?", (fid,)).fetchone()
    return RedirectResponse(f"/groups/{r['group_id']}" if r else "/groups", 303)

# ── CATEGORIES ──────────────────────────────────────────────────────────────

@app.post("/api/groups/{gid}/categories")
async def add_cat(gid: int, cat_value: str = Form(...)):
    D.add_category(gid, cat_value.strip())
    return RedirectResponse(f"/groups/{gid}", 303)

@app.post("/api/categories/{cid}/delete")
async def del_cat(cid: int):
    with D.db() as c:
        r = c.execute("SELECT group_id FROM categories WHERE cat_id=?", (cid,)).fetchone()
    D.delete_category(cid)
    return RedirectResponse(f"/groups/{r['group_id']}" if r else "/groups", 303)

@app.post("/api/groups/{gid}/clear-categories")
async def clear_cats(gid: int):
    D.delete_all_categories(gid)
    return RedirectResponse(f"/groups/{gid}", 303)

@app.get("/api/machines/{mid}")
async def get_machine_json(mid: int):
    m = D.get_machine(mid)
    if not m: raise HTTPException(404)
    return JSONResponse({k: m[k] for k in m.keys()})

@app.post("/api/machines/{mid}/test")
async def test_machine_route(mid: int):
    results = E.test_machine(mid)
    return JSONResponse({"results": [{"step": s, "ok": o, "msg": m} for s, o, m in results]})

@app.post("/api/groups/{gid}/import-categories")
async def import_cats(gid: int, file: UploadFile = File(...)):
    content = (await file.read()).decode("utf-8-sig")
    lines = []
    if "," in content.split("\n")[0]:
        reader = csv.reader(io.StringIO(content))
        header = next(reader, None)
        for row in reader:
            if row:
                lines.append(row[0].strip())
    else:
        lines = [l.strip() for l in content.strip().splitlines()]
    D.bulk_import_categories(gid, lines)
    return RedirectResponse(f"/groups/{gid}", 303)

# ── JOBS ────────────────────────────────────────────────────────────────────

@app.post("/api/groups/{gid}/run")
async def run_job(gid: int):
    g = D.get_group(gid)
    if not g: raise HTTPException(404)
    files = D.get_files(gid)
    machines = [m for m in D.get_machines(gid) if m["is_active"]]
    cats = D.get_categories(gid)
    if not files: raise HTTPException(400, "No files uploaded")
    if not machines: raise HTTPException(400, "No active machines")
    if not cats: raise HTTPException(400, "No categories defined")
    if not g["macro_name"]: raise HTTPException(400, "Macro name not configured in group settings")
    if not g["excel_file_name"]: raise HTTPException(400, "Excel file not set in group settings")

    jid = D.create_job(gid)
    E.run_async(jid)
    return RedirectResponse(f"/jobs/{jid}", 303)

@app.get("/api/jobs/{jid}/status")
async def job_status(jid: int):
    j = D.get_job(jid)
    if not j: raise HTTPException(404)
    q = D.get_queue(jid)
    return JSONResponse({"job_id": jid, "status": j["status"],
        "completed": j["completed_cats"], "failed": j["failed_cats"], "total": j["total_cats"],
        "running": E.is_running(jid),
        "queue": [{"id":i["queue_id"],"cat":i["cat_value"],"machine":i["machine_name"]or"",
                    "status":i["status"],"duration":i["duration_secs"],"error":i["error_message"]} for i in q]})

@app.post("/api/jobs/{jid}/kill")
async def kill_job(jid: int):
    if E.is_running(jid):
        E.kill_job(jid)
    else:
        D.kill_job_db(jid)
    return RedirectResponse(f"/jobs/{jid}", 303)

@app.post("/api/jobs/{jid}/delete")
async def delete_job(jid: int):
    if E.is_running(jid):
        raise HTTPException(400, "Cannot delete a running job. Kill it first.")
    D.delete_job(jid)
    return RedirectResponse("/jobs", 303)

# ── SETTINGS ────────────────────────────────────────────────────────────────

@app.post("/api/settings")
async def save_settings(smtp_host:str=Form(""), smtp_port:str=Form("587"),
    smtp_username:str=Form(""), smtp_password:str=Form(""), smtp_from:str=Form(""),
    notify_emails:str=Form(""), email_enabled:str=Form("0"), compile_path:str=Form(""),
    exec_method:str=Form("auto"), psexec_path:str=Form("psexec"),
    macro_timeout:str=Form("600"), excel_visible:str=Form("1")):
    for k,v in {"smtp_host":smtp_host,"smtp_port":smtp_port,"smtp_username":smtp_username,
        "smtp_from":smtp_from,"notify_emails":notify_emails,"email_enabled":email_enabled,
        "compile_path":compile_path,"exec_method":exec_method,"psexec_path":psexec_path,
        "macro_timeout":macro_timeout,"excel_visible":excel_visible}.items():
        D.set_setting(k, v)
    if smtp_password.strip():
        D.set_setting("smtp_password", smtp_password)
    return RedirectResponse("/settings", 303)

@app.post("/api/settings/test-email")
async def test_email():
    ok, msg = N.send_test()
    return JSONResponse({"success": ok, "message": msg})

# ── CSV TEMPLATES ───────────────────────────────────────────────────────────

@app.get("/api/templates/machines.csv")
async def tpl_machines():
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["name","system_name","ip","shared_folder","remote_path","username","password","department","location"])
    w.writerow(["PC-SALES-01","PCSAL01","192.168.1.101","\\\\PCSAL01\\Share\\Macro","D:\\Share\\Macro","admin","pass123","Sales","Floor 2"])
    w.writerow(["PC-FIN-01","PCFIN01","192.168.1.110","\\\\PCFIN01\\Reports\\Macro","E:\\Reports\\Macro","admin","pass123","Finance","Floor 3"])
    buf.seek(0)
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=machines_template.csv"})

@app.get("/api/templates/categories.csv")
async def tpl_cats():
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["cat_value"])
    for v in ["CAT001","CAT002","CAT003","MAJ-100","MAJ-200"]:
        w.writerow([v])
    buf.seek(0)
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=categories_template.csv"})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
