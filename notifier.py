"""
Email notifications -- SMTP alerts with test support.
"""
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import database as D

def _cfg():
    return {
        "host": D.get_setting("smtp_host","smtp.gmail.com"),
        "port": int(D.get_setting("smtp_port","587")),
        "user": D.get_setting("smtp_username",""),
        "pwd": D.get_setting("smtp_password",""),
        "from": D.get_setting("smtp_from",""),
        "to": D.get_setting("notify_emails",""),
        "on": D.get_setting("email_enabled","0")=="1",
    }

def _send(subject, html, jid=None, machine=""):
    cfg = _cfg()
    if not cfg["on"] or not cfg["to"]:
        return False, "Email disabled or no recipients"
    to = [e.strip() for e in cfg["to"].split(",") if e.strip()]
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = cfg["from"] or cfg["user"]
    msg["To"] = ", ".join(to)
    msg.attach(MIMEText(html, "html"))
    try:
        ctx = ssl.create_default_context()
        with smtplib.SMTP(cfg["host"], cfg["port"], timeout=15) as s:
            s.ehlo(); s.starttls(context=ctx); s.ehlo()
            if cfg["user"] and cfg["pwd"]:
                s.login(cfg["user"], cfg["pwd"])
            s.sendmail(msg["From"], to, msg.as_string())
        D.log_email(jid, machine, subject, ",".join(to), "SENT")
        return True, "OK"
    except Exception as e:
        D.log_email(jid, machine, subject, ",".join(to), "FAILED", str(e))
        return False, str(e)

def send_test():
    """Send a test email. Returns (success, message)."""
    cfg = _cfg()
    if not cfg["to"]:
        return False, "No recipient emails configured"
    html = f"""
    <div style="font-family:Segoe UI,sans-serif;padding:24px;max-width:500px;margin:auto;border:2px solid #6c5ce7;border-radius:10px">
        <h2 style="color:#6c5ce7;margin:0 0 12px">Test Email - Macro Orchestrator</h2>
        <p>This is a test email from the Macro Orchestrator.</p>
        <table style="margin:16px 0;font-size:14px">
            <tr><td style="color:#888;padding:4px 12px 4px 0">SMTP Host:</td><td>{cfg['host']}:{cfg['port']}</td></tr>
            <tr><td style="color:#888;padding:4px 12px 4px 0">From:</td><td>{cfg['from'] or cfg['user']}</td></tr>
            <tr><td style="color:#888;padding:4px 12px 4px 0">Time:</td><td>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</td></tr>
        </table>
        <p style="color:#27ae60;font-weight:600">Email configuration is working correctly.</p>
    </div>"""
    # Temporarily force enable
    orig = cfg["on"]
    if not orig:
        D.set_setting("email_enabled", "1")
    ok, msg = _send("[MACRO TEST] Email Configuration Test", html, machine="TEST")
    if not orig:
        D.set_setting("email_enabled", "0")
    return ok, msg

def notify_copy_failure(jid, qid, machine, ip, folder, error, step="FILE_COPY"):
    _send(f"[MACRO ALERT] {step} Failed - {machine}", f"""
    <div style="font-family:Segoe UI,sans-serif;max-width:600px;margin:auto;border:1px solid #e53e3e;border-radius:8px;overflow:hidden">
        <div style="background:#e53e3e;color:white;padding:14px 20px"><h3 style="margin:0">{step} Failure - {machine}</h3></div>
        <div style="padding:20px">
            <p><b>Machine:</b> {machine} ({ip})</p>
            <p><b>Folder:</b> <code>{folder}</code></p>
            <p><b>Time:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <div style="margin-top:12px;padding:10px;background:#fff5f5;border-left:3px solid #e53e3e;font-size:13px">
                <b>Error:</b> <code>{error[:300]}</code>
            </div>
        </div>
    </div>""", jid, machine)

def notify_macro_failure(jid, qid, machine, ip, excel, macro, error, cat_value=""):
    _send(f"[MACRO ALERT] Macro Failed - {machine} [{cat_value}]", f"""
    <div style="font-family:Segoe UI,sans-serif;max-width:600px;margin:auto;border:1px solid #e67e22;border-radius:8px;overflow:hidden">
        <div style="background:#e67e22;color:white;padding:14px 20px"><h3 style="margin:0">Macro Failure - {machine}</h3></div>
        <div style="padding:20px">
            <p><b>Machine:</b> {machine} ({ip})</p>
            <p><b>Category:</b> <span style="background:#ffeaa7;padding:2px 8px;border-radius:4px;font-weight:600">{cat_value}</span></p>
            <p><b>File:</b> {excel} / <b>Macro:</b> {macro}</p>
            <div style="margin-top:12px;padding:10px;background:#fef9e7;border-left:3px solid #e67e22;font-size:13px">
                <b>Error:</b> <code>{error[:300]}</code>
            </div>
        </div>
    </div>""", jid, machine)

def notify_job_summary(jid, group, total, ok, fail, failures):
    rows = "".join(f"<tr><td style='padding:5px 8px;border-bottom:1px solid #eee'>{f['machine']}</td>"
                   f"<td style='padding:5px 8px;border-bottom:1px solid #eee'>{f['step']}</td>"
                   f"<td style='padding:5px 8px;border-bottom:1px solid #eee;font-size:12px'>{f['error'][:100]}</td></tr>"
                   for f in failures)
    _send(f"[MACRO] Job #{jid} Done - {fail}/{total} Failed - {group}", f"""
    <div style="font-family:Segoe UI,sans-serif;max-width:650px;margin:auto;border:1px solid #aaa;border-radius:8px;overflow:hidden">
        <div style="background:#2d3748;color:white;padding:14px 20px"><h3 style="margin:0">Job Summary - {group}</h3></div>
        <div style="padding:20px">
            <div style="display:flex;gap:16px;margin-bottom:16px">
                <div style="flex:1;text-align:center;padding:10px;background:#f0fff4;border-radius:6px"><div style="font-size:24px;font-weight:700;color:#38a169">{ok}</div><div style="font-size:12px;color:#666">OK</div></div>
                <div style="flex:1;text-align:center;padding:10px;background:#fff5f5;border-radius:6px"><div style="font-size:24px;font-weight:700;color:#e53e3e">{fail}</div><div style="font-size:12px;color:#666">Failed</div></div>
                <div style="flex:1;text-align:center;padding:10px;background:#ebf8ff;border-radius:6px"><div style="font-size:24px;font-weight:700;color:#3182ce">{total}</div><div style="font-size:12px;color:#666">Total</div></div>
            </div>
            <table style="width:100%;border-collapse:collapse;font-size:13px">
                <tr style="background:#f7fafc"><th style="padding:6px 8px;text-align:left">Machine</th><th style="padding:6px 8px;text-align:left">Category</th><th style="padding:6px 8px;text-align:left">Error</th></tr>
                {rows}
            </table>
        </div>
    </div>""", jid)
