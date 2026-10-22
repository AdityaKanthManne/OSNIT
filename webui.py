#!/usr/bin/env python3
"""Local web UI for user-scanner.

Run from the repo root:   python3 webui.py        (then open http://127.0.0.1:8080)
Options:                  python3 webui.py --port 9000
Binds to 127.0.0.1 only, so it is not exposed to your network.
"""
import argparse
import asyncio
import inspect
import json
import threading
import time
import uuid
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse

from user_scanner.core.helpers import (
    load_categories, load_modules, get_scan_func, get_site_name,
    is_loud, is_valid_email,
)
from user_scanner.core.result import Result, Status

CONCURRENCY = 60
MODULE_TIMEOUT = 25.0
JOBS = {}
JOBS_LOCK = threading.Lock()


def result_to_dict(r, site, category, is_email):
    reason = r.reason
    if isinstance(reason, Exception):
        reason = str(reason)
    return {
        "site": site,
        "category": category,
        "status": r.status.to_label(is_email=is_email),
        "url": r.url or "",
        "extra": {k: v for k, v in (r.extra or {}).items()},
        "media": dict(r.media or {}),
        "reason": reason or "",
    }


async def _run_job(job):
    is_email = job["type"] == "email"
    cats = load_categories(is_email=is_email, no_nsfw=not job["nsfw"])
    if job["category"] != "all":
        cats = {k: v for k, v in cats.items() if k.lower() == job["category"].lower()}

    work = []
    for cat_name, path in cats.items():
        for m in load_modules(path):
            work.append((cat_name, m))
    job["total"] = len(work)

    sem = asyncio.Semaphore(CONCURRENCY)
    loop = asyncio.get_running_loop()

    async def one(cat_name, module):
        site = get_site_name(module)
        async with sem:
            if is_loud(site, is_email=is_email) and not job["loud"]:
                r = Result.skipped()
                r.reason = "Loud module (may notify the account owner) - skipped"
            else:
                func = get_scan_func(module)
                try:
                    if func is None:
                        r = Result.error("no validate_ function")
                    elif inspect.iscoroutinefunction(func):
                        r = await asyncio.wait_for(func(job["target"]), MODULE_TIMEOUT)
                    else:
                        r = await asyncio.wait_for(
                            loop.run_in_executor(None, func, job["target"]), MODULE_TIMEOUT)
                except asyncio.TimeoutError:
                    r = Result.error("timed out")
                except Exception as e:
                    r = Result.error(e)
        d = result_to_dict(r, site, cat_name, is_email)
        with JOBS_LOCK:
            job["results"].append(d)
            job["done"] += 1

    await asyncio.gather(*(one(c, m) for c, m in work))


def start_job(target, scan_type, category, nsfw, loud):
    job = {
        "id": uuid.uuid4().hex[:12], "target": target, "type": scan_type,
        "category": category, "nsfw": nsfw, "loud": loud,
        "total": 0, "done": 0, "results": [], "finished": False,
        "error": None, "started": time.time(),
    }
    JOBS[job["id"]] = job

    def runner():
        try:
            asyncio.run(_run_job(job))
        except Exception as e:
            job["error"] = str(e)
        job["finished"] = True
        job["elapsed"] = round(time.time() - job["started"], 1)

    threading.Thread(target=runner, daemon=True).start()
    return job["id"]


class Handler(BaseHTTPRequestHandler):
    def _json(self, obj, code=200):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        p = urlparse(self.path).path
        if p == "/":
            body = PAGE.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif p == "/api/categories":
            out = {}
            for t, is_email in (("username", False), ("email", True)):
                out[t] = {name: len(load_modules(path))
                          for name, path in load_categories(is_email=is_email).items()}
            self._json(out)
        elif p.startswith("/api/job/"):
            job = JOBS.get(p.rsplit("/", 1)[-1])
            if not job:
                return self._json({"error": "unknown job"}, 404)
            with JOBS_LOCK:
                snap = {k: v for k, v in job.items() if k != "results"}
                snap["results"] = list(job["results"])
            self._json(snap)
        else:
            self._json({"error": "not found"}, 404)

    def do_POST(self):
        if urlparse(self.path).path != "/api/scan":
            return self._json({"error": "not found"}, 404)
        try:
            data = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
        except Exception:
            return self._json({"error": "bad JSON"}, 400)
        target = str(data.get("target", "")).strip()
        scan_type = "email" if data.get("type") == "email" else "username"
        if not target or len(target) > 254:
            return self._json({"error": "Enter a username or email."}, 400)
        if scan_type == "email" and not is_valid_email(target):
            return self._json({"error": "That doesn't look like a valid email."}, 400)
        jid = start_job(target, scan_type, str(data.get("category", "all")),
                        bool(data.get("nsfw")), bool(data.get("loud")))
        self._json({"id": jid})

    def log_message(self, *a):
        pass


PAGE = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>User Scanner</title>
<style>
:root{--bg:#f6f7f9;--card:#fff;--ink:#16181d;--muted:#667085;--line:#e4e7ec;--acc:#3b5bdb;--ok:#2b8a3e;--err:#c92a2a;--skip:#868e96}
@media (prefers-color-scheme:dark){:root{--bg:#111318;--card:#1a1d24;--ink:#e9ecef;--muted:#98a2b3;--line:#2c313c;--acc:#748ffc;--ok:#51cf66;--err:#ff8787;--skip:#adb5bd}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:15px/1.45 system-ui,-apple-system,Segoe UI,sans-serif}
main{max-width:1000px;margin:0 auto;padding:24px 16px}
h1{font-size:22px;margin:0 0 4px}.sub{color:var(--muted);margin:0 0 20px}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:16px;margin-bottom:16px}
.row{display:flex;gap:10px;flex-wrap:wrap;align-items:end}
label{font-size:12px;color:var(--muted);display:block;margin-bottom:4px}
input[type=text],select{padding:9px 10px;border:1px solid var(--line);border-radius:7px;background:var(--bg);color:var(--ink);font:inherit}
#target{flex:1;min-width:220px;width:100%}
button{padding:9px 18px;border:0;border-radius:7px;background:var(--acc);color:#fff;font:inherit;font-weight:600;cursor:pointer}
button:disabled{opacity:.5;cursor:default}button.ghost{background:transparent;color:var(--acc);border:1px solid var(--line)}
.opts{display:flex;gap:16px;margin-top:10px;font-size:13px;color:var(--muted);flex-wrap:wrap}
.bar{height:6px;background:var(--line);border-radius:3px;overflow:hidden;margin:10px 0}.bar>div{height:100%;background:var(--acc);width:0;transition:width .3s}
.stats{display:flex;gap:18px;font-size:13px;color:var(--muted);flex-wrap:wrap}.stats b{color:var(--ink)}
table{width:100%;border-collapse:collapse;font-size:14px}th,td{text-align:left;padding:7px 6px;border-bottom:1px solid var(--line);vertical-align:top}
th{font-size:12px;color:var(--muted);font-weight:600}td.s{white-space:nowrap;font-weight:600}
.Found,.Registered{color:var(--ok)}.Error{color:var(--err)}.Skipped{color:var(--skip)}
.x{color:var(--muted);font-size:12px;word-break:break-word}a{color:var(--acc);word-break:break-all}
.tools{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:8px}.msg{color:var(--err);margin-top:8px}
</style></head><body><main>
<h1>User Scanner</h1>
<p class="sub">Checks a username or email against the scanner's site modules. Runs locally from your computer.</p>
<div class="card">
 <div class="row">
  <div style="flex:1;min-width:220px"><label for="target">Username or email</label><input id="target" type="text" placeholder="e.g. johndoe or john@example.com" autocomplete="off"></div>
  <div><label for="type">Type</label><select id="type"><option value="username">Username</option><option value="email">Email</option></select></div>
  <div><label for="cat">Category</label><select id="cat"><option value="all">All</option></select></div>
  <button id="go">Scan</button>
 </div>
 <div class="opts"><label style="margin:0"><input type="checkbox" id="nsfw"> include adult sites</label>
 <label style="margin:0"><input type="checkbox" id="loud"> include "loud" modules (may notify the account owner)</label></div>
 <div class="msg" id="msg"></div>
</div>
<div class="card" id="out" hidden>
 <div class="stats" id="stats"></div><div class="bar"><div id="prog"></div></div>
 <div class="tools"><select id="filter"><option value="hit">Found / registered only</option><option value="all">All results</option><option value="Error">Errors</option></select>
 <button class="ghost" id="dlj">Download JSON</button><button class="ghost" id="dlc">Download CSV</button></div>
 <table><thead><tr><th>Site</th><th>Category</th><th>Status</th><th>Details</th></tr></thead><tbody id="rows"></tbody></table>
</div>
</main><script>
const $=id=>document.getElementById(id);let cats={},job=null,timer=null;
const esc=s=>String(s??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const safeUrl=u=>/^https?:\/\//i.test(u||"")?u:"";
function fillCats(){const t=$("type").value,s=$("cat");s.innerHTML='<option value="all">All</option>';
 for(const [k,n] of Object.entries(cats[t]||{}).sort())s.innerHTML+=`<option value="${esc(k)}">${esc(k)} (${n})</option>`;}
fetch("/api/categories").then(r=>r.json()).then(d=>{cats=d;fillCats();});
$("type").onchange=fillCats;$("target").onkeydown=e=>{if(e.key==="Enter")go();};$("go").onclick=go;$("filter").onchange=render;
async function go(){$("msg").textContent="";const target=$("target").value.trim();if(!target)return;
 if(target.includes("@")&&$("type").value==="username"){$("type").value="email";fillCats();}
 $("go").disabled=true;
 const r=await fetch("/api/scan",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({target,type:$("type").value,category:$("cat").value,nsfw:$("nsfw").checked,loud:$("loud").checked})});
 const d=await r.json();if(!r.ok){$("msg").textContent=d.error;$("go").disabled=false;return;}
 $("out").hidden=false;clearInterval(timer);timer=setInterval(()=>poll(d.id),1000);poll(d.id);}
async function poll(id){job=await (await fetch("/api/job/"+id)).json();render();
 if(job.finished){clearInterval(timer);$("go").disabled=false;}}
function render(){if(!job)return;const R=job.results,c=s=>R.filter(x=>x.status===s).length;
 const hits=c("Found")+c("Registered");
 $("stats").innerHTML=`<span>Target <b>${esc(job.target)}</b></span><span>Checked <b>${job.done}/${job.total}</b></span><span>Found <b>${hits}</b></span><span>Errors <b>${c("Error")}</b></span><span>Skipped <b>${c("Skipped")}</b></span>`+(job.finished?`<span>Done in <b>${job.elapsed}s</b></span>`:"<span>Scanning…</span>");
 $("prog").style.width=(job.total?100*job.done/job.total:0)+"%";
 const f=$("filter").value;let rows=R.filter(x=>f==="all"||(f==="hit"?(x.status==="Found"||x.status==="Registered"):x.status===f));
 rows.sort((a,b)=>a.category.localeCompare(b.category)||a.site.localeCompare(b.site));
 $("rows").innerHTML=rows.map(x=>{const u=safeUrl(x.url);const ex=Object.entries(x.extra||{}).map(([k,v])=>`${esc(k)}: ${esc(v)}`).join(" · ");
  return `<tr><td>${esc(x.site)}</td><td>${esc(x.category)}</td><td class="s ${esc(x.status)}">${esc(x.status)}</td><td>${u?`<a href="${esc(u)}" target="_blank" rel="noopener noreferrer">${esc(u)}</a>`:""}${ex?`<div class="x">${ex}</div>`:""}${x.reason&&x.status!=="Found"&&x.status!=="Registered"?`<div class="x">${esc(x.reason)}</div>`:""}</td></tr>`}).join("")||`<tr><td colspan=4 class="x">${job.finished?"Nothing to show for this filter.":"Waiting for results…"}</td></tr>`;}
function dl(name,text,type){const a=document.createElement("a");a.href=URL.createObjectURL(new Blob([text],{type}));a.download=name;a.click();}
$("dlj").onclick=()=>job&&dl(`scan_${job.target}.json`,JSON.stringify(job,null,2),"application/json");
$("dlc").onclick=()=>{if(!job)return;const q=v=>'"'+String(v??"").replace(/"/g,'""').replace(/^([=+\-@])/,"'$1")+'"';
 const lines=[["site","category","status","url","extra","reason"].join(",")].concat(job.results.map(x=>[x.site,x.category,x.status,x.url,JSON.stringify(x.extra),x.reason].map(q).join(",")));
 dl(`scan_${job.target}.csv`,lines.join("\n"),"text/csv");};
</script></body></html>"""


def main():
    ap = argparse.ArgumentParser(description="Local web UI for user-scanner")
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--host", default="127.0.0.1")
    a = ap.parse_args()
    srv = ThreadingHTTPServer((a.host, a.port), Handler)
    print(f"User Scanner web UI running at http://{a.host}:{a.port}  (Ctrl+C to stop)")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
