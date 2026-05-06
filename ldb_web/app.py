"""
LDB Average Balance Calculator — Web Version
Flask server ທີ່ run ເທິງຄອມຂອງເຈົ້າ
"""
from flask import Flask, render_template, request, jsonify, session, redirect, url_for, send_file
from functools import wraps
import polars as pl
import glob, os, re, time, json, io
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count
import pandas as pd

app = Flask(__name__)
app.secret_key = "ldb-secret-key-change-me-2024"

# ═══════════════════════════════════════
# ຕັ້ງຄ່າ — ແກ້ໄດ້ຕາມຕ້ອງການ
# ═══════════════════════════════════════
USERS = {
    "admin": "ldb2024",
    "user1": "user1234",
}

DATA_FOLDERS = {
    "2024": "D:/2024",
    "2025": "D:/2025",
    "2026": "D:/2026",
}

CASA_SAVINGS_CODES = [
    "2203111","2203311","2202110","2201100","2201400",
    "2201800","2201210","2201220","2201280","2204111","2201300","2203211",
]

# ═══════════════════════════════════════
# Login
# ═══════════════════════════════════════
def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if "user" not in session:
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated

@app.route("/login", methods=["GET","POST"])
def login():
    if request.method == "POST":
        u = request.form.get("username","")
        p = request.form.get("password","")
        if u in USERS and USERS[u] == p:
            session["user"] = u
            return redirect(url_for("index"))
        return render_template("index.html", page="login", error="Username ຫຼື Password ບໍ່ຖືກ")
    return render_template("index.html", page="login", error=None)

@app.route("/logout")
def logout():
    session.pop("user", None)
    return redirect(url_for("login"))

# ═══════════════════════════════════════
# ໜ້າຫຼັກ
# ═══════════════════════════════════════
@app.route("/")
@login_required
def index():
    folders = {k: os.path.exists(v) for k,v in DATA_FOLDERS.items()}
    return render_template("index.html", page="main", user=session["user"], folders=folders)

# ═══════════════════════════════════════
# Scan Engine
# ═══════════════════════════════════════
def scan_single_file(filepath, contracts, start_date, end_date):
    try:
        lf = pl.scan_csv(filepath, separator="|", ignore_errors=True, infer_schema_length=0)
        schema = lf.collect_schema().names()
        col_bal = "Local CCY Balance" if "Local CCY Balance" in schema else "Closing Balance"
        col_cbal = "Contract Balance" if "Contract Balance" in schema else None
        col_ccy = "Currency" if "Currency" in schema else None

        select_cols = [
            pl.col("Contract"), pl.col("Processing Date"),
            pl.col("BOL Line").str.strip_chars().alias("BOL Line"),
            pl.col(col_bal).alias("Balance"),
        ]
        if col_ccy: select_cols.append(pl.col(col_ccy).str.strip_chars().alias("Currency"))
        if col_cbal: select_cols.append(pl.col(col_cbal).alias("Contract Balance"))

        result = lf.filter(
            pl.col("Contract").is_in(contracts) &
            (pl.col("Processing Date") >= start_date) &
            (pl.col("Processing Date") <= end_date) &
            (pl.col("BOL Line").str.strip_chars().is_in(CASA_SAVINGS_CODES))
        ).select(select_cols).collect()
        if len(result) > 0:
            return result.to_dicts()
    except: pass
    return []

def smart_filter_files(all_files, start_date, end_date):
    pat = re.compile(r'(\d{8})')
    out = []
    for f in all_files:
        m = pat.search(os.path.basename(f))
        if m:
            if start_date <= m.group(1) <= end_date: out.append(f)
        else: out.append(f)
    return out

# ═══════════════════════════════════════
# API: ຄິດໄລ່
# ═══════════════════════════════════════
@app.route("/api/calculate", methods=["POST"])
@login_required
def calculate():
    try:
        data = request.json
        contracts = [c.strip() for c in data.get("contracts","").split("\n") if c.strip()]
        years = data.get("years", [])
        start_date = data.get("start_date","").strip()
        end_date = data.get("end_date","").strip()

        if not contracts: return jsonify({"error":"ກະລຸນາປ້ອນເລກບັນຊີ"}), 400
        if not years: return jsonify({"error":"ກະລຸນາເລືອກປີ"}), 400
        if not re.match(r'^\d{8}$', start_date) or not re.match(r'^\d{8}$', end_date):
            return jsonify({"error":"ວັນທີບໍ່ຖືກ (YYYYMMDD)"}), 400

        # ລວບລວມໄຟລ໌
        all_files = []
        for y in years:
            folder = DATA_FOLDERS.get(y,"")
            if folder and os.path.exists(folder):
                all_files.extend(glob.glob(os.path.join(folder, "*.txt")))

        filtered = smart_filter_files(all_files, start_date, end_date)
        if not filtered:
            return jsonify({"error":"ບໍ່ພົບໄຟລ໌ຂໍ້ມູນ"}), 404

        # Parallel scan
        all_rows = []
        mw = min(cpu_count(), 8)
        t0 = time.time()

        with ProcessPoolExecutor(max_workers=mw) as exe:
            futures = {exe.submit(scan_single_file, f, contracts, start_date, end_date): f for f in filtered}
            for fut in as_completed(futures):
                try:
                    r = fut.result()
                    if r: all_rows.extend(r)
                except: pass

        elapsed = time.time() - t0

        if not all_rows:
            return jsonify({"error":"ບໍ່ພົບຂໍ້ມູນ"}), 404

        raw = pl.DataFrame(all_rows)
        clean = raw.with_columns(
            pl.col("Balance").str.replace_all(",","").str.strip_chars()
            .cast(pl.Float64, strict=False).alias("Balance")
        )
        # Clean Contract Balance ຖ້າມີ
        if "Contract Balance" in clean.columns:
            clean = clean.with_columns(
                pl.col("Contract Balance").str.replace_all(",","").str.strip_chars()
                .cast(pl.Float64, strict=False).alias("Contract Balance")
            )

        dedup = clean.unique(subset=["Contract","Processing Date"], keep="last")
        # drop BOL Line ອອກ (ໃຊ້ແຕ່ keep row order ໃຫ້ deterministic)
        if "BOL Line" in dedup.columns:
            dedup = dedup.drop("BOL Line")

        # Daily data ສຳລັບ chart — ເພີ່ມ Currency + Contract Balance
        daily_cols = ["Contract","Processing Date","Balance"]
        if "Currency" in dedup.columns: daily_cols.append("Currency")
        if "Contract Balance" in dedup.columns: daily_cols.append("Contract Balance")
        daily = dedup.sort("Processing Date").select(daily_cols).to_dicts()

        # Average per contract — ເພີ່ມ Currency + avg Contract Balance
        agg_cols = [
            pl.col("Balance").mean().round(2).alias("avg"),
            pl.col("Balance").min().round(2).alias("min"),
            pl.col("Balance").max().round(2).alias("max"),
            pl.col("Balance").count().alias("days"),
        ]
        if "Currency" in dedup.columns:
            agg_cols.append(pl.col("Currency").first().alias("Currency"))
        if "Contract Balance" in dedup.columns:
            agg_cols.append(pl.col("Contract Balance").mean().round(2).alias("avg_contract_bal"))

        avg_df = dedup.group_by("Contract").agg(agg_cols).sort("Contract")
        summary = avg_df.to_dicts()

        # Grand totals
        grand_avg = dedup["Balance"].mean()
        grand_total = dedup["Balance"].sum()

        return jsonify({
            "summary": summary,
            "daily": daily,
            "stats": {
                "total_accounts": len(summary),
                "total_days": len(dedup),
                "total_raw": len(raw),
                "grand_avg": round(grand_avg, 2) if grand_avg else 0,
                "grand_total": round(grand_total, 2) if grand_total else 0,
                "elapsed": round(elapsed, 1),
                "files_scanned": len(filtered),
            }
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ═══════════════════════════════════════
# ເກັບ result ໄວ້ server-side ສຳລັບ export
# ═══════════════════════════════════════
last_results = {}

@app.route("/api/save_for_export", methods=["POST"])
@login_required
def save_for_export():
    """Browser ສົ່ງ data ມາເກັບໄວ້ server ກ່ອນ export."""
    try:
        data = request.json
        user = session.get("user", "unknown")
        last_results[user] = data
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/export/<kind>/<fmt>")
@login_required
def export_file(kind, fmt):
    """Export ດ້ວຍ GET — browser ເປີດ link ໂດຍກົງ."""
    try:
        user = session.get("user", "unknown")
        data = last_results.get(user)
        if not data:
            return "No data. Please calculate first.", 400

        if kind == "summary":
            rows = data.get("summary", [])
            if not rows: return "No summary data", 400
            pdf = pd.DataFrame(rows)
            rename_map = {"avg":"Average Balance","min":"Min Balance","max":"Max Balance","days":"Days Count"}
            pdf = pdf.rename(columns={k:v for k,v in rename_map.items() if k in pdf.columns})
            fname_prefix = "LDB_AvgBalance"
        elif kind == "daily":
            rows = data.get("daily", [])
            if not rows: return "No daily data", 400
            pdf = pd.DataFrame(rows)
            if "Processing Date" in pdf.columns:
                pdf = pdf.rename(columns={"Processing Date": "Date"})
            sort_cols = [c for c in ["Contract","Date"] if c in pdf.columns]
            if sort_cols: pdf = pdf.sort_values(sort_cols)
            fname_prefix = "LDB_Daily"
        else:
            return "Invalid kind", 400

        buf = io.BytesIO()
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        if fmt == "xlsx":
            pdf.to_excel(buf, index=False, engine="openpyxl")
            buf.seek(0)
            return send_file(buf,
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                as_attachment=True, download_name=f"{fname_prefix}_{ts}.xlsx")
        else:
            pdf.to_csv(buf, index=False, encoding="utf-8-sig")
            buf.seek(0)
            return send_file(buf,
                mimetype="text/csv",
                as_attachment=True, download_name=f"{fname_prefix}_{ts}.csv")

    except Exception as e:
        return f"Error: {str(e)}", 500

if __name__ == "__main__":
    print("=" * 50)
    print("  LDB Avg Balance Calculator — Web Version")
    print("  ເປີດ browser → http://localhost:5000")
    print("=" * 50)
    app.run(host="0.0.0.0", port=5000, debug=True)
