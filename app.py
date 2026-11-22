import csv
import sqlite3
from datetime import datetime, timezone
from flask import Flask, jsonify, send_from_directory, request
from tinydb import TinyDB, Query

app = Flask(__name__)

SQL_DB_PATH = "sql_db.sqlite"
NOSQL_DB_PATH = "nosql_db.json"
CSV_PATH = "sales_data_dictionary.csv"

nosql_db = TinyDB(NOSQL_DB_PATH)
Doc = Query()
replication_log = []


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def get_sql_conn():
    conn = sqlite3.connect(SQL_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_and_seed_dbs():
    conn = get_sql_conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sales_dictionary (
            Column_Name TEXT PRIMARY KEY,
            Description TEXT,
            Data_Type TEXT,
            Example_Value TEXT,
            Business_Meaning TEXT,
            version INTEGER DEFAULT 1,
            updated_at TEXT
        )
    """)
    conn.commit()

    sql_count = conn.execute("SELECT COUNT(*) FROM sales_dictionary").fetchone()[0]
    nosql_count = len(nosql_db)

    if sql_count == 0 and nosql_count == 0:
        with open(CSV_PATH, mode="r", encoding="utf-8-sig") as f:
            rows = list(csv.DictReader(f))

        third = len(rows) // 3
        sql_only = rows[:third]
        nosql_only = rows[third : 2 * third]
        both = rows[2 * third :]
        ts = now_iso()

        def insert_sql(record):
            conn.execute(
                """
                INSERT OR REPLACE INTO sales_dictionary
                (Column_Name, Description, Data_Type, Example_Value, Business_Meaning, version, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record["Column_Name"],
                    record["Description"],
                    record["Data_Type"],
                    record["Example_Value"],
                    record["Business_Meaning"],
                    1,
                    ts,
                ),
            )

        def insert_nosql(record):
            doc = dict(record)
            doc["version"] = 1
            doc["updated_at"] = ts
            nosql_db.upsert(doc, Doc.Column_Name == record["Column_Name"])

        for r in sql_only:
            insert_sql(r)
        for r in nosql_only:
            insert_nosql(r)
        for r in both:
            insert_sql(r)
            insert_nosql(r)

        conn.commit()
    conn.close()


def fetch_sql(query_str=""):
    conn = get_sql_conn()
    if query_str:
        like = f"%{query_str}%"
        rows = conn.execute(
            """
            SELECT * FROM sales_dictionary
            WHERE Column_Name LIKE ? OR Description LIKE ?
               OR Data_Type LIKE ? OR Example_Value LIKE ?
               OR Business_Meaning LIKE ?
            """,
            (like, like, like, like, like),
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM sales_dictionary").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def fetch_nosql(query_str=""):
    q = query_str.lower()
    docs = nosql_db.all()
    if not q:
        return docs
    return [
        d for d in docs
        if any(
            q in str(d.get(field, "")).lower()
            for field in ("Column_Name", "Description", "Data_Type", "Example_Value", "Business_Meaning")
        )
    ]


@app.route("/")
def index():
    return send_from_directory("templates", "index.html")


@app.route("/api/data", methods=["GET"])
def get_data():
    source = request.args.get("source", "all").lower()
    q = request.args.get("q", "").strip()

    sql_rows = {r["Column_Name"]: r for r in fetch_sql(q)}
    nosql_rows = {r["Column_Name"]: r for r in fetch_nosql(q)}

    results = []
    if source == "sql":
        for key, row in sql_rows.items():
            row["source"] = "Both" if key in nosql_rows else "SQL"
            results.append(row)
    elif source == "nosql":
        for key, row in nosql_rows.items():
            row["source"] = "Both" if key in sql_rows else "NoSQL"
            results.append(row)
    else:
        all_keys = sorted(set(sql_rows.keys()) | set(nosql_rows.keys()))
        for key in all_keys:
            if key in sql_rows and key in nosql_rows:
                # Prefer the higher version if any drift occurred
                row = (
                    sql_rows[key]
                    if sql_rows[key]["version"] >= nosql_rows[key]["version"]
                    else nosql_rows[key]
                )
                row["source"] = "Both"
            elif key in sql_rows:
                row = sql_rows[key]
                row["source"] = "SQL"
            else:
                row = nosql_rows[key]
                row["source"] = "NoSQL"
            results.append(row)

    return jsonify({"records": results, "replication_log": replication_log[-10:][::-1]})


@app.route("/api/data/<column_name>", methods=["PUT"])
def update_data(column_name):
    payload = request.json
    replicate_to_other = payload.get("replicate", False)

    conn = get_sql_conn()
    sql_existing = conn.execute(
        "SELECT * FROM sales_dictionary WHERE Column_Name = ?", (column_name,)
    ).fetchone()
    nosql_existing = nosql_db.get(Doc.Column_Name == column_name)

    if not sql_existing and not nosql_existing:
        conn.close()
        return jsonify({"error": "Record not found"}), 404

    current_version = max(
        sql_existing["version"] if sql_existing else 0,
        nosql_existing["version"] if nosql_existing else 0,
    )
    new_version = current_version + 1
    ts = now_iso()

    updated_record = {
        "Column_Name": column_name,
        "Description": payload.get("Description", ""),
        "Data_Type": payload.get("Data_Type", ""),
        "Example_Value": payload.get("Example_Value", ""),
        "Business_Meaning": payload.get("Business_Meaning", ""),
        "version": new_version,
        "updated_at": ts,
    }

    # Determine target databases (Synchronous Replication Protocol)
    write_sql = bool(sql_existing) or replicate_to_other
    write_nosql = bool(nosql_existing) or replicate_to_other

    if write_sql:
        conn.execute(
            """
            INSERT OR REPLACE INTO sales_dictionary
            (Column_Name, Description, Data_Type, Example_Value, Business_Meaning, version, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                updated_record["Column_Name"],
                updated_record["Description"],
                updated_record["Data_Type"],
                updated_record["Example_Value"],
                updated_record["Business_Meaning"],
                new_version,
                ts,
            ),
        )
        conn.commit()

    if write_nosql:
        nosql_db.upsert(updated_record, Doc.Column_Name == column_name)

    conn.close()

    # Log replication event
    if write_sql and write_nosql:
        action = (
            "Replicated to both DBs (promoted)"
            if replicate_to_other and not (sql_existing and nosql_existing)
            else "Synchronous replication across SQL & NoSQL"
        )
        replication_log.append(
            f"[{ts}] {column_name} (v{new_version}): {action}"
        )

    return jsonify({"status": "updated", "record": updated_record})


@app.route("/api/replicate", methods=["POST"])
def manual_sync():
    """Last-Write-Wins (LWW) consistency check and replication across shared records."""
    conn = get_sql_conn()
    sql_rows = {r["Column_Name"]: dict(r) for r in conn.execute("SELECT * FROM sales_dictionary").fetchall()}
    nosql_rows = {r["Column_Name"]: r for r in nosql_db.all()}

    synced = 0
    ts = now_iso()

    for key in set(sql_rows.keys()) & set(nosql_rows.keys()):
        s_row, n_row = sql_rows[key], nosql_rows[key]
        if s_row["version"] > n_row["version"]:
            nosql_db.upsert(s_row, Doc.Column_Name == key)
            synced += 1
        elif n_row["version"] > s_row["version"]:
            conn.execute(
                """
                UPDATE sales_dictionary
                SET Description=?, Data_Type=?, Example_Value=?, Business_Meaning=?, version=?, updated_at=?
                WHERE Column_Name=?
                """,
                (
                    n_row["Description"],
                    n_row["Data_Type"],
                    n_row["Example_Value"],
                    n_row["Business_Meaning"],
                    n_row["version"],
                    n_row["updated_at"],
                    key,
                ),
            )
            synced += 1

    conn.commit()
    conn.close()
    replication_log.append(f"[{ts}] Manual LWW Sync completed ({synced} conflicts resolved)")
    return jsonify({"synced": synced})


if __name__ == "__main__":
    init_and_seed_dbs()
    app.run(debug=True, port=5000)