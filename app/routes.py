from flask import Blueprint, request, jsonify
import psycopg2
from psycopg2 import sql
from app.config import  *

bp = Blueprint("routes", __name__)

def get_existing_columns():
    try:
        conn = psycopg2.connect(**POSTGRESS_CON)
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {POSTGRES_TABLE} LIMIT 0;")
        columns = [desc[0] for desc in cursor.description]
        cursor.close()
        conn.close()
        return columns
    except Exception as e:
        print(f"⚠️ Error Fetching Columns: {e}")
        return None

@bp.route("/bank1-json-insert", methods=["POST"])
def insert():
    try:
        payload = request.get_json()
        if not payload or "data" not in payload:
            return jsonify({"error": "Invalid Request, 'Data' Key Missing"}), 400

        records = payload["data"]
        if not isinstance(records, list):
            return jsonify({"error": "'Data' Must Be a List of Records"}), 400

        valid_columns = get_existing_columns()
        if not valid_columns:
            return jsonify({"error": "Unable To Fetch Table Columns"}), 500

        conn = psycopg2.connect(**POSTGRESS_CON)
        cursor = conn.cursor()
        inserted_count = 0

        for i, record in enumerate(records):
            if not isinstance(record, dict):
                return jsonify({"error": f"Record {i + 1} Is Not a Valid JSON Object"}), 400

            record_keys = set(record.keys())
            table_keys = set(valid_columns)
            missing = table_keys - record_keys
            extra = record_keys - table_keys

            if missing:
                return jsonify({
                    "error": f"Record {i + 1} Missing Columns: {sorted(missing)}",
                    "valid_columns": sorted(valid_columns)
                }), 400

            if extra:
                return jsonify({
                    "error": f"Record {i + 1} Contains Invalid Columns: {sorted(extra)}",
                    "valid_columns": sorted(valid_columns)
                }), 400

            keys = list(record.keys())
            values = [record[k] for k in keys]

            insert_query = sql.SQL("INSERT Overwrite Table {} ({}) VALUES ({});").format(
                sql.Identifier(POSTGRES_TABLE),
                sql.SQL(', ').join(map(sql.Identifier, keys)),
                sql.SQL(', ').join(sql.Placeholder() * len(keys))
            )
            cursor.execute(insert_query, values)
            inserted_count += 1
        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({
            "message": "Records Inserted Successfully",
            "records_inserted": inserted_count
        }), 200

    except Exception as e:
        print(f"❌ Error in /insert: {e}")
        return jsonify({"error": "Internal server error"}), 500
