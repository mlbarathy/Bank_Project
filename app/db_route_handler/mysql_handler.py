# db/mysql_handler.py

import mysql.connector
import os

from app.config import MYSQL_CON


class MySQLHandler:
    def __init__(self):
        self.conn = mysql.connector.connect(**MYSQL_CON)

    def insert_or_update(self, data, main_table, history_table, pk):
        cursor = self.conn.cursor()

        for record in data:
            cursor.execute(f"SELECT * FROM {main_table} WHERE {pk} = %s", (record[pk],))
            existing = cursor.fetchone()

            if existing:
                columns = [desc[0] for desc in cursor.description]
                existing_dict = dict(zip(columns, existing))

                cursor.execute(f"SELECT MAX(version) FROM {history_table} WHERE {pk} = %s", (record[pk],))
                max_ver_result = cursor.fetchone()[0]
                version_to_insert = int(max_ver_result) + 1 if max_ver_result is not None else 1

                history_cols = ', '.join(columns) + ', version'
                placeholders = ', '.join(['%s'] * len(columns)) + ', %s'
                history_vals = list(existing_dict.values()) + [version_to_insert]
                cursor.execute(f"INSERT INTO {history_table} ({history_cols}) VALUES ({placeholders})", history_vals)

                update_cols = ', '.join([f"{key} = %s" for key in record.keys()])
                update_vals = list(record.values()) + [record[pk]]
                cursor.execute(f"UPDATE {main_table} SET {update_cols} WHERE {pk} = %s", update_vals)
            else:
                insert_cols = ', '.join(record.keys())
                insert_placeholders = ', '.join(['%s'] * len(record))
                cursor.execute(f"INSERT INTO {main_table} ({insert_cols}) VALUES ({insert_placeholders})", tuple(record.values()))

        self.conn.commit()
        cursor.close()
