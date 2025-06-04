import pymysql
from app.config import MYSQL_CON

class MySQLHandler:
    def __init__(self):
        self.conn = pymysql.connect(**MYSQL_CON)

    def insert_or_update(self, data, main_table, history_table, pk):
        cursor = self.conn.cursor()

        quoted_pk = f"`{pk}`"
        quoted_main_table = f"`{main_table}`"
        quoted_history_table = f"`{history_table}`"

        for record in data:
            # Fetch all rows with matching pk
            cursor.execute(
                f"SELECT * FROM {quoted_main_table} WHERE {quoted_pk} = %s",
                (record[pk],)
            )
            existing_rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            if existing_rows:
                # Get max version from history table
                cursor.execute(
                    f"SELECT MAX(version) FROM {quoted_history_table} WHERE {quoted_pk} = %s",
                    (record[pk],)
                )
                max_ver_result = cursor.fetchone()
                version_to_insert = int(max_ver_result[0]) + 1 if max_ver_result[0] is not None else 1

                quoted_columns = ', '.join(f"`{col}`" for col in columns)
                history_cols = f"{quoted_columns}, version"
                placeholders = ', '.join(['%s'] * len(columns)) + ', %s'

                for existing in existing_rows:
                    existing_dict = dict(zip(columns, existing))
                    history_vals = list(existing_dict.values()) + [version_to_insert]
                    cursor.execute(
                        f"INSERT INTO {quoted_history_table} ({history_cols}) VALUES ({placeholders})",
                        history_vals
                    )

                update_cols = ', '.join([f"`{key}` = %s" for key in record.keys()])
                update_vals = list(record.values()) + [record[pk]]
                cursor.execute(
                    f"UPDATE {quoted_main_table} SET {update_cols} WHERE {quoted_pk} = %s",
                    update_vals
                )
            else:
                insert_cols = ', '.join(f"`{col}`" for col in record.keys())
                insert_placeholders = ', '.join(['%s'] * len(record))
                cursor.execute(
                    f"INSERT INTO {quoted_main_table} ({insert_cols}) VALUES ({insert_placeholders})",
                    tuple(record.values())
                )

        self.conn.commit()
        cursor.close()
