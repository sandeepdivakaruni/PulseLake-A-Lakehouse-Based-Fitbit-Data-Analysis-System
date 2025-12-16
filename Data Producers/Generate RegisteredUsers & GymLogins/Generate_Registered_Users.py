import random
import pyodbc
import pandas as pd
from datetime import datetime, timezone

# ---------- 1. Connection string ----------
# change these to your actual values
SERVER = "sbitserver15.database.windows.net"  # or whatever your server is
DATABASE = "sbit databse"                    # exact DB name (no typo!)
USERNAME = "sandeep"                          # your SQL login
PASSWORD = "Miryalguda@15"              # your SQL password

conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    f"UID={USERNAME};"
    f"PWD={PASSWORD};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
)


# ---------- 2. Helper: random MAC + timestamp in 2023 ----------
def random_mac():
    return ":".join(f"{random.randint(0, 255):02x}" for _ in range(6))


def random_registration_timestamp_2023():
    start = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    end   = datetime(2023, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
    delta_seconds = int((end - start).total_seconds())
    offset = random.randint(0, delta_seconds)
    return int(start.timestamp()) + offset  # unix seconds


# ---------- 3. Read existing IDs/MACs from Azure SQL ----------
def load_existing_values():
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT user_id, device_id, mac_address FROM dbo.registered_users;"
        )

        used_user_ids = set()
        used_device_ids = set()
        used_macs = set()

        for user_id, device_id, mac in cursor.fetchall():
            used_user_ids.add(int(user_id))
            used_device_ids.add(int(device_id))
            used_macs.add(mac.lower())

    return used_user_ids, used_device_ids, used_macs


# ---------- 4. Generate NEW rows avoiding everything in DB ----------
def generate_registered_users(
    n_rows,
    used_user_ids,
    used_device_ids,
    used_macs,
    user_id_min=11000,
    user_id_max=20000,
    device_id_min=100000,
    device_id_max=199999,
):
    rows = []

    while len(rows) < n_rows:
        user_id = random.randint(user_id_min, user_id_max)
        if user_id in used_user_ids:
            continue  # already used before

        device_id = random.randint(device_id_min, device_id_max)
        if device_id in used_device_ids:
            continue  # already used

        mac = random_mac().lower()
        if mac in used_macs:
            continue  # already used

        ts = random_registration_timestamp_2023()

        rows.append(
            {
                "user_id": user_id,
                "device_id": device_id,
                "mac_address": mac,
                "registration_timestamp": ts,
            }
        )

        # mark as used so we don't repeat within the same run
        used_user_ids.add(user_id)
        used_device_ids.add(device_id)
        used_macs.add(mac)

    return pd.DataFrame(rows)


# ---------- 5. Insert into Azure SQL ----------
def insert_into_azure_sql(df: pd.DataFrame):
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        for _, row in df.iterrows():
            cursor.execute(
                """
                INSERT INTO dbo.registered_users
                    (user_id, device_id, mac_address, registration_timestamp)
                VALUES (?, ?, ?, ?)
                """,
                int(row["user_id"]),
                int(row["device_id"]),
                row["mac_address"],
                int(row["registration_timestamp"]),
            )
        conn.commit()


# ---------- 6. Main ----------
if __name__ == "__main__":
    # how many NEW users you want this run
    N = 50

    used_user_ids, used_device_ids, used_macs = load_existing_values()
    df_new = generate_registered_users(N, used_user_ids, used_device_ids, used_macs)

    print(df_new.head())
    insert_into_azure_sql(df_new)
    print(f"Inserted {len(df_new)} new rows into dbo.registered_users.")
