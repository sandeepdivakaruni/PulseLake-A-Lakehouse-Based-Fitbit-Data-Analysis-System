import random
import pyodbc
import pandas as pd
from datetime import datetime, timezone

# ------------ CONFIG ---------------

SERVER   = "sbitserver15.database.windows.net"   # <== your server
DATABASE = "sbit databse"                       # exact name as in portal
USERNAME = "sandeep"                             # same login as before
PASSWORD = "Miryalguda@15"                  # <-- update

# how many new login records you want to generate per run
N_ROWS = 50

# gyms in your system
GYM_IDS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

# UNIX time range (inclusive) for 2023-01-01 to 2023-12-31
START_TS = int(datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp())
END_TS   = int(datetime(2023, 12, 31, 23, 59, 59, tzinfo=timezone.utc).timestamp())

# ODBC connection string
CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    f"UID={USERNAME};"
    f"PWD={PASSWORD};"
    "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
)


# ------------ HELPERS ---------------

def get_mac_addresses_from_registered_users():
    """
    Read all MAC addresses from dbo.registered_users,
    so gym_logins always use existing devices.
    """
    with pyodbc.connect(CONN_STR) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT mac_address FROM dbo.registered_users;")
        rows = cursor.fetchall()
    macs = [r[0] for r in rows]
    if not macs:
        raise RuntimeError("No MAC addresses found in dbo.registered_users.")
    return macs


def get_existing_keys():
    """
    Get existing (mac_address, login) combinations from dbo.gym_logins
    to avoid generating duplicates across runs.
    """
    existing = set()
    with pyodbc.connect(CONN_STR) as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT mac_address, login FROM dbo.gym_logins;")
            for mac, login in cursor.fetchall():
                existing.add((mac, int(login)))
        except pyodbc.ProgrammingError:
            # table might be empty first time
            pass
    return existing


def generate_gym_logins(macs, existing_keys, n_rows):
    """
    Generate a DataFrame of random gym logins, skipping duplicates of
    (mac_address, login) both within this batch and vs DB.
    """
    rows = []
    local_keys = set(existing_keys)

    while len(rows) < n_rows:
        mac = random.choice(macs)
        gym = random.choice(GYM_IDS)

        # choose login timestamp within 2023 range
        login_ts = random.randint(START_TS, END_TS - 60 * 30)  # leave room for logout

        # ensure uniqueness on (mac, login)
        if (mac, login_ts) in local_keys:
            continue

        duration_minutes = random.randint(30, 180)
        logout_ts = login_ts + duration_minutes * 60

        rows.append({
            "mac_address": mac,
            "gym": gym,
            "login": int(login_ts),
            "logout": int(logout_ts),
        })
        local_keys.add((mac, login_ts))

    return pd.DataFrame(rows)


def insert_into_azure_sql(df: pd.DataFrame):
    """
    Insert generated rows into dbo.gym_logins.
    Primary key (mac_address, login) prevents duplicates.
    """
    with pyodbc.connect(CONN_STR) as conn:
        cursor = conn.cursor()

        insert_sql = """
        INSERT INTO dbo.gym_login (mac_address, gym, login, logout)
        VALUES (?, ?, ?, ?);
        """

        for _, row in df.iterrows():
            try:
                cursor.execute(
                    insert_sql,
                    row["mac_address"],
                    int(row["gym"]),
                    int(row["login"]),
                    int(row["logout"]),
                )
            except pyodbc.IntegrityError as e:
                # If a duplicate sneaks in, just skip it
                if "2627" in str(e):  # PK violation
                    continue
                else:
                    raise
        conn.commit()


# ------------ MAIN ---------------

if __name__ == "__main__":
    # 1) pull MACs from registered_users
    macs = get_mac_addresses_from_registered_users()

    # 2) get existing keys in gym_logins
    existing_keys = get_existing_keys()

    # 3) generate new logins
    df = generate_gym_logins(macs, existing_keys, N_ROWS)
    print(df.head())

    # 4) insert into Azure SQL
    insert_into_azure_sql(df)

    print(f"Inserted {len(df)} gym_login rows (skipping any duplicates).")
