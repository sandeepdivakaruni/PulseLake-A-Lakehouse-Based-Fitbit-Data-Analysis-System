import logging
import json
import time
import random
from datetime import datetime

import pyodbc
from confluent_kafka import Producer, KafkaException
from faker import Faker

fake = Faker("en_US")


class UserInfoProducer:
    END_2023_TS = int(datetime(2023, 12, 31, 23, 59, 59).timestamp())

    def __init__(self):
        # ---------- KAFKA CONFIG ----------
        self.topic = "user_info"
        self.conf = {
            "bootstrap.servers": "pkc-921jm.us-east-2.aws.confluent.cloud:9092",
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": "WLBMXFTRTW73LQQN",
            "sasl.password": "cfltMji9WOLt7PvLBf8CmXszMwi4xpQ8TtkeXWiLk7zubxkHdnwoW4qx5Wn0eS1A",
            "client.id": "Sandeep-UserInfo-Producer",
        }

        # ---------- AZURE SQL CONFIG ----------
        self.sql_server = "sbitserver15.database.windows.net"
        self.sql_database = "sbit databse"
        self.sql_username = "sandeep"
        self.sql_password = "Miryalguda@15"

        # ---------- LOGGER ----------
        self.logger = logging.getLogger("KafkaUserInfoProducer")
        self.logger.setLevel(logging.INFO)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        # avoid adding multiple handlers if instantiated more than once
        if not self.logger.handlers:
            self.logger.addHandler(ch)

    # ============================================================
    # =======================   DB HELPERS   ======================
    # ============================================================

    def get_sql_connection(self):
        conn_str = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            f"SERVER={self.sql_server};"
            f"DATABASE={self.sql_database};"
            f"UID={self.sql_username};"
            f"PWD={self.sql_password};"
            "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
        )
        return pyodbc.connect(conn_str)

    def ensure_history_table(self):
        """
        Creates dbo.user_info_history if it doesn't exist.
        This stores the last event per user, so new runs know whether
        to emit 'new' or 'update' and which timestamp to start from.
        """
        sql = """
        IF NOT EXISTS (
            SELECT 1
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE t.name = 'user_info_history'
              AND s.name = 'dbo'
        )
        BEGIN
            CREATE TABLE dbo.user_info_history (
                user_id          BIGINT       NOT NULL PRIMARY KEY,
                last_update_type NVARCHAR(20) NULL,
                last_timestamp   BIGINT       NULL,
                dob              VARCHAR(10)  NULL,
                sex              NVARCHAR(10) NULL,
                gender           NVARCHAR(10) NULL,
                first_name       NVARCHAR(100) NULL,
                last_name        NVARCHAR(100) NULL,
                street_address   NVARCHAR(200) NULL,
                city             NVARCHAR(100) NULL,
                state            NVARCHAR(50)  NULL,
                zip              INT           NULL
            );
        END;
        """
        with self.get_sql_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql)
            conn.commit()
        self.logger.info("Ensured dbo.user_info_history exists.")

    def fetch_registered_users(self, limit=None):
        """
        Reads user_id + registration_timestamp from dbo.registered_users.
        """
        sql = """
        SELECT user_id, registration_timestamp
        FROM dbo.registered_users
        """
        if limit:
            sql += f" ORDER BY user_id OFFSET 0 ROWS FETCH NEXT {int(limit)} ROWS ONLY;"
        else:
            sql += " ORDER BY user_id;"

        with self.get_sql_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()

        users = []
        for row in rows:
            user_id = int(row.user_id)
            reg_ts = int(row.registration_timestamp)
            users.append(
                {
                    "user_id": user_id,
                    "registration_timestamp": reg_ts,
                }
            )
        return users

    def load_history(self):
        """
        Load existing user_info_history into a dict:
        { user_id: event_dict_like_your_JSON }
        """
        history = {}
        sql = """
        SELECT
            user_id,
            last_update_type,
            last_timestamp,
            dob,
            sex,
            gender,
            first_name,
            last_name,
            street_address,
            city,
            state,
            zip
        FROM dbo.user_info_history;
        """

        with self.get_sql_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()

            for row in rows:
                user_id = int(row.user_id)
                ts = int(row.last_timestamp) if row.last_timestamp is not None else None

                event = {
                    "user_id": user_id,
                    "update_type": row.last_update_type or "new",
                    "timestamp": float(ts) if ts is not None else None,
                    "dob": row.dob,
                    "sex": row.sex,
                    "gender": row.gender,
                    "first_name": row.first_name,
                    "last_name": row.last_name,
                    "address": {
                        "street_address": row.street_address,
                        "city": row.city,
                        "state": row.state,
                        "zip": int(row.zip) if row.zip is not None else None,
                    },
                }
                history[user_id] = event

        self.logger.info("Loaded %d users from user_info_history.", len(history))
        return history

    def upsert_history(self, event):
        """
        After sending any event for a user, update dbo.user_info_history
        with the latest data for that user.
        """
        user_id = int(event["user_id"])
        ts = int(event["timestamp"])
        update_type = event.get("update_type", "new")

        dob = event.get("dob")
        sex = event.get("sex")
        gender = event.get("gender")
        first_name = event.get("first_name")
        last_name = event.get("last_name")
        addr = event.get("address", {}) or {}

        street_address = addr.get("street_address")
        city = addr.get("city")
        state = addr.get("state")
        zip_code = addr.get("zip")

        sql = """
        MERGE dbo.user_info_history AS target
        USING (SELECT ? AS user_id) AS source
        ON target.user_id = source.user_id
        WHEN MATCHED THEN
            UPDATE SET
                last_update_type = ?,
                last_timestamp   = ?,
                dob              = ?,
                sex              = ?,
                gender           = ?,
                first_name       = ?,
                last_name        = ?,
                street_address   = ?,
                city             = ?,
                state            = ?,
                zip              = ?
        WHEN NOT MATCHED THEN
            INSERT (
                user_id,
                last_update_type,
                last_timestamp,
                dob,
                sex,
                gender,
                first_name,
                last_name,
                street_address,
                city,
                state,
                zip
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """

        params = [
            # USING (SELECT ? AS user_id)
            user_id,
            # UPDATE part
            update_type,
            ts,
            dob,
            sex,
            gender,
            first_name,
            last_name,
            street_address,
            city,
            state,
            zip_code,
            # INSERT part
            user_id,
            update_type,
            ts,
            dob,
            sex,
            gender,
            first_name,
            last_name,
            street_address,
            city,
            state,
            zip_code,
        ]

        with self.get_sql_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql, params)
            conn.commit()

    # ============================================================
    # =================   EVENT GENERATION HELPERS   =============
    # ============================================================

    def random_sex_gender(self):
        sex = random.choice(["M", "F"])
        return sex, sex  # keep sex == gender for now

    def random_address(self):
        """
        Generate random US address with arbitrary state.
        """
        full_addr = fake.address().split("\n")
        street_address = full_addr[0]

        # typically: "City, ST 12345"
        city_state_zip = full_addr[1]
        try:
            city_part, state_zip_part = city_state_zip.split(",")
            city = city_part.strip()
            parts = state_zip_part.strip().split(" ")
            state = parts[0]
            zip_code = parts[-1]
        except Exception:
            city = fake.city()
            state = fake.state_abbr()
            zip_code = fake.postcode()

        # make zip an int and 5 digits only
        zip_5 = zip_code[:5]
        zip_int = int(zip_5) if zip_5.isdigit() else 99999

        return {
            "street_address": street_address,
            "city": city,
            "state": state,
            "zip": zip_int,
        }

    def build_new_event(self, user_row):
        """
        Build a 'new' event from registered_users row.
        """
        user_id = user_row["user_id"]
        sex, gender = self.random_sex_gender()
        dob = fake.date_of_birth(minimum_age=18, maximum_age=85).strftime("%m/%d/%Y")
        first_name = fake.first_name_male() if sex == "M" else fake.first_name_female()
        last_name = fake.last_name()
        address = self.random_address()

        ts = int(user_row["registration_timestamp"])
        # clamp to end of 2023 just in case
        ts = min(ts, self.END_2023_TS)

        event = {
            "user_id": user_id,
            "update_type": "new",
            "timestamp": float(ts),
            "dob": dob,
            "sex": sex,
            "gender": gender,
            "first_name": first_name,
            "last_name": last_name,
            "address": address,
        }
        return event

    def build_update_event(self, prev_event):
        """
        Build an 'update' event based on the last event from history.
        We increase timestamp and change last_name + address.
        """
        updated = dict(prev_event)  # shallow copy

        prev_ts = int(prev_event["timestamp"])
        # add 1 to 30 days of seconds
        delta_seconds = random.randint(1, 30 * 24 * 60 * 60)
        new_ts = prev_ts + delta_seconds
        # keep within year 2023
        new_ts = min(new_ts, self.END_2023_TS)
        # ensure strictly greater (just in case prev_ts already at END_2023_TS)
        if new_ts <= prev_ts:
            new_ts = prev_ts + 1

        updated["update_type"] = "update"
        updated["timestamp"] = float(new_ts)

        # change some fields for realism
        updated["last_name"] = fake.last_name()
        updated["address"] = self.random_address()

        return updated

    def maybe_build_delete_event(self, prev_event, delete_probability=0.1):
        """
        Optional: sometimes create a delete event for this user_id.
        """
        if random.random() > delete_probability:
            return None

        prev_ts = int(prev_event["timestamp"])
        delta_seconds = random.randint(1 * 24 * 60 * 60, 5 * 24 * 60 * 60)
        new_ts = prev_ts + delta_seconds
        new_ts = min(new_ts, self.END_2023_TS)
        if new_ts <= prev_ts:
            new_ts = prev_ts + 1

        deleted = {
            "user_id": prev_event["user_id"],
            "update_type": "delete",
            "timestamp": float(new_ts),
            # rest of the fields are optional for a delete
        }
        return deleted

    # ============================================================
    # =========================  KAFKA  ==========================
    # ============================================================

    def delivery_callback(self, err, msg):
        if err:
            self.logger.error("Message failed delivery: %s", err.str())
        else:
            key = msg.key().decode("utf-8") if msg.key() else None
            try:
                user_id = json.loads(msg.value().decode("utf-8"))["user_id"]
            except Exception:
                user_id = None
            self.logger.info(
                "Produced event to %s: key=%s user_id=%s",
                msg.topic(),
                key,
                user_id,
            )

    def produce_user_info(self, batch_size=1000, with_updates=True, with_deletes=False):
        """
        Main method:
          - Reads registered_users from Azure SQL.
          - Uses user_info_history to decide whether to send 'new' or 'update'.
          - Produces to Kafka topic user_info.
        """
        self.ensure_history_table()
        history = self.load_history()
        users = self.fetch_registered_users()
        self.logger.info("Fetched %d users from registered_users.", len(users))

        producer = Producer(self.conf)
        batch = []

        for user_row in users:
            user_id = user_row["user_id"]

            # ---------------------------------------
            # 1) If user never seen before -> NEW
            # ---------------------------------------
            if user_id not in history or history[user_id]["timestamp"] is None:
                new_event = self.build_new_event(user_row)
                events_to_send = [new_event]
                # update in-memory history now
                history[user_id] = new_event

            # ---------------------------------------
            # 2) If user seen before -> maybe UPDATE / DELETE
            # ---------------------------------------
            else:
                prev_event = history[user_id]
                events_to_send = []

                # optional update
                if with_updates and random.random() < 0.5:
                    upd_event = self.build_update_event(prev_event)
                    events_to_send.append(upd_event)
                    history[user_id] = upd_event  # keep latest

                    # optional delete after update
                    if with_deletes:
                        del_event = self.maybe_build_delete_event(upd_event)
                        if del_event:
                            events_to_send.append(del_event)
                            history[user_id] = del_event

            # ---------------------------------------
            # 3) Produce all events for this user_id
            # ---------------------------------------
            for event in events_to_send:
                producer.poll(0)
                producer.produce(
                    self.topic,
                    key=str(event["user_id"]).encode("utf-8"),
                    value=json.dumps(event).encode("utf-8"),
                    callback=self.delivery_callback,
                )
                batch.append(event)

                # persist to history table
                self.upsert_history(event)

                if len(batch) >= batch_size:
                    self.logger.info("Flushing %d events...", len(batch))
                    producer.flush()
                    batch = []

        # Flush leftover messages
        if batch:
            self.logger.info("Flushing final %d events...", len(batch))
            producer.flush()

        self.logger.info("Done producing user_info events.")
