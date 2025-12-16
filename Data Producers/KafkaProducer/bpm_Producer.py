import logging
import json
import random
import pyodbc
from confluent_kafka import Producer


class BpmProducer:
    def __init__(self):
        # ---------- KAFKA CONFIG ----------
        self.topic = "bpm"
        self.conf = {
            "bootstrap.servers": "pkc-921jm.us-east-2.aws.confluent.cloud:9092",
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": "WLBMXFTRTW73LQQN",
            "sasl.password": "cfltMji9WOLt7PvLBf8CmXszMwi4xpQ8TtkeXWiLk7zubxkHdnwoW4qx5Wn0eS1A",
            "client.id": "Sandeep-Bpm-Producer",
        }

        # ---------- AZURE SQL CONFIG ----------
        self.sql_server = "sbitserver15.database.windows.net"
        self.sql_database = "sbit databse"
        self.sql_username = "sandeep"
        self.sql_password = "Miryalguda@15"

        # ---------- LOGGER ----------
        self.logger = logging.getLogger("KafkaBpmProducer")
        self.logger.setLevel(logging.INFO)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)

    # ----------------- DB HELPERS -----------------
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

    def fetch_sessions_needing_bpm(self):
        """
        Fetch all workout sessions that do NOT yet have BPM generated.
        NO LIMITS ANYWHERE.
        """
        sql = """
        SELECT 
               ws.id AS workout_session_id,       -- <--- NEW
               ws.user_id,
               ws.session_id,
               ws.session_start_ts,
               ws.session_end_ts,
               ru.device_id
        FROM dbo.generated_workout_sessions AS ws
        INNER JOIN dbo.registered_users AS ru
            ON ru.user_id = ws.user_id
        LEFT JOIN dbo.generated_bpm_history AS bh
            ON bh.workout_session_id = ws.id     -- <--- FIXED (was bh.user_id = ws.id)
        WHERE bh.id IS NULL
        ORDER BY ws.user_id, ws.session_start_ts;
        """

        with self.get_sql_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()

        sessions = []
        for row in rows:
            sessions.append(
                {
                    "workout_session_id": int(row.workout_session_id),  # <--- NEW
                    "user_id": int(row.user_id),
                    "session_id": int(row.session_id),
                    "session_start_ts": int(row.session_start_ts),
                    "session_end_ts": int(row.session_end_ts),
                    "device_id": int(row.device_id),
                }
            )

        self.logger.info("Fetched %d workout sessions needing BPM.", len(sessions))
        return sessions

    def insert_bpm_history_row(self, conn, session):
        """
        Insert into dbo.generated_bpm_history so we don't re-generate BPM
        for the same session in future runs.
        """
        sql = """
        INSERT INTO dbo.generated_bpm_history
            (workout_session_id, user_id, device_id, session_id, session_start_ts, session_end_ts)
        VALUES (?, ?, ?, ?, ?, ?);
        """
        cursor = conn.cursor()
        cursor.execute(
            sql,
            session["workout_session_id"],   # <--- now present in dict
            session["user_id"],
            session["device_id"],
            session["session_id"],
            session["session_start_ts"],
            session["session_end_ts"],
        )

    # ----------------- BPM GENERATION -----------------
    def simulate_heartrate(self, t_relative_seconds):
        base = 75 + 10 * (t_relative_seconds % 60) / 60.0
        noise = random.uniform(-20, 20)
        hr = base + noise
        return max(35.0, min(190.0, hr))

    def generate_bpm_points_for_session(self, session):
        start_ts = session["session_start_ts"]
        end_ts = session["session_end_ts"]

        points = []
        t_relative = 0
        for ts in range(start_ts, end_ts + 1):
            points.append(
                {
                    "device_id": session["device_id"],
                    "time": ts,
                    "heartrate": self.simulate_heartrate(t_relative),
                }
            )
            t_relative += 1

        return points

    # ----------------- KAFKA -----------------
    def delivery_callback(self, err, msg):
        if err:
            self.logger.error("Message failed delivery: %s", err.str())
        else:
            try:
                payload = json.loads(msg.value().decode("utf-8"))
                device_id = payload.get("device_id")
                ts = payload.get("time")
            except Exception:
                device_id = None
                ts = None

            self.logger.info(
                "Produced BPM event: key=%s device_id=%s time=%s",
                msg.key().decode("utf-8") if msg.key() else None,
                device_id,
                ts,
            )

    # ----------------- MAIN -----------------
    def produce_bpm(self, batch_size=1000):
        """
        1. Fetch ALL unprocessed workout sessions
        2. Generate per-second BPM
        3. Produce to Kafka
        4. Mark session completed
        """
        producer = Producer(self.conf)
        sessions = self.fetch_sessions_needing_bpm()

        if not sessions:
            self.logger.info("No new sessions to generate BPM for. Exiting.")
            return

        with self.get_sql_connection() as conn:
            batch = []

            for session in sessions:
                self.logger.info(
                    "Generating BPM for user_id=%s session_id=%s [%s → %s]",
                    session["user_id"],
                    session["session_id"],
                    session["session_start_ts"],
                    session["session_end_ts"],
                )

                bpm_points = self.generate_bpm_points_for_session(session)

                for point in bpm_points:
                    producer.poll(0)
                    producer.produce(
                        self.topic,
                        key=str(point["device_id"]).encode("utf-8"),
                        value=json.dumps(point).encode("utf-8"),
                        callback=self.delivery_callback,
                    )
                    batch.append(point)

                    if len(batch) >= batch_size:
                        self.logger.info("Flushing %d BPM events...", len(batch))
                        producer.flush()
                        batch = []

                # Mark session processed in history
                self.insert_bpm_history_row(conn, session)
                conn.commit()

                self.logger.info(
                    "Marked session (workout_session_id=%s, user_id=%s, session_id=%s) as generated.",
                    session["workout_session_id"],
                    session["user_id"],
                    session["session_id"],
                )

            if batch:
                self.logger.info("Flushing final %d BPM events...", len(batch))
                producer.flush()

        self.logger.info("Done producing BPM events.")


