import logging
import json
import random
import pyodbc
from confluent_kafka import Producer


class WorkoutProducer:
    def __init__(self):
        # ---------- KAFKA CONFIG ----------
        self.topic = "workout"
        self.conf = {
            "bootstrap.servers": "pkc-921jm.us-east-2.aws.confluent.cloud:9092",
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": "WLBMXFTRTW73LQQN",
            "sasl.password": "cfltMji9WOLt7PvLBf8CmXszMwi4xpQ8TtkeXWiLk7zubxkHdnwoW4qx5Wn0eS1A",
            "client.id": "Sandeep-Workout-Producer",
        }

        # ---------- AZURE SQL CONFIG ----------
        self.sql_server = "sbitserver15.database.windows.net"
        self.sql_database = "sbit databse"
        self.sql_username = "sandeep"
        self.sql_password = "Miryalguda@15"

        # ---------- CONSTANTS ----------
        self.BUFFER_SECONDS = 5 * 60              # 5 min after login & before logout
        self.MIN_SESSION_SECONDS = 20 * 60        # minimum 20 min per session
        self.MIN_GAP_BETWEEN_SESS = 4 * 60        # 4 min min gap
        self.MAX_GAP_BETWEEN_SESS = 7 * 60        # 7 min max gap
        self.MIN_TOTAL_SECONDS = 30 * 60          # below this → 1 session
        self.MAX_TOTAL_SECONDS = 180 * 60         # above this → cap to 3 sessions

        # ---------- LOGGER ----------
        self.logger = logging.getLogger("KafkaWorkoutProducer")
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

    def fetch_gym_logins_to_process(self, limit=None):
        sql = """
        SELECT
            gl.mac_address,
            gl.login AS login_ts,
            gl.logout AS logout_ts,
            ru.user_id
        FROM dbo.gym_login AS gl
        JOIN dbo.registered_users AS ru
            ON ru.mac_address = gl.mac_address
        LEFT JOIN dbo.generated_workout_sessions AS gws
            ON gws.user_id = ru.user_id
           AND gws.login_ts = gl.login
        WHERE gws.user_id IS NULL
        """

        if limit:
            sql += f" ORDER BY gl.login OFFSET 0 ROWS FETCH NEXT {int(limit)} ROWS ONLY;"

        with self.get_sql_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()

        logins = []
        for row in rows:
            logins.append(
                {
                    "user_id": int(row.user_id),
                    "mac_address": row.mac_address.strip(),
                    "login_ts": int(row.login_ts),
                    "logout_ts": int(row.logout_ts),
                }
            )

        self.logger.info("Fetched %d gym login rows to process.", len(logins))
        return logins

    # ----------------- SESSION COUNT -----------------
    def determine_session_count(self, login_ts, logout_ts):
        duration = logout_ts - login_ts
        if duration < self.MIN_TOTAL_SECONDS:
            return 1
        elif duration < 60 * 60:
            return 1
        elif duration < 120 * 60:
            return 2
        elif duration <= self.MAX_TOTAL_SECONDS:
            return 3
        else:
            return 3

    # ----------------- SESSION GENERATION (UPDATED) -----------------
    def generate_sessions_for_login(self, login_row):
        """
        For one gym visit, generate a list of sessions with:
          - random gaps (4–7 min) between sessions
          - random exercise durations per session
        Total (all session lengths + all gaps) fits exactly inside:
            [login_ts + BUFFER, logout_ts - BUFFER]
        """
        user_id = login_row["user_id"]
        mac = login_row["mac_address"]
        login_ts = login_row["login_ts"]
        logout_ts = login_row["logout_ts"]

        # Apply fixed buffer at entry & exit
        effective_start = login_ts + self.BUFFER_SECONDS
        effective_end = logout_ts - self.BUFFER_SECONDS

        if effective_end <= effective_start:
            self.logger.warning(
                "Login too short for user %s (login=%s, logout=%s)",
                user_id, login_ts, logout_ts,
            )
            return []

        total_span = effective_end - effective_start
        session_count = self.determine_session_count(login_ts, logout_ts)

        # If we don't have enough time for multiple sessions + gaps,
        # just fall back to 1 session using the entire window.
        if session_count == 1:
            return [
                {
                    "user_id": user_id,
                    "mac_address": mac,
                    "login_ts": login_ts,
                    "session_start_ts": int(effective_start),
                    "session_end_ts": int(effective_end),
                    "session_id": 1,
                }
            ]

        min_gap_total = (session_count - 1) * self.MIN_GAP_BETWEEN_SESS
        max_gap_total = (session_count - 1) * self.MAX_GAP_BETWEEN_SESS
        min_len_total = session_count * self.MIN_SESSION_SECONDS

        # If total time is too small to respect min length + min gaps, collapse to 1 session
        if total_span < min_gap_total + min_len_total:
            self.logger.warning(
                "Not enough time for %d sessions with gaps for user %s, "
                "falling back to single session.",
                session_count, user_id,
            )
            return [
                {
                    "user_id": user_id,
                    "mac_address": mac,
                    "login_ts": login_ts,
                    "session_start_ts": int(effective_start),
                    "session_end_ts": int(effective_end),
                    "session_id": 1,
                }
            ]

        # ----- 1) choose total gap time randomly -----
        max_allowed_gap_total = min(
            max_gap_total,
            total_span - min_len_total
        )
        gap_total = random.randint(min_gap_total, max_allowed_gap_total)

        # Split total gap into (session_count - 1) random pieces
        gaps = []
        remaining_gap = gap_total
        remaining_slots = session_count - 1
        for i in range(session_count - 1):
            if i == session_count - 2:
                g = remaining_gap
            else:
                max_for_this = remaining_gap - (remaining_slots - 1) * self.MIN_GAP_BETWEEN_SESS
                g = random.randint(self.MIN_GAP_BETWEEN_SESS, max_for_this)
            gaps.append(g)
            remaining_gap -= g
            remaining_slots -= 1

        # ----- 2) allocate exercise time randomly across sessions -----
        len_total = total_span - gap_total
        lengths = []
        remaining_len = len_total
        remaining_sessions = session_count
        for i in range(session_count):
            if i == session_count - 1:
                length = remaining_len
            else:
                max_for_this = remaining_len - (remaining_sessions - 1) * self.MIN_SESSION_SECONDS
                length = random.randint(self.MIN_SESSION_SECONDS, max_for_this)
            lengths.append(length)
            remaining_len -= length
            remaining_sessions -= 1

        # ----- 3) build actual sessions timeline -----
        sessions = []
        current_start = effective_start

        for idx in range(session_count):
            sid = idx + 1
            session_start = current_start
            session_end = session_start + lengths[idx]

            sessions.append(
                {
                    "user_id": user_id,
                    "mac_address": mac,
                    "login_ts": login_ts,
                    "session_start_ts": int(session_start),
                    "session_end_ts": int(session_end),
                    "session_id": sid,
                }
            )

            if idx < session_count - 1:
                current_start = session_end + gaps[idx]

        # At this point the last session_end should be exactly effective_end
        # (because lengths + gaps = total_span).  Just in case, clamp it.
        sessions[-1]["session_end_ts"] = int(effective_end)

        return sessions

    # ----------------- EVENT BUILDERS, KAFKA, produce_workout_events -----------------
    # (Everything below is your existing code – unchanged.)

    def build_start_event(self, session_row, workout_id=1):
        return {
            "user_id": session_row["user_id"],
            "workout_id": workout_id,
            "timestamp": float(session_row["session_start_ts"]),
            "action": "start",
            "session_id": session_row["session_id"],
        }

    def build_stop_event(self, session_row, workout_id=1):
        return {
            "user_id": session_row["user_id"],
            "workout_id": workout_id,
            "timestamp": float(session_row["session_end_ts"]),
            "action": "stop",
            "session_id": session_row["session_id"],
        }

    def delivery_callback(self, err, msg):
        if err:
            self.logger.error("Message failed delivery: %s", err.str())
        else:
            key = msg.key().decode("utf-8") if msg.key() else None
            try:
                payload = json.loads(msg.value().decode("utf-8"))
                user_id = payload.get("user_id")
                action = payload.get("action")
                session_id = payload.get("session_id")
            except Exception:
                user_id = None
                action = None
                session_id = None

            self.logger.info(
                "Produced event to %s: key=%s user_id=%s action=%s session_id=%s",
                msg.topic(),
                key,
                user_id,
                action,
                session_id,
            )

    def produce_workout_events(self, batch_size=1000, limit=None):
        producer = Producer(self.conf)
        logins = self.fetch_gym_logins_to_process(limit=limit)

        if not logins:
            self.logger.info("No new gym logins found to process.")
            return

        with self.get_sql_connection() as conn:
            cursor = conn.cursor()

            insert_sql = """
            INSERT INTO dbo.generated_workout_sessions
                (user_id, mac_address, login_ts, session_start_ts, session_end_ts, session_id)
            VALUES (?, ?, ?, ?, ?, ?)
            """

            events_in_batch = 0

            for login_row in logins:
                sessions = self.generate_sessions_for_login(login_row)
                if not sessions:
                    continue

                for session in sessions:
                    # Insert into SQL
                    cursor.execute(
                        insert_sql,
                        session["user_id"],
                        session["mac_address"],
                        session["login_ts"],
                        session["session_start_ts"],
                        session["session_end_ts"],
                        session["session_id"],
                    )

                    # Start + stop events
                    for event in (
                        self.build_start_event(session),
                        self.build_stop_event(session),
                    ):
                        producer.poll(0)
                        producer.produce(
                            self.topic,
                            key=str(event["user_id"]).encode("utf-8"),
                            value=json.dumps(event).encode("utf-8"),
                            callback=self.delivery_callback,
                        )
                        events_in_batch += 1

                        if events_in_batch >= batch_size:
                            self.logger.info(
                                "Flushing %d events to Kafka...", events_in_batch
                            )
                            producer.flush()
                            events_in_batch = 0

            if events_in_batch > 0:
                self.logger.info(
                    "Flushing final %d events to Kafka...", events_in_batch
                )
                producer.flush()

            conn.commit()

        self.logger.info("Done producing workout events.")
