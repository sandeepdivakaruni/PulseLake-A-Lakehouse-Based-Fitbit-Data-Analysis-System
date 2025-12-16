import logging

from userinfo_producer import UserInfoProducer
from workout_producer import WorkoutProducer
from bpm_Producer import BpmProducer


def main():
    # ---------- GLOBAL LOGGING SETUP ----------
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # ---------- 1) USER INFO → KAFKA ----------
    user_logger = logging.getLogger("Main.UserInfo")
    user_logger.info("Starting UserInfoProducer...")

    user_producer = UserInfoProducer()
    user_producer.produce_user_info(
        batch_size=500,
        with_updates=True,   # False => only 'new'
        with_deletes=False,  # True  => also send 'delete'
    )

    user_logger.info("Finished UserInfoProducer.\n")

    # ---------- 2) WORKOUT SESSIONS → KAFKA & SQL ----------
    workout_logger = logging.getLogger("Main.Workout")
    workout_logger.info("Starting WorkoutProducer...")

    workout_producer = WorkoutProducer()
    workout_producer.produce_workout_events(
        batch_size=10,   # similar to user_info producer
        limit=None,      # or e.g. 100 if you want to test with small subset
    )

    workout_logger.info("Finished WorkoutProducer.\n")

    # ---------- 3) BPM EVENTS → KAFKA & HISTORY ----------
    bpm_logger = logging.getLogger("Main.BPM")
    bpm_logger.info("Starting BpmProducer...")

    bpm_producer = BpmProducer()
    bpm_producer.produce_bpm(batch_size=1000)

    bpm_logger.info("Finished BpmProducer.\n")

    logging.getLogger("Main").info("All producers finished successfully.")


if __name__ == "__main__":
    main()
 