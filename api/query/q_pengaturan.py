# api/query/q_pengaturan.py

from sqlalchemy import text

from sqlalchemy.exc import SQLAlchemyError

from ..utils.config import get_connection, get_wita


# =========================================================
# GET PENGATURAN POIN
# =========================================================

def get_pengaturan_poin():

    engine = get_connection()

    try:

        with engine.connect() as connection:

            result = connection.execute(text("""
                SELECT
                    key,
                    value
                FROM pengaturan
                WHERE key = 'poin_kelipatan';
            """)).mappings().fetchone()

            if not result:
                return None

            return {
                "poin_kelipatan": int(result["value"])
            }

    except SQLAlchemyError as e:

        print(f"Error occurred: {str(e)}")

        return None


# =========================================================
# UPDATE PENGATURAN POIN
# =========================================================

def update_pengaturan_poin(payload):

    timestamp_wita = get_wita()

    engine = get_connection()

    try:

        with engine.begin() as connection:

            poin_kelipatan = payload.get("poin_kelipatan")

            if poin_kelipatan is None:
                raise ValueError("poin_kelipatan wajib diisi.")

            if poin_kelipatan <= 0:
                raise ValueError(
                    "poin_kelipatan harus lebih dari 0."
                )

            connection.execute(text("""
                UPDATE pengaturan
                SET
                    value = :value
                WHERE key = 'poin_kelipatan';
            """), {
                "value": str(poin_kelipatan)
            })

            return {
                "poin_kelipatan": poin_kelipatan,
                "updated_at": str(timestamp_wita)
            }

    except ValueError as ve:
        raise ve

    except SQLAlchemyError as e:

        print(f"Error occurred: {str(e)}")

        return None