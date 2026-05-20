# api/query/q_reward_poin.py

from sqlalchemy import text

from sqlalchemy.exc import SQLAlchemyError

from ..utils.config import get_connection, get_wita


# =========================================================
# GET ALL REWARD
# =========================================================

def get_all_reward_poin():

    engine = get_connection()

    try:

        with engine.connect() as connection:

            result = connection.execute(text("""
                SELECT
                    rp.id_reward,
                    rp.id_produk,

                    p.nama_produk,
                    p.barcode,
                    p.kategori,
                    p.harga_jual,

                    rp.poin_required,

                    rp.created_at,
                    rp.updated_at

                FROM reward_poin rp

                INNER JOIN produk p
                    ON rp.id_produk = p.id_produk

                WHERE rp.status = 1
                AND p.status = 1

                ORDER BY rp.id_reward DESC;
            """)).mappings().fetchall()

            return [dict(row) for row in result]

    except SQLAlchemyError as e:

        print(f"Error occurred: {str(e)}")

        return []


# =========================================================
# GET REWARD BY ID
# =========================================================

def get_reward_poin_by_id(id_reward):

    engine = get_connection()

    try:

        with engine.connect() as connection:

            result = connection.execute(text("""
                SELECT
                    rp.id_reward,
                    rp.id_produk,

                    p.nama_produk,
                    p.barcode,
                    p.kategori,
                    p.harga_jual,

                    rp.poin_required,

                    rp.created_at,
                    rp.updated_at

                FROM reward_poin rp

                INNER JOIN produk p
                    ON rp.id_produk = p.id_produk

                WHERE rp.id_reward = :id_reward
                AND rp.status = 1
                AND p.status = 1;
            """), {
                "id_reward": id_reward
            }).mappings().fetchone()

            return dict(result) if result else None

    except SQLAlchemyError as e:

        print(f"Error occurred: {str(e)}")

        return None


# =========================================================
# INSERT REWARD
# =========================================================

def insert_reward_poin(payload):

    timestamp_wita = get_wita()

    engine = get_connection()

    try:

        with engine.begin() as connection:

            id_produk = payload.get("id_produk")
            poin_required = payload.get("poin_required")

            if not id_produk:
                raise ValueError("id_produk wajib diisi.")

            if poin_required is None or poin_required <= 0:
                raise ValueError("poin_required harus lebih dari 0.")

            # cek produk ada
            produk = connection.execute(text("""
                SELECT id_produk, nama_produk
                FROM produk
                WHERE id_produk = :id_produk
                AND status = 1;
            """), {
                "id_produk": id_produk
            }).mappings().fetchone()

            if not produk:
                raise ValueError("Produk tidak ditemukan.")

            # cek reward duplicate
            existing = connection.execute(text("""
                SELECT id_reward
                FROM reward_poin
                WHERE id_produk = :id_produk
                AND status = 1;
            """), {
                "id_produk": id_produk
            }).fetchone()

            if existing:
                raise ValueError("Produk sudah terdaftar sebagai reward.")

            result = connection.execute(text("""
                INSERT INTO reward_poin (
                    id_produk,
                    poin_required,
                    status,
                    created_at,
                    updated_at
                )
                VALUES (
                    :id_produk,
                    :poin_required,
                    1,
                    :timestamp_wita,
                    :timestamp_wita
                )
                RETURNING id_reward;
            """), {
                "id_produk": id_produk,
                "poin_required": poin_required,
                "timestamp_wita": timestamp_wita
            })

            id_reward = result.scalar()

            return {
                "id_reward": id_reward,
                "id_produk": id_produk,
                "nama_produk": produk["nama_produk"],
                "poin_required": poin_required
            }

    except ValueError as ve:
        raise ve

    except SQLAlchemyError as e:

        print(f"Error occurred: {str(e)}")

        return None


# =========================================================
# UPDATE REWARD
# =========================================================

def update_reward_poin(id_reward, payload):

    timestamp_wita = get_wita()

    engine = get_connection()

    try:

        with engine.begin() as connection:

            id_produk = payload.get("id_produk")
            poin_required = payload.get("poin_required")

            if not id_produk:
                raise ValueError("id_produk wajib diisi.")

            if poin_required is None or poin_required <= 0:
                raise ValueError("poin_required harus lebih dari 0.")

            result = connection.execute(text("""
                UPDATE reward_poin
                SET
                    id_produk = :id_produk,
                    poin_required = :poin_required,
                    updated_at = :timestamp_wita
                WHERE id_reward = :id_reward
                AND status = 1
                RETURNING id_produk;
            """), {
                "id_reward": id_reward,
                "id_produk": id_produk,
                "poin_required": poin_required,
                "timestamp_wita": timestamp_wita
            }).fetchone()

            if not result:
                return None

            produk = connection.execute(text("""
                SELECT nama_produk
                FROM produk
                WHERE id_produk = :id_produk;
            """), {
                "id_produk": id_produk
            }).mappings().fetchone()

            return {
                "id_reward": id_reward,
                "nama_produk": produk["nama_produk"]
            }

    except ValueError as ve:
        raise ve

    except SQLAlchemyError as e:

        print(f"Error occurred: {str(e)}")

        return None


# =========================================================
# DELETE REWARD
# =========================================================

def delete_reward_poin(id_reward):

    timestamp_wita = get_wita()

    engine = get_connection()

    try:

        with engine.begin() as connection:

            result = connection.execute(text("""
                UPDATE reward_poin
                SET
                    status = 0,
                    updated_at = :timestamp_wita
                WHERE id_reward = :id_reward
                AND status = 1
                RETURNING id_produk;
            """), {
                "id_reward": id_reward,
                "timestamp_wita": timestamp_wita
            }).mappings().fetchone()

            if not result:
                return None

            produk = connection.execute(text("""
                SELECT nama_produk
                FROM produk
                WHERE id_produk = :id_produk;
            """), {
                "id_produk": result["id_produk"]
            }).mappings().fetchone()

            return {
                "nama_produk": produk["nama_produk"]
            }

    except SQLAlchemyError as e:

        print(f"Error occurred: {str(e)}")

        return None