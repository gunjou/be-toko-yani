# api/query/q_pelanggan.py
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from ..utils.config import get_connection, get_wita


# =========================================================
# GET ALL PELANGGAN
# =========================================================

def get_all_pelanggan():

    engine = get_connection()

    try:

        with engine.connect() as connection:

            result = connection.execute(text("""
                SELECT
                    id_pelanggan,
                    nama_pelanggan,
                    kontak,
                    alamat,
                    poin
                FROM pelanggan
                WHERE status = 1
                ORDER BY id_pelanggan DESC;
            """)).mappings().fetchall()

            return [dict(row) for row in result]

    except SQLAlchemyError as e:
        print(f"Error occurred: {str(e)}")
        return []


# =========================================================
# INSERT PELANGGAN
# =========================================================

def insert_pelanggan(data):

    timestamp_wita = get_wita()

    engine = get_connection()

    try:

        with engine.begin() as connection:

            result = connection.execute(
                text("""
                    INSERT INTO pelanggan (
                        nama_pelanggan,
                        kontak,
                        alamat,
                        poin,
                        status,
                        created_at,
                        updated_at
                    )
                    VALUES (
                        :nama_pelanggan,
                        :kontak,
                        :alamat,
                        0,
                        1,
                        :timestamp_wita,
                        :timestamp_wita
                    )
                    RETURNING
                        id_pelanggan,
                        nama_pelanggan,
                        kontak,
                        alamat,
                        poin;
                """),
                {
                    "nama_pelanggan": data.get("nama_pelanggan"),
                    "kontak": data.get("kontak"),
                    "alamat": data.get("alamat"),
                    "timestamp_wita": timestamp_wita
                }
            ).mappings().fetchone()

            return dict(result)

    except SQLAlchemyError as e:
        print(f"Error occurred: {str(e)}")
        return None


# =========================================================
# GET DETAIL PELANGGAN
# =========================================================

def get_pelanggan_by_id(id_pelanggan):

    engine = get_connection()

    try:

        with engine.connect() as connection:

            result = connection.execute(
                text("""
                    SELECT
                        id_pelanggan,
                        nama_pelanggan,
                        kontak,
                        alamat,
                        poin,
                        created_at,
                        updated_at
                    FROM pelanggan
                    WHERE id_pelanggan = :id_pelanggan
                    AND status = 1;
                """),
                {
                    'id_pelanggan': id_pelanggan
                }
            ).mappings().fetchone()

            return dict(result) if result else None

    except SQLAlchemyError as e:
        print(f"Error occurred: {str(e)}")
        return None


# =========================================================
# UPDATE PELANGGAN
# =========================================================

def update_pelanggan(id_pelanggan, data):

    timestamp_wita = get_wita()

    engine = get_connection()

    try:

        with engine.begin() as connection:

            result = connection.execute(
                text("""
                    UPDATE pelanggan
                    SET
                        nama_pelanggan = :nama_pelanggan,
                        kontak = :kontak,
                        alamat = :alamat,
                        updated_at = :timestamp_wita
                    WHERE id_pelanggan = :id_pelanggan
                    AND status = 1
                    RETURNING nama_pelanggan;
                """),
                {
                    "id_pelanggan": id_pelanggan,
                    "nama_pelanggan": data.get("nama_pelanggan"),
                    "kontak": data.get("kontak"),
                    "alamat": data.get("alamat"),
                    "timestamp_wita": timestamp_wita
                }
            ).mappings().fetchone()

            return dict(result) if result else None

    except SQLAlchemyError as e:
        print(f"Error: {e}")
        return None


# =========================================================
# DELETE PELANGGAN
# =========================================================

def delete_pelanggan(id_pelanggan):

    timestamp_wita = get_wita()

    engine = get_connection()

    try:

        with engine.begin() as connection:

            result = connection.execute(
                text("""
                    UPDATE pelanggan
                    SET
                        status = 0,
                        updated_at = :timestamp_wita
                    WHERE status = 1
                    AND id_pelanggan = :id_pelanggan
                    RETURNING nama_pelanggan;
                """),
                {
                    "id_pelanggan": id_pelanggan,
                    "timestamp_wita": timestamp_wita
                }
            ).mappings().fetchone()

            return dict(result) if result else None

    except SQLAlchemyError as e:
        print(f"Error: {e}")
        return None


# =========================================================
# GET TOTAL POIN PELANGGAN
# =========================================================

def get_poin_pelanggan(id_pelanggan):

    engine = get_connection()

    try:

        with engine.connect() as connection:

            result = connection.execute(
                text("""
                    SELECT
                        id_pelanggan,
                        nama_pelanggan,
                        poin
                    FROM pelanggan
                    WHERE id_pelanggan = :id_pelanggan
                    AND status = 1;
                """),
                {
                    "id_pelanggan": id_pelanggan
                }
            ).mappings().fetchone()

            return dict(result) if result else None

    except SQLAlchemyError as e:
        print(f"Error: {e}")
        return None


# =========================================================
# GET HISTORI POIN
# =========================================================

def get_histori_poin_pelanggan(id_pelanggan):

    engine = get_connection()

    try:

        with engine.connect() as connection:

            result = connection.execute(
                text("""
                    SELECT
                        id_poin,
                        tipe,
                        poin,
                        deskripsi,
                        created_at
                    FROM poin_pelanggan
                    WHERE id_pelanggan = :id_pelanggan
                    AND status = 1
                    ORDER BY id_poin DESC;
                """),
                {
                    "id_pelanggan": id_pelanggan
                }
            ).mappings().fetchall()

            return [dict(row) for row in result]

    except SQLAlchemyError as e:
        print(f"Error: {e}")
        return []


# =========================================================
# REDEEM POIN
# =========================================================

def redeem_poin(id_pelanggan, payload):

    timestamp_wita = get_wita()

    engine = get_connection()

    try:

        with engine.begin() as connection:

            id_reward = payload.get("id_reward")
            qty = payload.get("qty", 1)

            if not id_reward:
                raise ValueError("id_reward wajib diisi.")

            if qty <= 0:
                raise ValueError("qty harus lebih dari 0.")

            # =========================================================
            # CEK PELANGGAN
            # =========================================================

            pelanggan = connection.execute(text("""
                SELECT
                    id_pelanggan,
                    nama_pelanggan,
                    poin
                FROM pelanggan
                WHERE id_pelanggan = :id_pelanggan
                AND status = 1
                FOR UPDATE;
            """), {
                "id_pelanggan": id_pelanggan
            }).mappings().fetchone()

            if not pelanggan:
                raise ValueError("Pelanggan tidak ditemukan.")

            # =========================================================
            # CEK REWARD
            # =========================================================

            reward = connection.execute(text("""
                SELECT
                    rp.id_reward,
                    rp.id_produk,
                    rp.poin_required,

                    p.nama_produk

                FROM reward_poin rp

                INNER JOIN produk p
                    ON rp.id_produk = p.id_produk

                WHERE rp.id_reward = :id_reward
                AND rp.status = 1
                AND p.status = 1;
            """), {
                "id_reward": id_reward
            }).mappings().fetchone()

            if not reward:
                raise ValueError("Reward poin tidak ditemukan.")

            total_poin_required = reward["poin_required"] * qty

            # =========================================================
            # CEK POIN
            # =========================================================

            if pelanggan["poin"] < total_poin_required:
                raise ValueError(
                    f"Poin pelanggan tidak cukup. "
                    f"Minimal {total_poin_required} poin."
                )

            # =========================================================
            # CEK STOK
            # =========================================================

            stok = connection.execute(text("""
                SELECT
                    id_stok,
                    jumlah
                FROM stok
                WHERE id_produk = :id_produk
                AND status = 1
                ORDER BY jumlah DESC
                LIMIT 1
                FOR UPDATE;
            """), {
                "id_produk": reward["id_produk"]
            }).mappings().fetchone()

            if not stok:
                raise ValueError("Stok reward tidak ditemukan.")

            if stok["jumlah"] < qty:
                raise ValueError("Stok reward tidak mencukupi.")

            # =========================================================
            # KURANGI POIN
            # =========================================================

            connection.execute(text("""
                UPDATE pelanggan
                SET
                    poin = poin - :total_poin_required,
                    updated_at = :timestamp_wita
                WHERE id_pelanggan = :id_pelanggan;
            """), {
                "total_poin_required": total_poin_required,
                "id_pelanggan": id_pelanggan,
                "timestamp_wita": timestamp_wita
            })

            # =========================================================
            # KURANGI STOK
            # =========================================================

            connection.execute(text("""
                UPDATE stok
                SET
                    jumlah = jumlah - :qty,
                    updated_at = :timestamp_wita
                WHERE id_stok = :id_stok;
            """), {
                "qty": qty,
                "id_stok": stok["id_stok"],
                "timestamp_wita": timestamp_wita
            })

            # =========================================================
            # INSERT HISTORI POIN
            # =========================================================

            connection.execute(text("""
                INSERT INTO poin_pelanggan (
                    id_pelanggan,
                    tipe,
                    poin,
                    deskripsi,
                    status,
                    created_at,
                    updated_at
                )
                VALUES (
                    :id_pelanggan,
                    'redeem',
                    :poin,
                    :deskripsi,
                    1,
                    :timestamp_wita,
                    :timestamp_wita
                );
            """), {
                "id_pelanggan": id_pelanggan,
                "poin": -total_poin_required,
                "deskripsi": (
                    f"Redeem {reward['nama_produk']} "
                    f"x{qty}"
                ),
                "timestamp_wita": timestamp_wita
            })

            # =========================================================
            # AMBIL SISA POIN
            # =========================================================

            updated_poin = connection.execute(text("""
                SELECT poin
                FROM pelanggan
                WHERE id_pelanggan = :id_pelanggan;
            """), {
                "id_pelanggan": id_pelanggan
            }).mappings().fetchone()

            return {
                "id_pelanggan": id_pelanggan,
                "nama_pelanggan": pelanggan["nama_pelanggan"],

                "id_reward": reward["id_reward"],
                "nama_produk": reward["nama_produk"],

                "qty": qty,

                "poin_digunakan": total_poin_required,
                "sisa_poin": updated_poin["poin"]
            }

    except ValueError as ve:
        raise ve

    except SQLAlchemyError as e:

        print(f"Error redeem poin: {str(e)}")

        return None