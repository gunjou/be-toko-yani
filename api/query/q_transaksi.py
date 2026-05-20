# api/query/q_transaksi.py

from datetime import date, datetime
from flask import request
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from ..utils.config import get_connection, get_wita


# =========================================================
# GET ALL TRANSAKSI
# =========================================================

def get_all_transaksi():

    engine = get_connection()

    try:

        with engine.connect() as connection:

            id_pelanggan = request.args.get("id_pelanggan")
            tanggal = request.args.get("tanggal")
            status_hutang = request.args.get("status_hutang")
            id_lokasi = request.args.get("id_lokasi")

            conditions = ["t.status = 1"]
            params = {}

            if id_pelanggan:
                conditions.append("t.id_pelanggan = :id_pelanggan")
                params["id_pelanggan"] = id_pelanggan

            if tanggal:
                conditions.append("t.tanggal = :tanggal")
                params["tanggal"] = tanggal

            if id_lokasi:
                conditions.append("t.id_lokasi = :id_lokasi")
                params["id_lokasi"] = id_lokasi

            if status_hutang:

                if status_hutang.lower() == "lunas":
                    conditions.append(
                        "(h.status_hutang IS NULL OR h.status_hutang = 'lunas')"
                    )

                elif status_hutang.lower() == "belum lunas":
                    conditions.append(
                        "h.status_hutang = 'belum lunas'"
                    )

            where_clause = " AND ".join(conditions)

            query = f"""
                SELECT 
                    t.id_transaksi, t.id_kasir, t.id_lokasi, t.id_pelanggan,
                    u.username, l.nama_lokasi,
                    p.nama_pelanggan, p.kontak, p.alamat, p.poin,
                    t.tanggal, t.total, t.tunai, t.kembalian,
                    h.sisa_hutang, h.status_hutang
                FROM transaksi t
                INNER JOIN users u
                    ON t.id_kasir = u.id_user
                INNER JOIN lokasi l
                    ON t.id_lokasi = l.id_lokasi
                LEFT JOIN pelanggan p
                    ON t.id_pelanggan = p.id_pelanggan
                LEFT JOIN hutang h
                    ON t.id_transaksi = h.id_transaksi
                WHERE {where_clause}
                ORDER BY t.tanggal DESC;
            """
            result = connection.execute(
                text(query),
                params
            ).mappings().fetchall()

            data = []

            for row in result:

                row_dict = dict(row)

                if isinstance(row_dict["tanggal"], (date, datetime)):
                    row_dict["tanggal"] = row_dict["tanggal"].isoformat()

                id_transaksi = row_dict["id_transaksi"]
                id_pelanggan = row_dict.get("id_pelanggan")

                # =========================================================
                # DETAIL ITEM
                # =========================================================

                detail_result = connection.execute(text("""
                    SELECT
                        dt.id_produk,
                        pr.nama_produk,
                        dt.qty,
                        dt.harga_jual
                    FROM detailtransaksi dt
                    INNER JOIN produk pr
                        ON dt.id_produk = pr.id_produk
                    WHERE dt.id_transaksi = :id_transaksi
                    AND dt.status = 1;
                """), {
                    "id_transaksi": id_transaksi
                }).mappings().fetchall()

                row_dict["items"] = [
                    {
                        "id_produk": item["id_produk"],
                        "nama_produk": item["nama_produk"],
                        "qty": item["qty"],
                        "harga_jual": item["harga_jual"]
                    }
                    for item in detail_result
                ]

                # =========================================================
                # TOTAL HUTANG
                # =========================================================

                total_hutang = 0

                if id_pelanggan:

                    hutang_result = connection.execute(text("""
                        SELECT
                            SUM(sisa_hutang) AS total_hutang
                        FROM hutang
                        WHERE id_pelanggan = :id_pelanggan
                        AND status = 1;
                    """), {
                        "id_pelanggan": id_pelanggan
                    }).mappings().fetchone()

                    total_hutang = (
                        hutang_result["total_hutang"]
                        if hutang_result["total_hutang"] is not None
                        else 0
                    )

                row_dict["total_hutang"] = total_hutang

                data.append(row_dict)

            return data

    except SQLAlchemyError as e:
        print(f"Error occurred: {str(e)}")
        return []


# =========================================================
# INSERT TRANSAKSI
# =========================================================

def insert_transaksi(payload):

    timestamp_wita = get_wita()

    engine = get_connection()

    try:

        with engine.begin() as connection:

            id_kasir = payload.get("id_kasir")
            id_lokasi = payload.get("id_lokasi")

            id_pelanggan = payload.get("id_pelanggan")

            nama_pelanggan = payload.get("nama_pelanggan")
            kontak = payload.get("kontak")
            alamat = payload.get("alamat")

            total = payload.get("total")
            tunai = payload.get("tunai")

            items = payload.get("items", [])

            # =========================================================
            # VALIDASI
            # =========================================================

            if not id_kasir or not id_lokasi:
                raise ValueError(
                    "Field id_kasir dan id_lokasi wajib diisi."
                )

            if total is None or tunai is None:
                raise ValueError(
                    "Field total dan tunai wajib diisi."
                )

            if not items or not isinstance(items, list):
                raise ValueError(
                    "Daftar produk (items) tidak boleh kosong."
                )

            kembalian = tunai - total if tunai >= total else 0
            sisa_hutang = total - tunai if tunai < total else 0

            # =========================================================
            # TAMBAH PELANGGAN BARU
            # =========================================================

            if not id_pelanggan and nama_pelanggan:

                result = connection.execute(text("""
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
                    RETURNING id_pelanggan;
                """), {
                    "nama_pelanggan": nama_pelanggan,
                    "kontak": kontak,
                    "alamat": alamat,
                    "timestamp_wita": timestamp_wita
                })

                id_pelanggan = result.scalar()

            # =========================================================
            # VALIDASI HUTANG
            # =========================================================

            if tunai < total and not id_pelanggan:
                raise ValueError(
                    "Pelanggan wajib diisi jika transaksi hutang."
                )

            # =========================================================
            # INSERT TRANSAKSI
            # =========================================================

            result = connection.execute(text("""
                INSERT INTO transaksi (
                    id_kasir,
                    id_lokasi,
                    id_pelanggan,
                    tanggal,
                    total,
                    tunai,
                    kembalian,
                    status,
                    created_at,
                    updated_at
                )
                VALUES (
                    :id_kasir,
                    :id_lokasi,
                    :id_pelanggan,
                    :tanggal,
                    :total,
                    :tunai,
                    :kembalian,
                    1,
                    :timestamp_wita,
                    :timestamp_wita
                )
                RETURNING id_transaksi;
            """), {
                "id_kasir": id_kasir,
                "id_lokasi": id_lokasi,
                "id_pelanggan": id_pelanggan,
                "tanggal": timestamp_wita.date(),
                "total": total,
                "tunai": tunai,
                "kembalian": kembalian,
                "timestamp_wita": timestamp_wita
            })

            id_transaksi = result.scalar()

            # =========================================================
            # HITUNG POIN
            # =========================================================

            earned_point = 0

            if id_pelanggan:

                # =========================================================
                # AMBIL PENGATURAN POIN
                # =========================================================

                setting = connection.execute(text("""
                    SELECT value
                    FROM pengaturan
                    WHERE key = 'poin_kelipatan';
                """)).mappings().fetchone()

                poin_kelipatan = int(setting["value"]) if setting else 35000

                # =========================================================
                # HITUNG POIN
                # =========================================================

                earned_point = total // poin_kelipatan

                if earned_point > 0:

                    # update poin pelanggan
                    connection.execute(text("""
                        UPDATE pelanggan
                        SET
                            poin = poin + :earned_point,
                            updated_at = :timestamp_wita
                        WHERE id_pelanggan = :id_pelanggan
                        AND status = 1;
                    """), {
                        "earned_point": earned_point,
                        "id_pelanggan": id_pelanggan,
                        "timestamp_wita": timestamp_wita
                    })

                    # insert histori poin
                    connection.execute(text("""
                        INSERT INTO poin_pelanggan (
                            id_pelanggan,
                            id_transaksi,
                            tipe,
                            poin,
                            deskripsi,
                            status,
                            created_at,
                            updated_at
                        )
                        VALUES (
                            :id_pelanggan,
                            :id_transaksi,
                            'earn',
                            :earned_point,
                            :deskripsi,
                            1,
                            :timestamp_wita,
                            :timestamp_wita
                        );
                    """), {
                        "id_pelanggan": id_pelanggan,
                        "id_transaksi": id_transaksi,
                        "earned_point": earned_point,
                        "deskripsi": f"Poin dari transaksi #{id_transaksi}",
                        "timestamp_wita": timestamp_wita
                    })

            # =========================================================
            # INSERT HUTANG
            # =========================================================

            if tunai < total:

                connection.execute(text("""
                    INSERT INTO hutang (
                        id_transaksi,
                        id_pelanggan,
                        sisa_hutang,
                        status_hutang,
                        status,
                        created_at,
                        updated_at
                    )
                    VALUES (
                        :id_transaksi,
                        :id_pelanggan,
                        :sisa_hutang,
                        'belum lunas',
                        1,
                        :timestamp_wita,
                        :timestamp_wita
                    );
                """), {
                    "id_transaksi": id_transaksi,
                    "id_pelanggan": id_pelanggan,
                    "sisa_hutang": sisa_hutang,
                    "timestamp_wita": timestamp_wita
                })

            # =========================================================
            # INSERT DETAIL & KURANGI STOK
            # =========================================================

            for item in items:

                id_produk = item.get("id_produk")
                qty = item.get("qty")
                harga_jual = item.get("harga_jual")

                if not id_produk:
                    raise ValueError("id_produk wajib diisi.")

                if qty is None or qty <= 0:
                    raise ValueError(
                        f"Qty tidak valid untuk produk ID {id_produk}."
                    )

                if harga_jual is None or harga_jual < 0:
                    raise ValueError(
                        f"Harga jual tidak valid untuk produk ID {id_produk}."
                    )

                # lock stok
                result_stok = connection.execute(text("""
                    SELECT jumlah
                    FROM stok
                    WHERE id_lokasi = :id_lokasi
                    AND id_produk = :id_produk
                    AND status = 1
                    FOR UPDATE;
                """), {
                    "id_lokasi": id_lokasi,
                    "id_produk": id_produk
                }).fetchone()

                if not result_stok:
                    raise ValueError(
                        f"Stok produk ID {id_produk} tidak ditemukan."
                    )

                if result_stok.jumlah < qty:
                    raise ValueError(
                        f"Stok tidak cukup untuk produk ID {id_produk}."
                    )

                # kurangi stok
                connection.execute(text("""
                    UPDATE stok
                    SET
                        jumlah = jumlah - :qty,
                        updated_at = :timestamp_wita
                    WHERE id_lokasi = :id_lokasi
                    AND id_produk = :id_produk
                    AND status = 1;
                """), {
                    "qty": qty,
                    "id_lokasi": id_lokasi,
                    "id_produk": id_produk,
                    "timestamp_wita": timestamp_wita
                })

                # insert detail transaksi
                connection.execute(text("""
                    INSERT INTO detailtransaksi (
                        id_transaksi,
                        id_produk,
                        qty,
                        harga_jual,
                        status,
                        created_at,
                        updated_at
                    )
                    VALUES (
                        :id_transaksi,
                        :id_produk,
                        :qty,
                        :harga_jual,
                        1,
                        :timestamp_wita,
                        :timestamp_wita
                    );
                """), {
                    "id_transaksi": id_transaksi,
                    "id_produk": id_produk,
                    "qty": qty,
                    "harga_jual": harga_jual,
                    "timestamp_wita": timestamp_wita
                })

            return {
                "id_transaksi": id_transaksi,
                "id_pelanggan": id_pelanggan,
                "earned_point": earned_point,
                "status_hutang": (
                    "belum lunas"
                    if tunai < total
                    else "lunas"
                ),
                "sisa_hutang": sisa_hutang,
                "kembalian": kembalian
            }

    except ValueError as ve:
        raise ve

    except SQLAlchemyError as e:
        print(f"DB Error: {str(e)}")
        return None


# =========================================================
# GET TRANSAKSI BY ID
# =========================================================

def get_transaksi_by_id(id_transaksi):

    engine = get_connection()

    try:

        with engine.connect() as connection:

            result = connection.execute(text("""
                SELECT 
                    t.id_transaksi,
                    t.id_kasir,
                    t.id_lokasi,
                    t.id_pelanggan,

                    u.username,
                    l.nama_lokasi,

                    p.nama_pelanggan,
                    p.kontak,
                    p.alamat,
                    p.poin,

                    t.tanggal,
                    t.total,
                    t.tunai,
                    t.kembalian,

                    h.sisa_hutang,
                    h.status_hutang

                FROM transaksi t

                INNER JOIN users u
                    ON t.id_kasir = u.id_user

                INNER JOIN lokasi l
                    ON t.id_lokasi = l.id_lokasi

                LEFT JOIN pelanggan p
                    ON t.id_pelanggan = p.id_pelanggan

                LEFT JOIN hutang h
                    ON t.id_transaksi = h.id_transaksi

                WHERE t.id_transaksi = :id_transaksi
                AND t.status = 1;
            """), {
                "id_transaksi": id_transaksi
            }).mappings().fetchall()

            data = []

            for row in result:

                row_dict = dict(row)

                if isinstance(row_dict["tanggal"], (date, datetime)):
                    row_dict["tanggal"] = row_dict["tanggal"].isoformat()

                detail_result = connection.execute(text("""
                    SELECT
                        dt.id_produk,
                        pr.nama_produk,
                        dt.qty,
                        dt.harga_jual
                    FROM detailtransaksi dt
                    INNER JOIN produk pr
                        ON dt.id_produk = pr.id_produk
                    WHERE dt.id_transaksi = :id_transaksi
                    AND dt.status = 1;
                """), {
                    "id_transaksi": row_dict["id_transaksi"]
                }).mappings().fetchall()

                row_dict["items"] = [
                    {
                        "id_produk": item["id_produk"],
                        "nama_produk": item["nama_produk"],
                        "qty": item["qty"],
                        "harga_jual": item["harga_jual"]
                    }
                    for item in detail_result
                ]

                data.append(row_dict)

            return data

    except SQLAlchemyError as e:
        print(f"Error occurred: {str(e)}")
        return []