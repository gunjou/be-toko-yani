from datetime import date, datetime
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from ..utils.config import get_connection, get_wita


timestamp_wita = get_wita()

from flask import request

def get_all_transaksi():
    engine = get_connection()
    try:
        with engine.connect() as connection:
            id_pelanggan = request.args.get("id_pelanggan")
            tanggal = request.args.get("tanggal")
            status_hutang = request.args.get("status_hutang")
            id_lokasi = request.args.get("id_lokasi")  # <--- Tambahan filter

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
                    conditions.append("(h.status_hutang IS NULL OR h.status_hutang = 'lunas')")
                elif status_hutang.lower() == "belum lunas":
                    conditions.append("h.status_hutang = 'belum lunas'")

            where_clause = " AND ".join(conditions)

            query = f"""
                SELECT 
                    t.id_transaksi, t.id_kasir, t.id_lokasi, t.id_pelanggan,
                    u.username, l.nama_lokasi, p.nama_pelanggan, 
                    t.tanggal, t.total, t.tunai, t.kembalian,
                    h.sisa_hutang, h.status_hutang
                FROM transaksi t
                INNER JOIN users u ON t.id_kasir = u.id_user
                INNER JOIN lokasi l ON t.id_lokasi = l.id_lokasi
                LEFT JOIN pelanggan p ON t.id_pelanggan = p.id_pelanggan
                LEFT JOIN hutang h ON t.id_transaksi = h.id_transaksi
                WHERE {where_clause}
                ORDER BY t.tanggal DESC;
            """

            result = connection.execute(text(query), params).mappings().fetchall()

            data = []
            for row in result:
                row_dict = dict(row)

                if isinstance(row_dict["tanggal"], (date, datetime)):
                    row_dict["tanggal"] = row_dict["tanggal"].isoformat()

                id_transaksi = row_dict["id_transaksi"]
                id_pelanggan = row_dict.get("id_pelanggan")

                # Ambil items transaksi
                detail_result = connection.execute(text("""
                    SELECT dt.id_produk, pr.nama_produk, dt.qty, dt.harga_jual
                    FROM detailtransaksi dt
                    INNER JOIN produk pr ON dt.id_produk = pr.id_produk
                    WHERE dt.id_transaksi = :id_transaksi AND dt.status = 1;
                """), {"id_transaksi": id_transaksi}).mappings().fetchall()

                row_dict["items"] = [
                    {
                        "id_produk": item["id_produk"],
                        "nama_produk": item["nama_produk"],
                        "qty": item["qty"],
                        "harga_jual": item["harga_jual"]
                    } for item in detail_result
                ]

                # Hitung total_hutang jika ada id_pelanggan
                total_hutang = 0
                if id_pelanggan:
                    hutang_result = connection.execute(text("""
                        SELECT SUM(sisa_hutang) AS total_hutang
                        FROM hutang
                        WHERE id_pelanggan = :id_pelanggan AND status = 1;
                    """), {"id_pelanggan": id_pelanggan}).mappings().fetchone()
                    total_hutang = hutang_result["total_hutang"] if hutang_result["total_hutang"] is not None else 0

                row_dict["total_hutang"] = total_hutang

                data.append(row_dict)
            return data
    except SQLAlchemyError as e:
        print(f"Error occurred: {str(e)}")
        return []

def insert_transaksi(payload):
    engine = get_connection()
    try:
        with engine.begin() as connection:
            id_kasir = payload.get("id_kasir")
            id_lokasi = payload.get("id_lokasi")
            id_pelanggan = payload.get("id_pelanggan")  # Optional
            nama_pelanggan = payload.get("nama_pelanggan")
            kontak = payload.get("kontak")
            total = payload.get("total")
            tunai = payload.get("tunai")
            items = payload.get("items", [])

            if not id_kasir or not id_lokasi or total is None or tunai is None:
                raise ValueError("Field id_kasir, id_lokasi, total, dan tunai wajib diisi.")

            if not items or not isinstance(items, list):
                raise ValueError("Daftar produk (items) tidak boleh kosong dan harus berupa list.")

            kembalian = tunai - total if tunai >= total else 0
            sisa_hutang = total - tunai if tunai < total else 0

            # 1. Tambah pelanggan jika belum ada
            if not id_pelanggan and nama_pelanggan:
                result = connection.execute(text("""
                    INSERT INTO pelanggan (nama_pelanggan, kontak, status, created_at, updated_at)
                    VALUES (:nama_pelanggan, :kontak, 1, :timestamp_wita, :timestamp_wita)
                    RETURNING id_pelanggan;
                """), {
                    "nama_pelanggan": nama_pelanggan,
                    "kontak": kontak,
                    "timestamp_wita": timestamp_wita
                })
                id_pelanggan = result.scalar()

            # 2. Jika ada hutang, pelanggan wajib ada
            if tunai < total and not id_pelanggan:
                raise ValueError("Pelanggan wajib diisi atau dibuat jika ada hutang.")

            # 3. Insert transaksi
            result = connection.execute(text("""
                INSERT INTO transaksi (
                    id_kasir, id_lokasi, id_pelanggan,
                    tanggal, total, tunai, kembalian,
                    status, created_at, updated_at
                ) VALUES (
                    :id_kasir, :id_lokasi, :id_pelanggan,
                    :tanggal, :total, :tunai, :kembalian,
                    1, :timestamp_wita, :timestamp_wita
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

            # 4. Insert hutang jika tunai < total
            if tunai < total:
                connection.execute(text("""
                    INSERT INTO hutang (
                        id_transaksi, id_pelanggan, sisa_hutang,
                        status_hutang, status, created_at, updated_at
                    ) VALUES (
                        :id_transaksi, :id_pelanggan, :sisa_hutang,
                        'belum lunas', 1, :timestamp_wita, :timestamp_wita
                    );
                """), {
                    "id_transaksi": id_transaksi,
                    "id_pelanggan": id_pelanggan,
                    "sisa_hutang": sisa_hutang,
                    "timestamp_wita": timestamp_wita
                })

            # 5. Insert detail_transaksi dan kurangi stok
            for item in items:
                id_produk = item.get("id_produk")
                qty = item.get("qty")
                harga_jual = item.get("harga_jual")

                if not id_produk or qty is None or harga_jual is None:
                    raise ValueError("Setiap item harus memiliki id_produk, qty, dan harga_jual.")
                if qty <= 0:
                    raise ValueError(f"Qty harus lebih dari 0 untuk produk ID {id_produk}.")
                if harga_jual < 0:
                    raise ValueError(f"Harga satuan tidak boleh negatif untuk produk ID {id_produk}.")

                # Cek stok di lokasi
                result_stok = connection.execute(text("""
                    SELECT jumlah FROM stok
                    WHERE id_lokasi = :id_lokasi AND id_produk = :id_produk AND status = 1
                    FOR UPDATE;
                """), {
                    "id_lokasi": id_lokasi,
                    "id_produk": id_produk
                }).fetchone()

                if not result_stok or result_stok.jumlah < qty:
                    raise ValueError(f"Stok tidak cukup untuk produk ID {id_produk}.")

                # Kurangi stok
                connection.execute(text("""
                    UPDATE stok
                    SET jumlah = jumlah - :qty, updated_at = :timestamp_wita
                    WHERE id_lokasi = :id_lokasi AND id_produk = :id_produk AND status = 1;
                """), {
                    "qty": qty,
                    "id_lokasi": id_lokasi,
                    "id_produk": id_produk,
                    "timestamp_wita": timestamp_wita
                })

                # Insert ke detail_transaksi
                connection.execute(text("""
                    INSERT INTO detailtransaksi (
                        id_transaksi, id_produk, qty, harga_jual,
                        status, created_at, updated_at
                    ) VALUES (
                        :id_transaksi, :id_produk, :qty, :harga_jual,
                        1, :timestamp_wita, :timestamp_wita
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
                "status_hutang": "belum lunas" if tunai < total else "lunas",
                "sisa_hutang": sisa_hutang,
                "kembalian": kembalian
            }
    except ValueError as ve:
        raise ve
    except SQLAlchemyError as e:
        print(f"DB Error: {str(e)}")
        return None
    
def get_transaksi_by_id(id_transaksi):
    engine = get_connection()
    try:
        with engine.connect() as connection:
            # Ambil semua transaksi beserta user, lokasi, pelanggan, dan hutang
            result = connection.execute(text("""
                SELECT 
                    t.id_transaksi, t.id_kasir, t.id_lokasi, t.id_pelanggan,
                    u.username, l.nama_lokasi, p.nama_pelanggan, 
                    t.tanggal, t.total, t.tunai, t.kembalian,
                    h.sisa_hutang, h.status_hutang
                FROM transaksi t
                INNER JOIN users u ON t.id_kasir = u.id_user
                INNER JOIN lokasi l ON t.id_lokasi = l.id_lokasi
                LEFT JOIN pelanggan p ON t.id_pelanggan = p.id_pelanggan
                LEFT JOIN hutang h ON t.id_transaksi = h.id_transaksi
                WHERE t.id_transaksi = :id_transaksi AND t.status = 1;
            """), {"id_transaksi": id_transaksi}).mappings().fetchall()

            data = []
            for row in result:
                row_dict = dict(row)

                # Format tanggal ke ISO
                if isinstance(row_dict["tanggal"], (date, datetime)):
                    row_dict["tanggal"] = row_dict["tanggal"].isoformat()

                id_transaksi = row_dict["id_transaksi"]
                id_pelanggan = row_dict.get("id_pelanggan")

                # Ambil detail item per transaksi
                detail_result = connection.execute(text("""
                    SELECT dt.id_produk, pr.nama_produk, dt.qty, dt.harga_jual
                    FROM detailtransaksi dt
                    INNER JOIN produk pr ON dt.id_produk = pr.id_produk
                    WHERE dt.id_transaksi = :id_transaksi AND dt.status = 1;
                """), {"id_transaksi": id_transaksi}).mappings().fetchall()

                # Tambahkan item ke transaksi
                items = []
                for item in detail_result:
                    items.append({
                        "id_produk": item["id_produk"],
                        "nama_produk": item["nama_produk"],
                        "qty": item["qty"],
                        "harga_jual": item["harga_jual"]
                    })

                row_dict["items"] = items

                # Hitung total_hutang berdasarkan id_pelanggan
                total_hutang = 0
                if id_pelanggan:
                    hutang_total_result = connection.execute(text("""
                        SELECT SUM(sisa_hutang) AS total_hutang
                        FROM hutang
                        WHERE id_pelanggan = :id_pelanggan AND status = 1;
                    """), {"id_pelanggan": id_pelanggan}).mappings().fetchone()
                    total_hutang = hutang_total_result["total_hutang"] if hutang_total_result["total_hutang"] is not None else 0

                row_dict["total_hutang"] = total_hutang

                data.append(row_dict)

            return data
    except SQLAlchemyError as e:
        print(f"Error occurred: {str(e)}")
        return []
