from datetime import date, datetime
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from ..utils.config import get_connection, get_wita


timestamp_wita = get_wita()

def get_all_mutasi_stok(filters={}):
    engine = get_connection()
    try:
        with engine.connect() as connection:
            conditions = ["m.status = 1"]
            params = {}

            if filters.get("id_produk"):
                conditions.append("m.id_produk = :id_produk")
                params["id_produk"] = filters["id_produk"]

            if filters.get("id_lokasi_asal"):
                conditions.append("m.id_lokasi_asal = :id_lokasi_asal")
                params["id_lokasi_asal"] = filters["id_lokasi_asal"]

            if filters.get("id_lokasi_tujuan"):
                conditions.append("m.id_lokasi_tujuan = :id_lokasi_tujuan")
                params["id_lokasi_tujuan"] = filters["id_lokasi_tujuan"]

            if filters.get("tanggal_awal") and filters.get("tanggal_akhir"):
                conditions.append("m.tanggal BETWEEN :tanggal_awal AND :tanggal_akhir")
                params["tanggal_awal"] = filters["tanggal_awal"]
                params["tanggal_akhir"] = filters["tanggal_akhir"]

            query = f"""
                SELECT 
                    m.id_mutasi_stok, 
                    m.id_produk, 
                    p.nama_produk, 
                    m.id_lokasi_asal, 
                    asal.nama_lokasi AS lokasi_asal, 
                    m.id_lokasi_tujuan, 
                    tujuan.nama_lokasi AS lokasi_tujuan, 
                    m.qty, 
                    m.tanggal, 
                    m.keterangan
                FROM mutasistok m
                INNER JOIN produk p ON m.id_produk = p.id_produk
                INNER JOIN lokasi asal ON m.id_lokasi_asal = asal.id_lokasi
                INNER JOIN lokasi tujuan ON m.id_lokasi_tujuan = tujuan.id_lokasi
                WHERE {" AND ".join(conditions)}
                ORDER BY m.tanggal DESC;
            """

            result = connection.execute(text(query), params).mappings().fetchall()

            data = []
            for row in result:
                row_dict = dict(row)
                # Convert date to string
                if isinstance(row_dict["tanggal"], (date, datetime)):
                    row_dict["tanggal"] = row_dict["tanggal"].isoformat()
                data.append(row_dict)
            return data
    except SQLAlchemyError as e:
        print(f"Error occurred: {str(e)}")
        return []

def insert_mutasi_stok(data):
    engine = get_connection()
    try:
        with engine.begin() as connection:
            # 1. Ambil stok di lokasi asal
            stok_asal = connection.execute(text("""
                SELECT jumlah FROM stok 
                WHERE id_produk = :id_produk AND id_lokasi = :id_lokasi_asal
            """), {
                "id_produk": data["id_produk"],
                "id_lokasi_asal": data["id_lokasi_asal"]
            }).fetchone()

            if not stok_asal:
                return {"success": False, "error": "Stok di lokasi asal tidak ditemukan"}

            if stok_asal[0] < data["qty"]:
                return {"success": False, "error": f"Stok tidak mencukupi di lokasi asal (tersedia {stok_asal[0]})"}

            # 2. Kurangi stok dari lokasi asal
            connection.execute(text("""
                UPDATE stok SET jumlah = jumlah - :qty, updated_at = :updated_at
                WHERE id_produk = :id_produk AND id_lokasi = :id_lokasi_asal
            """), {
                "id_produk": data["id_produk"],
                "id_lokasi_asal": data["id_lokasi_asal"],
                "qty": data["qty"],
                "updated_at": timestamp_wita
            })

            # 3. Tambah atau insert stok di lokasi tujuan
            stok_tujuan = connection.execute(text("""
                SELECT jumlah FROM stok 
                WHERE id_produk = :id_produk AND id_lokasi = :id_lokasi_tujuan
            """), {
                "id_produk": data["id_produk"],
                "id_lokasi_tujuan": data["id_lokasi_tujuan"]
            }).fetchone()

            if stok_tujuan:
                # Update stok
                connection.execute(text("""
                    UPDATE stok SET jumlah = jumlah + :qty, updated_at = :updated_at
                    WHERE id_produk = :id_produk AND id_lokasi = :id_lokasi_tujuan
                """), {
                    "id_produk": data["id_produk"],
                    "id_lokasi_tujuan": data["id_lokasi_tujuan"],
                    "qty": data["qty"],
                    "updated_at": timestamp_wita
                })
            else:
                # Insert stok baru
                connection.execute(text("""
                    INSERT INTO stok (id_produk, id_lokasi, jumlah, status, created_at, updated_at)
                    VALUES (:id_produk, :id_lokasi, :jumlah, 1, :created_at, :updated_at)
                """), {
                    "id_produk": data["id_produk"],
                    "id_lokasi": data["id_lokasi_tujuan"],
                    "jumlah": data["qty"],
                    "created_at": timestamp_wita,
                    "updated_at": timestamp_wita
                })

            # 4. Insert mutasi stok
            result = connection.execute(text("""
                INSERT INTO mutasistok (
                    id_produk, id_lokasi_asal, id_lokasi_tujuan, qty, keterangan,
                    tanggal, status, created_at, updated_at
                )
                VALUES (
                    :id_produk, :id_lokasi_asal, :id_lokasi_tujuan, :qty, :keterangan,
                    :tanggal, 1, :timestamp_wita, :timestamp_wita
                )
                RETURNING id_mutasi_stok
            """), {
                "id_produk": data["id_produk"],
                "id_lokasi_asal": data["id_lokasi_asal"],
                "id_lokasi_tujuan": data["id_lokasi_tujuan"],
                "qty": data["qty"],
                "keterangan": data["keterangan"],
                "tanggal": timestamp_wita.date(),
                "timestamp_wita": timestamp_wita
            }).mappings().fetchone()

            # 5. Ambil informasi produk dan lokasi
            detail = connection.execute(text("""
                SELECT 
                    p.nama_produk,
                    p.satuan,
                    asal.nama_lokasi AS lokasi_asal,
                    tujuan.nama_lokasi AS lokasi_tujuan
                FROM produk p
                JOIN lokasi asal ON asal.id_lokasi = :id_lokasi_asal
                JOIN lokasi tujuan ON tujuan.id_lokasi = :id_lokasi_tujuan
                WHERE p.id_produk = :id_produk
            """), {
                "id_produk": data["id_produk"],
                "id_lokasi_asal": data["id_lokasi_asal"],
                "id_lokasi_tujuan": data["id_lokasi_tujuan"]
            }).mappings().fetchone()

            return {
                "success": True,
                "data": {
                    "id_mutasi_stok": result["id_mutasi_stok"],
                    "nama_produk": detail["nama_produk"],
                    "satuan": detail["satuan"],
                    "jumlah": data["qty"],
                    "lokasi_asal": detail["lokasi_asal"],
                    "lokasi_tujuan": detail["lokasi_tujuan"]
                }
            }
    except SQLAlchemyError as e:
        print(f"Error occurred: {str(e)}")
        return {"success": False, "error": "Terjadi kesalahan pada database"}

