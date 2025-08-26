from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from ..utils.config import get_connection, get_wita


timestamp_wita = get_wita()

def get_stok(id_stok):
    engine = get_connection()
    try:
        with engine.connect() as connection:
            # Ambil id_produk dari stok
            result = connection.execute(text("""
                SELECT id_produk FROM stok WHERE id_stok = :id_stok AND status = 1
            """), {"id_stok": id_stok}).mappings().fetchone()

            if not result:
                return None  # stok tidak ditemukan atau sudah nonaktif
            return result["id_produk"]
    except SQLAlchemyError as e:
        print(f"Error: {str(e)}")
        return []


def get_all_stok(lokasi_id=None):
    engine = get_connection()
    try:
        with engine.connect() as connection:
            query = """
                SELECT s.id_stok, s.id_produk, s.id_lokasi, s.jumlah, 
                    p.nama_produk, p.barcode, p.kategori, p.satuan, p.harga_beli, 
                    p.harga_jual, p.expired_date, p.stok_optimal, l.nama_lokasi, l.tipe
                FROM stok s
                INNER JOIN produk p ON s.id_produk = p.id_produk
                INNER JOIN lokasi l ON s.id_lokasi = l.id_lokasi
                WHERE s.status = 1
            """
            params = {}

            if lokasi_id:
                query += " AND s.id_lokasi = :lokasi_id"
                params["lokasi_id"] = lokasi_id

            result = connection.execute(text(query), params).mappings().fetchall()
            
            data = []
            for row in result:
                row_dict = dict(row)
                if row_dict.get("expired_date"):
                    row_dict["expired_date"] = row_dict["expired_date"].isoformat()
                data.append(row_dict)
            return data
    except SQLAlchemyError as e:
        print(f"Error occurred: {str(e)}")
        return []

def insert_stok(data):
    engine = get_connection()
    try:
        with engine.begin() as connection:
            result_produk = connection.execute(text("""
                INSERT INTO produk 
                    (nama_produk, barcode, kategori, satuan, harga_beli, harga_jual, status, expired_date, stok_optimal, created_at, updated_at)
                VALUES 
                    (:nama_produk, :barcode, :kategori, :satuan, :harga_beli, :harga_jual, 1, :expired_date, :stok_optimal, :timestamp_wita, :timestamp_wita)
                RETURNING id_produk, nama_produk, satuan
            """), {
                "nama_produk": data["nama_produk"],
                "barcode": data["barcode"],
                "kategori": data["kategori"],
                "satuan": data["satuan"],
                "harga_beli": data["harga_beli"],
                "harga_jual": data["harga_jual"],
                "expired_date": data.get("expired_date"),  # Optional
                "stok_optimal": data.get("stok_optimal", 0),
                "timestamp_wita": timestamp_wita
            }).mappings().fetchone()

            id_produk = result_produk["id_produk"]

            # Insert stok
            connection.execute(text("""
                INSERT INTO stok 
                    (id_produk, id_lokasi, jumlah, status, created_at, updated_at)
                VALUES 
                    (:id_produk, :id_lokasi, :jumlah, 1, :timestamp_wita, :timestamp_wita)
            """), {
                "id_produk": id_produk,
                "id_lokasi": data["id_lokasi"],
                "jumlah": data["jumlah"],
                "timestamp_wita": timestamp_wita
            })

            return {
                "id_produk": id_produk,
                "jumlah": data["jumlah"],
                "satuan": result_produk["satuan"],
                "nama_produk": result_produk["nama_produk"]
            }
    except SQLAlchemyError as e:
        print(f"Error occurred: {str(e)}")
        return None

def update_stok(id_stok, data):
    engine = get_connection()
    try:
        with engine.begin() as connection:
            id_produk = get_stok(id_stok)

            # Update produk
            result_produk = connection.execute(text("""
                UPDATE produk SET
                    nama_produk = :nama_produk,
                    barcode = :barcode, 
                    kategori = :kategori, 
                    satuan = :satuan, 
                    harga_beli = :harga_beli, 
                    harga_jual = :harga_jual,
                    expired_date = :expired_date,
                    stok_optimal = :stok_optimal,
                    updated_at = :timestamp_wita
                WHERE id_produk = :id_produk
                RETURNING id_produk, nama_produk, satuan
            """), {
                "id_produk": id_produk,
                "nama_produk": data["nama_produk"],
                "barcode": data["barcode"],
                "kategori": data["kategori"],
                "satuan": data["satuan"],
                "harga_beli": data["harga_beli"],
                "harga_jual": data["harga_jual"],
                "expired_date": data.get("expired_date"),        # Optional
                "stok_optimal": data.get("stok_optimal"),        # Optional
                "timestamp_wita": timestamp_wita
            }).mappings().fetchone()

            # Update stok
            connection.execute(text("""
                UPDATE stok SET
                    id_lokasi = :id_lokasi,
                    jumlah = :jumlah,
                    updated_at = :timestamp_wita
                WHERE id_stok = :id_stok
            """), {
                "id_lokasi": data["id_lokasi"],
                "jumlah": data["jumlah"],
                "timestamp_wita": timestamp_wita,
                "id_stok": id_stok
            })

            return {
                "id_produk": id_produk,
                "jumlah": data["jumlah"],
                "satuan": result_produk["satuan"],
                "nama_produk": result_produk["nama_produk"]
            }
    except SQLAlchemyError as e:
        print(f"Error occurred: {str(e)}")
        return None

def delete_stok(id_stok):
    engine = get_connection()
    try:
        with engine.begin() as connection:
            id_produk = get_stok(id_stok)

            # Nonaktifkan stok
            connection.execute(text("""
                UPDATE stok SET status = 0, updated_at = :timestamp_wita
                WHERE id_stok = :id_stok
            """), {"id_stok": id_stok, "timestamp_wita": timestamp_wita})

            # Nonaktifkan produk
            result_produk = connection.execute(text("""
                UPDATE produk SET status = 0, updated_at = :timestamp_wita
                WHERE id_produk = :id_produk
                RETURNING nama_produk
            """), {"id_produk": id_produk, "timestamp_wita": timestamp_wita}).mappings().fetchone()

            return result_produk["nama_produk"] if result_produk else None
    except SQLAlchemyError as e:
        print(f"Error occurred: {str(e)}")
        return None
