from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from ..utils.config import get_connection, get_wita


timestamp_wita = get_wita()

def get_all_hutang():
    engine = get_connection()
    try:
        with engine.connect() as connection:
            result = connection.execute(text("""
                SELECT id_hutang, id_transaksi, id_pelanggan, sisa_hutang, status_hutang
                FROM hutang
                WHERE status = 1
                AND status_hutang = 'belum lunas';   
            """)).mappings().fetchall()
            return [dict(row) for row in result]
    except SQLAlchemyError as e:
        print(f"Error occurred: {str(e)}")
        return []
    
def insert_hutang(data):
    engine = get_connection()
    try:
        with engine.begin() as connection:
            result = connection.execute(text("""
                INSERT INTO hutang (id_transaksi, id_pelanggan, sisa_hutang, status_hutang, status, created_at, updated_at)
                VALUES (:id_transaksi, :id_pelanggan, :sisa_hutang, :status_hutang, 1, :timestamp_wita, :timestamp_wita)
                RETURNING sisa_hutang, status_hutang
            """), {**data, "timestamp_wita": timestamp_wita}).mappings().fetchone()
            return dict(result)
    except SQLAlchemyError as e:
        print(f"Error occurred: {str(e)}")
        return None
    
def get_hutang_by_id(id_hutang):
    engine = get_connection()
    try:
        with engine.connect() as connection:
            result = connection.execute(text("""
                SELECT id_hutang, id_transaksi, id_pelanggan, sisa_hutang, status_hutang
                FROM hutang
                WHERE id_hutang = :id_hutang
                AND status = 1
                AND status_hutang = 'belum lunas';   
            """), {'id_hutang': id_hutang}).mappings().fetchone()
            return dict(result) if result else None
    except SQLAlchemyError as e:
        print(f"Error occurred: {str(e)}")
        return []
    
def update_hutang(id_hutang, data):
    engine = get_connection()
    try:
        with engine.begin() as connection:
            result = connection.execute(
                text("""
                    UPDATE hutang SET sisa_hutang = :sisa_hutang, status_hutang = :status_hutang, updated_at = :timestamp_wita
                    WHERE id_hutang = :id_hutang AND status_hutang = 'belum lunas' RETURNING sisa_hutang, status_hutang;
                    """
                    ),
                {**data, "id_hutang": id_hutang, "timestamp_wita": timestamp_wita}
            ).fetchone()
            return result
    except SQLAlchemyError as e:
        print(f"Error: {e}")
        return None
    
def delete_hutang(id_hutang):
    engine = get_connection()
    try:
        with engine.begin() as connection:
            result = connection.execute(
                text("""UPDATE hutang SET status = 0, updated_at = :timestamp_wita 
                    WHERE status = 1 
                    AND id_hutang = :id_hutang
                    RETURNING id_hutang;"""),
                {"id_hutang": id_hutang, "timestamp_wita": timestamp_wita}
            ).fetchone()
            return result
    except SQLAlchemyError as e:
        print(f"Error: {e}")
        return None
    
def get_total_hutang_per_pelanggan(id_pelanggan=None):
    engine = get_connection()
    try:
        with engine.connect() as connection:
            query = """
                SELECT
                    h.id_pelanggan,
                    p.nama_pelanggan,
                    p.kontak,
                    COALESCE(SUM(h.sisa_hutang), 0) AS total_sisa_hutang
                FROM hutang h
                JOIN pelanggan p ON h.id_pelanggan = p.id_pelanggan
                WHERE h.status = 1 AND h.status_hutang = 'belum lunas'
            """
            params = {}

            if id_pelanggan:
                query += " AND h.id_pelanggan = :id_pelanggan"
                params["id_pelanggan"] = id_pelanggan

            query += " GROUP BY h.id_pelanggan, p.nama_pelanggan, p.kontak ORDER BY total_sisa_hutang DESC"

            result = connection.execute(text(query), params).mappings().fetchall()
            return [dict(row) for row in result]
    except SQLAlchemyError as e:
        print(f"Error occurred: {str(e)}")
        return []

def count_total_hutang_by_id(id_pelanggan):
    engine = get_connection()
    try:
        with engine.connect() as connection:
            result = connection.execute(text("""
                SELECT 
                    h.id_pelanggan,
                    COALESCE(SUM(h.sisa_hutang), 0) AS total_hutang,
                    p.nama_pelanggan,
                    p.kontak
                FROM hutang h
                INNER JOIN pelanggan p ON h.id_pelanggan = p.id_pelanggan   
                WHERE h.status = 1 
                AND h.status_hutang = 'belum lunas' 
                AND h.id_pelanggan = :id_pelanggan
                GROUP BY h.id_pelanggan, p.nama_pelanggan, p.kontak
            """), {"id_pelanggan": id_pelanggan}).mappings().fetchone()

            if not result:
                return {
                    "status": "error",
                    "message": "Tidak ada hutang ditemukan untuk pelanggan ini"
                }

            return {
                "id_pelanggan": result['id_pelanggan'],
                "nama_pelanggan": result['nama_pelanggan'],
                "kontak": result['kontak'],
                "total_hutang": result['total_hutang']
            }
    except SQLAlchemyError as e:
        print(f"Error: {e}")
        return []

def bayar_hutang(id_pelanggan, jumlah_bayar):
    engine = get_connection()
    try:
        with engine.begin() as connection:
            sisa_bayar = jumlah_bayar
            result = connection.execute(text("""
                SELECT h.id_hutang, h.sisa_hutang, p.nama_pelanggan, p.kontak
                FROM hutang h
                INNER JOIN pelanggan p ON h.id_pelanggan = p.id_pelanggan   
                WHERE h.id_pelanggan = :id_pelanggan
                AND h.status = 1
                AND h.status_hutang = 'belum lunas'
                ORDER BY id_hutang ASC
            """), {"id_pelanggan": id_pelanggan}).mappings().fetchall()

            if not result:
                return None

            data = []

            for row in result:
                if sisa_bayar <= 0:
                    break

                id_hutang = row['id_hutang']
                sisa_hutang = row['sisa_hutang']
                nama_pelanggan = row['nama_pelanggan']
                kontak = row['kontak']

                if sisa_bayar >= sisa_hutang:
                    # Lunas
                    connection.execute(text("""
                        UPDATE hutang
                        SET sisa_hutang = 0, status_hutang = 'lunas', updated_at = :timestamp_wita
                        WHERE id_hutang = :id_hutang
                    """), {"id_hutang": id_hutang, "timestamp_wita": timestamp_wita})
                    data.append({
                        "id_hutang": id_hutang, "nama_pelanggan": nama_pelanggan, "kontak": kontak, 
                        "dibayar": sisa_hutang, "status": "lunas"
                        })
                    sisa_bayar -= sisa_hutang
                else:
                    # Sebagian
                    connection.execute(text("""
                        UPDATE hutang
                        SET sisa_hutang = sisa_hutang - :dibayar, updated_at = :timestamp_wita
                        WHERE id_hutang = :id_hutang
                    """), {"dibayar": sisa_bayar, "id_hutang": id_hutang, "timestamp_wita": timestamp_wita})
                    data.append({
                        "id_hutang": id_hutang, "nama_pelanggan": nama_pelanggan, "kontak": kontak, 
                        "dibayar": sisa_bayar, "status": "sebagian"
                        })
                    sisa_bayar = 0
            return data
    except SQLAlchemyError as e:
        print(f"Error: {e}")
        return None
