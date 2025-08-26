from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from ..utils.config import get_connection, get_wita


timestamp_wita = get_wita()

def get_all_produk():
    engine = get_connection()
    try:
        with engine.connect() as connection:
            result = connection.execute(text("""
                SELECT id_produk, nama_produk, barcode, kategori, satuan, harga_beli, harga_jual, expired_date, stok_optimal
                FROM produk
                WHERE status = 1;   
            """)).mappings().fetchall()

            produk_list = []
            for row in result:
                data = dict(row)
                if data["expired_date"]:
                    data["expired_date"] = data["expired_date"].isoformat()
                produk_list.append(data)

            return produk_list
    except SQLAlchemyError as e:
        print(f"Error occurred: {str(e)}")
        return []

    
# def insert_produk(data):
#     engine = get_connection()
#     try:
#         with engine.begin() as connection:
#             result = connection.execute(text("""
#                 INSERT INTO produk (nama_produk, barcode, kategori, satuan, harga_beli, harga_jual, status, created_at, updated_at)
#                 VALUES (:nama_produk, :barcode, :kategori, :satuan, :harga_beli, :harga_jual, 1, :timestamp_wita, :timestamp_wita)
#                 RETURNING nama_produk
#             """), {**data, 'timestamp_wita': timestamp_wita}).mappings().fetchone()
#             return dict(result)
#     except SQLAlchemyError as e:
#         print(f"Error occurred: {str(e)}")
#         return None
    
def get_produk_by_id(id_produk):
    engine = get_connection()
    try:
        with engine.connect() as connection:
            result = connection.execute(text("""
                SELECT id_produk, nama_produk, barcode, kategori, satuan, harga_beli, harga_jual, expired_date, stok_optimal
                FROM produk
                WHERE id_produk = :id_produk
                AND status = 1;   
            """), {'id_produk': id_produk}).mappings().fetchone()
            
            if result:
                data = dict(result)
                if data["expired_date"]:
                    data["expired_date"] = data["expired_date"].isoformat()
                return data
            else:
                return None
    except SQLAlchemyError as e:
        print(f"Error occurred: {str(e)}")
        return []
    
def update_produk(id_produk, data):
    engine = get_connection()
    try:
        with engine.begin() as connection:
            result = connection.execute(
                text("""
                    UPDATE produk 
                    SET 
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
                    RETURNING nama_produk;
                    """
                    ),
                {**data, 'id_produk': id_produk, 'timestamp_wita': timestamp_wita}
            ).fetchone()
            return result
    except SQLAlchemyError as e:
        print(f"Error: {e}")
        return None
    
def delete_produk(id_produk):
    engine = get_connection()
    try:
        with engine.begin() as connection:
            result = connection.execute(
                text("UPDATE produk SET status = 0, updated_at = :timestamp_wita WHERE status = 1 AND id_produk = :id_produk RETURNING nama_produk;"),
                {'id_produk': id_produk, 'timestamp_wita': timestamp_wita}
            ).fetchone()
            return result
    except SQLAlchemyError as e:
        print(f"Error: {e}")
        return None