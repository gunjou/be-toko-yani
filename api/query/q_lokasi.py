from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from ..utils.config import get_connection, get_wita


timestamp_wita = get_wita()

def get_all_lokasi():
    engine = get_connection()
    try:
        with engine.connect() as connection:
            result = connection.execute(text("""
                SELECT id_lokasi, nama_lokasi, tipe
                FROM lokasi
                WHERE status = 1;   
            """)).mappings().fetchall()
            return [dict(row) for row in result]
    except SQLAlchemyError as e:
        print(f"Error occurred: {str(e)}")
        return []
    
def insert_lokasi(data):
    engine = get_connection()
    try:
        with engine.begin() as connection:
            result = connection.execute(text("""
                INSERT INTO lokasi (nama_lokasi, tipe, status)
                VALUES (:nama_lokasi, :tipe, 1)
                RETURNING nama_lokasi
            """), data).mappings().fetchone()
            return dict(result)
    except SQLAlchemyError as e:
        print(f"Error occurred: {str(e)}")
        return None
    
def get_lokasi_by_id(id_lokasi):
    engine = get_connection()
    try:
        with engine.connect() as connection:
            result = connection.execute(text("""
                SELECT id_lokasi, nama_lokasi, tipe
                FROM lokasi
                WHERE id_lokasi = :id_lokasi
                AND status = 1;   
            """), {'id_lokasi': id_lokasi}).mappings().fetchone()
            return dict(result) if result else None
    except SQLAlchemyError as e:
        print(f"Error occurred: {str(e)}")
        return []
    
def update_lokasi(id_lokasi, data):
    engine = get_connection()
    try:
        with engine.begin() as connection:
            result = connection.execute(
                text("""
                    UPDATE lokasi SET nama_lokasi = :nama_lokasi, tipe = :tipe, updated_at = :timestamp_wita
                    WHERE id_lokasi = :id_lokasi RETURNING nama_lokasi;
                    """
                    ),
                {**data, "id_lokasi": id_lokasi, "timestamp_wita": timestamp_wita}
            ).fetchone()
            return result
    except SQLAlchemyError as e:
        print(f"Error: {e}")
        return None
    
def delete_lokasi(id_lokasi):
    engine = get_connection()
    try:
        with engine.begin() as connection:
            result = connection.execute(
                text("UPDATE lokasi SET status = 0, updated_at = :timestamp_wita WHERE status = 1 AND id_lokasi = :id_lokasi RETURNING nama_lokasi;"),
                {"id_lokasi": id_lokasi, "timestamp_wita": timestamp_wita}
            ).fetchone()
            return result
    except SQLAlchemyError as e:
        print(f"Error: {e}")
        return None