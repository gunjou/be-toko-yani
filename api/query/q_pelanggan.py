from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from ..utils.config import get_connection, get_wita


timestamp_wita = get_wita()

def get_all_pelanggan():
    engine = get_connection()
    try:
        with engine.connect() as connection:
            result = connection.execute(text("""
                SELECT id_pelanggan, nama_pelanggan, kontak
                FROM pelanggan
                WHERE status = 1;   
            """)).mappings().fetchall()
            return [dict(row) for row in result]
    except SQLAlchemyError as e:
        print(f"Error occurred: {str(e)}")
        return []
    
def insert_pelanggan(data):
    engine = get_connection()
    try:
        with engine.begin() as connection:
            result = connection.execute(text("""
                INSERT INTO pelanggan (nama_pelanggan, kontak, status, created_at, updated_at)
                VALUES (:nama_pelanggan, :kontak, 1, :timestamp_wita, :timestamp_wita)
                RETURNING nama_pelanggan
            """), {**data, "timestamp_wita": timestamp_wita}).mappings().fetchone()
            return dict(result)
    except SQLAlchemyError as e:
        print(f"Error occurred: {str(e)}")
        return None
    
def get_pelanggan_by_id(id_pelanggan):
    engine = get_connection()
    try:
        with engine.connect() as connection:
            result = connection.execute(text("""
                SELECT id_pelanggan, nama_pelanggan, kontak
                FROM pelanggan
                WHERE id_pelanggan = :id_pelanggan
                AND status = 1;   
            """), {'id_pelanggan': id_pelanggan}).mappings().fetchone()
            return dict(result) if result else None
    except SQLAlchemyError as e:
        print(f"Error occurred: {str(e)}")
        return []
    
def update_pelanggan(id_pelanggan, data):
    engine = get_connection()
    try:
        with engine.begin() as connection:
            result = connection.execute(
                text("""
                    UPDATE pelanggan SET nama_pelanggan = :nama_pelanggan, kontak = :kontak, updated_at = :timestamp_wita
                    WHERE id_pelanggan = :id_pelanggan RETURNING nama_pelanggan;
                    """
                    ),
                {**data, "id_pelanggan": id_pelanggan, "timestamp_wita": timestamp_wita}
            ).fetchone()
            return result
    except SQLAlchemyError as e:
        print(f"Error: {e}")
        return None
    
def delete_pelanggan(id_pelanggan):
    engine = get_connection()
    try:
        with engine.begin() as connection:
            result = connection.execute(
                text("UPDATE pelanggan SET status = 0, updated_at = :timestamp_wita WHERE status = 1 AND id_pelanggan = :id_pelanggan RETURNING nama_pelanggan;"),
                {"id_pelanggan": id_pelanggan, "timestamp_wita": timestamp_wita}
            ).fetchone()
            return result
    except SQLAlchemyError as e:
        print(f"Error: {e}")
        return None