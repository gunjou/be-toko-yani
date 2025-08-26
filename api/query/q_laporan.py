from datetime import date, datetime, timedelta
from flask import logging
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from ..utils.config import get_connection, get_wita


timestamp_wita = get_wita()

def get_all_laporan_transaksi(periode=None, start_date=None, end_date=None):
    engine = get_connection()
    try:
        with engine.connect() as connection:
            query = """
                SELECT 
                    t.id_transaksi, t.id_kasir, t.id_lokasi, t.id_pelanggan,
                    t.tanggal, t.total, t.tunai, t.kembalian,
                    COALESCE(h.sisa_hutang, 0) AS sisa_hutang,
                    COALESCE(h.status_hutang, 'lunas') AS status_hutang
                FROM transaksi t
                LEFT JOIN hutang h ON t.id_transaksi = h.id_transaksi
                WHERE t.status = 1
            """
            params = {}

            # Tambahan filter berdasarkan periode
            today = date.today()
            if periode == "today":
                query += " AND DATE(t.tanggal) = :today"
                params["today"] = today
            elif periode == "this_week":
                start_of_week = today - timedelta(days=today.weekday())
                end_of_week = start_of_week + timedelta(days=6)
                query += " AND DATE(t.tanggal) BETWEEN :start_of_week AND :end_of_week"
                params["start_of_week"] = start_of_week
                params["end_of_week"] = end_of_week
            elif periode == "this_month":
                start_of_month = today.replace(day=1)
                next_month = (start_of_month.replace(day=28) + timedelta(days=4)).replace(day=1)
                end_of_month = next_month - timedelta(days=1)
                query += " AND DATE(t.tanggal) BETWEEN :start_of_month AND :end_of_month"
                params["start_of_month"] = start_of_month
                params["end_of_month"] = end_of_month

            # Filter berdasarkan rentang tanggal
            if start_date and end_date:
                try:
                    start_date_parsed = datetime.strptime(start_date, "%Y-%m-%d").date()
                    end_date_parsed = datetime.strptime(end_date, "%Y-%m-%d").date()
                    query += " AND DATE(t.tanggal) BETWEEN :start_date AND :end_date"
                    params["start_date"] = start_date_parsed
                    params["end_date"] = end_date_parsed
                except ValueError:
                    pass  # Jika format tidak valid, abaikan saja filter ini

            query += " ORDER BY t.tanggal DESC"

            result = connection.execute(text(query), params).mappings().fetchall()

            data = []
            for row in result:
                row_dict = dict(row)

                if isinstance(row_dict["tanggal"], (date, datetime)):
                    row_dict["tanggal"] = row_dict["tanggal"].isoformat()

                id_transaksi = row_dict["id_transaksi"]

                # Hitung modal = SUM(qty × harga_beli) dari detailtransaksi
                modal_result = connection.execute(text("""
                    SELECT 
                        SUM(dt.qty * pr.harga_beli) AS total_modal
                    FROM detailtransaksi dt
                    INNER JOIN produk pr ON dt.id_produk = pr.id_produk
                    WHERE dt.id_transaksi = :id_transaksi AND dt.status = 1;
                """), {"id_transaksi": id_transaksi}).scalar()

                modal = modal_result if modal_result else 0
                keuntungan = row_dict["total"] - modal

                row_dict["modal"] = modal
                row_dict["keuntungan"] = keuntungan

                data.append(row_dict)
            return data
    except SQLAlchemyError as e:
        print(f"Error occurred: {str(e)}")
        return []
    
def get_laporan_penjualan_item_grouped(id_produk=None, id_lokasi=None, start_date=None, end_date=None):
    engine = get_connection()
    try:
        with engine.connect() as connection:
            query = """
                SELECT 
                    dt.id_produk,
                    pr.nama_produk,
                    pr.satuan,
                    SUM(dt.qty) AS total_qty,
                    pr.harga_beli,
                    dt.harga_jual,
                    SUM(dt.qty * dt.harga_jual) AS subtotal,
                    SUM(dt.qty * pr.harga_beli) AS modal,
                    SUM((dt.qty * dt.harga_jual) - (dt.qty * pr.harga_beli)) AS keuntungan
                FROM detailtransaksi dt
                INNER JOIN produk pr ON dt.id_produk = pr.id_produk
                INNER JOIN transaksi t ON dt.id_transaksi = t.id_transaksi
                WHERE dt.status = 1 AND t.status = 1
            """
            params = {}

            if id_produk:
                query += " AND dt.id_produk = :id_produk"
                params["id_produk"] = id_produk
            if id_lokasi:
                query += " AND t.id_lokasi = :id_lokasi"
                params["id_lokasi"] = id_lokasi
            if start_date and end_date:
                query += " AND DATE(t.tanggal) BETWEEN :start_date AND :end_date"
                params["start_date"] = start_date
                params["end_date"] = end_date

            query += """
                GROUP BY dt.id_produk, pr.nama_produk, pr.satuan, pr.harga_beli, dt.harga_jual
                ORDER BY pr.nama_produk ASC
            """
            result = connection.execute(text(query), params).mappings().fetchall()
            return [dict(row) for row in result]
    except SQLAlchemyError as e:
        print(f"Error occurred: {str(e)}")
        return []

def get_laporan_stok(id_produk=None, id_lokasi=None, start_date=None, end_date=None):
    engine = get_connection()
    try:
        with engine.connect() as connection:
            query = """
                SELECT 
                    s.id_lokasi,
                    l.nama_lokasi,
                    s.id_produk,
                    p.nama_produk,
                    p.satuan,
                    p.harga_beli,
                    p.harga_jual,
                    p.expired_date,
                    p.stok_optimal,
                    s.jumlah AS sisa_stok,
                    (s.jumlah * p.harga_beli) AS nilai_modal,
                    (s.jumlah * (p.harga_jual - p.harga_beli)) AS potensi_keuntungan
                FROM stok s
                INNER JOIN produk p ON s.id_produk = p.id_produk
                INNER JOIN lokasi l ON s.id_lokasi = l.id_lokasi
                WHERE s.status = 1 AND p.status = 1 AND l.status = 1
            """
            params = {}

            if id_produk:
                query += " AND s.id_produk = :id_produk"
                params["id_produk"] = id_produk
            if id_lokasi:
                query += " AND s.id_lokasi = :id_lokasi"
                params["id_lokasi"] = id_lokasi
            if start_date and end_date:
                query += " AND DATE(s.updated_at) BETWEEN :start_date AND :end_date"
                params["start_date"] = start_date
                params["end_date"] = end_date

            query += " ORDER BY s.id_lokasi, s.id_produk"

            result = connection.execute(text(query), params).mappings().fetchall()

            laporan = []
            for row in result:
                data = dict(row)
                if data["expired_date"]:
                    data["expired_date"] = data["expired_date"].isoformat()
                laporan.append(data)

            return laporan
    except SQLAlchemyError as e:
        print(f"Error occurred: {str(e)}")
        return []

"""#=== Filter function ===#"""
def get_produk_yang_terjual(id_lokasi=None):
    engine = get_connection()
    try:
        with engine.connect() as connection:
            query = """
                SELECT DISTINCT
                    pr.id_produk,
                    pr.nama_produk
                FROM detailtransaksi dt
                INNER JOIN produk pr ON dt.id_produk = pr.id_produk
                INNER JOIN transaksi t ON dt.id_transaksi = t.id_transaksi
                WHERE dt.status = 1 AND t.status = 1
            """
            params = {}
            if id_lokasi:
                query += " AND t.id_lokasi = :id_lokasi"
                params["id_lokasi"] = id_lokasi

            query += " ORDER BY pr.nama_produk ASC"

            result = connection.execute(text(query), params).mappings().fetchall()
            return [dict(row) for row in result]
    except SQLAlchemyError as e:
        logging.error(f"Database error: {str(e)}")
        return []

def get_produk_dengan_stok(id_lokasi=None):
    engine = get_connection()
    try:
        with engine.connect() as connection:
            query = """
                SELECT DISTINCT
                    p.id_produk,
                    p.nama_produk
                FROM stok s
                INNER JOIN produk p ON s.id_produk = p.id_produk
                INNER JOIN lokasi l ON s.id_lokasi = l.id_lokasi
                WHERE s.status = 1 AND p.status = 1 AND l.status = 1
            """
            params = {}

            if id_lokasi:
                query += " AND s.id_lokasi = :id_lokasi"
                params["id_lokasi"] = id_lokasi

            query += " ORDER BY p.nama_produk ASC"

            result = connection.execute(text(query), params).mappings().fetchall()
            return [dict(row) for row in result]
    except SQLAlchemyError as e:
        logging.error(f"Database error: {str(e)}")
        return []
