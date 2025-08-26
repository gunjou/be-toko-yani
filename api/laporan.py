from datetime import timedelta, date
from flask import logging, request
from flask_restx import Namespace, Resource, fields
from flask_jwt_extended import jwt_required
from sqlalchemy.exc import SQLAlchemyError

from .utils.decorator import role_required
from .query.q_laporan import *


laporan_ns = Namespace("laporan", description="Laporan related endpoints")

@laporan_ns.route('/transaksi')
class LaporanListResource(Resource):
    @role_required('admin')
    @laporan_ns.param("periode", "Periode data: today, this_week, this_month", type="string")
    @laporan_ns.param("start_date", "Tanggal mulai (YYYY-MM-DD)", type="string")
    @laporan_ns.param("end_date", "Tanggal akhir (YYYY-MM-DD)", type="string")
    def get(self):
        """akses: admin; Laporan semua transaksi termasuk hutang, modal, dan keuntungan"""
        periode = request.args.get("periode")
        start_date = request.args.get("start_date")
        end_date = request.args.get("end_date")

        try:
            result = get_all_laporan_transaksi(periode, start_date, end_date)
            if not result:
                return {'status': 'error', 'message': 'Tidak ada laporan yang ditemukan'}, 404
            return {'data': result}, 200
        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")
            return {'status': "Internal server error"}, 500

        

@laporan_ns.route('/penjualan-item')
class LaporanPenjualanItemResource(Resource):
    @role_required('admin')
    @laporan_ns.param("id_produk", "Filter berdasarkan ID produk", type="integer")
    @laporan_ns.param("id_lokasi", "Filter berdasarkan ID lokasi", type="integer")
    @laporan_ns.param("periode", "Opsi: hari_ini, minggu_ini, bulan_ini, range", type="string")
    @laporan_ns.param("start_date", "Tanggal awal (YYYY-MM-DD)", type="string")
    @laporan_ns.param("end_date", "Tanggal akhir (YYYY-MM-DD)", type="string")
    def get(self):
        """
        akses: admin; Laporan penjualan item per produk yang sudah diakumulasi.
        Termasuk total qty, subtotal, modal, dan keuntungan.
        Bisa difilter berdasarkan id_produk, id_lokasi, dan periode tanggal.
        """
        id_produk = request.args.get("id_produk", type=int)
        id_lokasi = request.args.get("id_lokasi", type=int)
        periode = request.args.get("periode")
        start_date = request.args.get("start_date")
        end_date = request.args.get("end_date")

        # Hitung tanggal berdasarkan periode
        today = date.today()
        if periode == "hari_ini":
            start_date = end_date = today.isoformat()
        elif periode == "minggu_ini":
            start_date = (today - timedelta(days=today.weekday())).isoformat()
            end_date = (today + timedelta(days=6 - today.weekday())).isoformat()
        elif periode == "bulan_ini":
            start_date = today.replace(day=1).isoformat()
            end_date = today.isoformat()
        elif periode != "range":
            start_date = end_date = None

        try:
            result = get_laporan_penjualan_item_grouped(
                id_produk=id_produk,
                id_lokasi=id_lokasi,
                start_date=start_date,
                end_date=end_date
            )
            if not result:
                return {'status': 'error', 'message': 'Tidak ada data penjualan item ditemukan'}, 404
            return {'data': result}, 200
        except Exception as e:
            logging.error(f"Error: {str(e)}")
            return {'status': 'error', 'message': 'Terjadi kesalahan'}, 500


@laporan_ns.route('/stok')
class LaporanStokResource(Resource):
    @role_required('admin')
    @laporan_ns.param("id_produk", "Filter berdasarkan ID produk", type="integer")
    @laporan_ns.param("id_lokasi", "Filter berdasarkan ID lokasi", type="integer")
    def get(self):
        """
        akses: admin; Laporan stok per item, termasuk nilai modal dan potensi keuntungan.
        Bisa difilter berdasarkan id_produk dan id_lokasi.
        """
        id_produk = request.args.get("id_produk", type=int)
        id_lokasi = request.args.get("id_lokasi", type=int)

        try:
            result = get_laporan_stok(id_produk=id_produk, id_lokasi=id_lokasi)
            if not result:
                return {'status': 'error', 'message': 'Tidak ada data stok ditemukan'}, 404
            return {'data': result}, 200
        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")
            return {'status': "Internal server error"}, 500


"""#=== Filter function ===#"""
@laporan_ns.route('/filter/produk-terjual')
class ProdukTerjualResource(Resource):
    @role_required('admin')
    @laporan_ns.param("id_lokasi", "Filter berdasarkan ID lokasi", type="integer")
    def get(self):
        """
        akses: admin; Ambil list produk yang pernah muncul dalam transaksi penjualan.
        """
        id_lokasi = request.args.get("id_lokasi", type=int)
        try:
            result = get_produk_yang_terjual(id_lokasi)
            return {'data': result}, 200
        except SQLAlchemyError:
            return {'status': "Internal server error"}, 500


@laporan_ns.route('/filter/produk-tersedia')
class ProdukStokTersediaResource(Resource):
    @role_required('admin')
    @laporan_ns.param("id_lokasi", "Filter berdasarkan ID lokasi", type="integer")
    def get(self):
        """
        akses: admin; Mengambil list produk yang tersedia dalam stok aktif.
        Bisa difilter berdasarkan lokasi.
        """
        id_lokasi = request.args.get("id_lokasi", type=int)
        try:
            result = get_produk_dengan_stok(id_lokasi)
            return {'data': result}, 200
        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")
            return {'status': "Internal server error"}, 500
