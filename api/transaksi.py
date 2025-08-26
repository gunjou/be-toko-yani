from flask import logging, request
from flask_restx import Namespace, Resource, fields
from flask_jwt_extended import jwt_required
from sqlalchemy.exc import SQLAlchemyError

from .query.q_transaksi import *

transaksi_ns = Namespace("transaksi", description="Transaksi related endpoints")

item_model = transaksi_ns.model("Item", {
    "id_produk": fields.Integer(required=True, description="ID produk"),
    "qty": fields.Integer(required=True, description="Jumlah produk"),
    "harga_jual": fields.Integer(required=True, description="Harga jual satuan produk"),
})

transaksi_model = transaksi_ns.model("Transaksi", {
    "id_kasir": fields.Integer(required=True, description="ID kasir"),
    "id_lokasi": fields.Integer(required=True, description="ID lokasi"),
    "id_pelanggan": fields.Integer(required=False, description="ID pelanggan (opsional)"),
    "nama_pelanggan": fields.String(required=False, description="Nama pelanggan (untuk tambah baru)"),
    "kontak": fields.String(required=False, description="Kontak pelanggan (jika pelanggan baru)"),
    "total": fields.Integer(required=True, description="Total belanja"),
    "tunai": fields.Integer(required=True, description="Uang tunai"),
    "kembalian": fields.Integer(required=False, description="Kembalian (akan dihitung ulang jika tidak disediakan)"),
    "items": fields.List(fields.Nested(item_model), required=True, description="Daftar produk yang dibeli")
})

@transaksi_ns.route('/')
class TransaksiListResource(Resource):
    @transaksi_ns.doc(params={
        'id_pelanggan': 'Filter berdasarkan ID pelanggan',
        'tanggal': 'Filter berdasarkan tanggal transaksi (format: YYYY-MM-DD)',
        'status_hutang': 'Filter status hutang ("lunas" atau "belum lunas")',
        'id_lokasi': 'Filter berdasarkan ID lokasi'
    })
    @jwt_required()
    def get(self):
        """
        akses: admin, kasir; Ambil semua transaksi dengan optional filter
        """
        try:
            result = get_all_transaksi()
            if not result:
                return {'status': 'error', 'message': 'Tidak ada transaksi yang ditemukan'}, 401
            return result, 200
        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")
            return {'status': "Internal server error"}, 500
        
    @jwt_required()
    @transaksi_ns.expect(transaksi_model)
    def post(self):
        """akses: admin, kasir"""
        payload = request.get_json()
        try:
            new_transaksi = insert_transaksi(payload)
            if not new_transaksi:
                return {"status": "Gagal menambahkan transaksi"}, 401
            return {"data": new_transaksi, "status": "Transaksi berhasil ditambahkan"}, 201

        except ValueError as ve:
            return {"status": "error", "message": str(ve)}, 400
        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")
            return {'status': "Internal server error"}, 500


@transaksi_ns.route('/<int:id_transaksi>')
class TransaksiDetailResource(Resource):
    @jwt_required()
    def get(self, id_transaksi):
        """akses: admin, kasir"""
        try:
            transaksi = get_transaksi_by_id(id_transaksi)
            if not transaksi:
                return {'status': 'error', 'message': 'Tidak ada transaksi yang ditemukan'}, 401
            return {'data': transaksi}, 200
        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")
            return {'status': "Internal server error"}, 500