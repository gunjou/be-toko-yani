from flask import logging, request
from flask_restx import Namespace, Resource, fields
from flask_jwt_extended import jwt_required
from sqlalchemy.exc import SQLAlchemyError

from .query.q_stok import *

stok_ns = Namespace("stok", description="stok related endpoints")

stok_model = stok_ns.model("Stok", {
    "id_produk": fields.Integer(required=False, description="id barang"),
    "id_lokasi": fields.Integer(required=True, description="lokasi stok"),
    "jumlah": fields.Integer(required=True, description="jumlah stok"),
    "nama_produk": fields.String(required=True, description="Nama untuk produk"),
    "barcode": fields.String(required=True, description="Barcode untuk produk"),
    "kategori": fields.String(required=True, description="Kategori produk"),
    "satuan": fields.String(required=True, description="Satuan produk"),
    "expired_date": fields.String(required=False, description="Tanggal kedaluwarsa produk (DD-MM-YYYY)"),
    "stok_optimal": fields.Integer(required=False, description="Batas stok optimal untuk produk"),
})

@stok_ns.route('/')
class StokListResource(Resource):
    @jwt_required()
    @stok_ns.param("id_lokasi", "ID Lokasi untuk filter stok", type="integer")
    def get(self):
        """
        akses: admin, kasir; ambil data stok dengan optional filter
        """
        id_lokasi = request.args.get("id_lokasi", type=int)
        try:
            all_stok = get_all_stok(id_lokasi)
            if not all_stok:
                return {'status': 'error', 'message': 'Tidak ada stok yang ditemukan'}, 401
            return {'data': all_stok}, 200
        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")
            return {'status': "Internal server error"}, 500
        
    @jwt_required()
    @stok_ns.expect(stok_model)
    def post(self):
        """akses: admin, kasir"""
        payload = request.get_json()
        try:
            new_stok = insert_stok(payload)
            if not new_stok:
                return {"status": "Gagal menambahkan stok"}, 401
            return {
                "data": new_stok, 
                "status": f"{new_stok['nama_produk']} dengan stok {new_stok['jumlah']} {new_stok['satuan']} berhasil ditambahkan"
                }, 201
        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")
            return {'status': "Internal server error"}, 500
        

@stok_ns.route('/<int:id_stok>')
class StokDetailResource(Resource):
    @jwt_required()
    @stok_ns.expect(stok_model)
    def put(self, id_stok):
        """akses: admin, kasir"""
        payload = request.get_json()
        try:
            updated = update_stok(id_stok, payload)
            if not updated:
                return {'status': 'error', "message": "Stok tidak ditemukan"}, 404
            return {
                "status": f"{updated['nama_produk']} berhasil diupdate",
                "data": updated
            }, 200
        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")
            return {'status': "Internal server error"}, 500
        
    @jwt_required()
    def delete(self, id_stok):
        """akses: admin, kasir"""
        try:
            deleted = delete_stok(id_stok)
            if not deleted:
                return {'status': 'error', "message": "Stok tidak ditemukan atau sudah dihapus"}, 404
            return {"status": f"{deleted} berhasil dihapus"}, 200
        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")
            return {'status': "Internal server error"}, 500 