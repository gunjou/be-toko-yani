from flask import logging, request
from flask_restx import Namespace, Resource, fields
from flask_jwt_extended import jwt_required
from sqlalchemy.exc import SQLAlchemyError

from .utils.decorator import role_required
from .query.q_produk import *

produk_ns = Namespace("produk", description="Produk related endpoints")

produk_model = produk_ns.model("Produk", {
    "nama_produk": fields.String(required=True, description="Nama untuk produk"),
    "barcode": fields.String(required=True, description="Barcode untuk produk"),
    "kategori": fields.String(required=True, description="Kategori produk"),
    "satuan": fields.String(required=True, description="Satuan produk"),
    "harga_beli": fields.Integer(required=False, description="Harga beli produk (modal)"),
    "harga_jual": fields.Integer(required=False, description="Harga jual produk"),
    "expired_date": fields.String(required=False, description="Tanggal kedaluwarsa produk (DD-MM-YYYY)"),
    "stok_optimal": fields.Integer(required=False, description="Batas stok optimal untuk produk"),
})

@produk_ns.route('/')
class ProdukListResource(Resource):
    @jwt_required()
    def get(self):
        """akses: admin, kasir"""
        try:
            all_produk = get_all_produk()
            if not all_produk:
                return {'status': 'error', 'message': 'Tidak ada produk yang ditemukan'}, 401
            return {'data': all_produk}, 200
        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")
            return {'status': "Internal server error"}, 500
        
    # @jwt_required()
    # @produk_ns.expect(produk_model)
    # def post(self):
    #     payload = request.get_json()
    #     try:
    #         new_produk = insert_produk(payload)
    #         if not new_produk:
    #             return {"status": "Gagal menambahkan produk"}, 401
    #         return {"data": new_produk, "status": f"Produk {new_produk['nama_produk']} berhasil ditambahkan"}, 201
    #     except SQLAlchemyError as e:
    #         logging.error(f"Database error: {str(e)}")
    #         return {'status': "Internal server error"}, 500


@produk_ns.route('/<int:id>')
class ProdukDetailResource(Resource):
    @jwt_required()
    def get(self, id):
        """akses: admin, kasir"""
        try:
            produk = get_produk_by_id(id)
            if not produk:
                return {'status': 'error', 'message': 'Tidak ada produk yang ditemukan'}, 401
            return {'data': produk}, 200
        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")
            return {'status': "Internal server error"}, 500
        
    @role_required('admin')
    @produk_ns.expect(produk_model)
    def put(self, id):
        """akses: admin"""
        payload = request.get_json()
        try:
            updated = update_produk(id, payload)
            if not updated:
                return {'status': 'error', "message": "Produk tidak ditemukan"}, 401
            return {"status": f"{updated[0]} berhasil diupdate"}, 200
        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")
            return {'status': "Internal server error"}, 500  
        
    @role_required('admin')
    def delete(self, id):
        """akses: admin"""
        try:
            deleted = delete_produk(id)
            if not deleted:
                return {'status': 'error', "message": "Produk tidak ditemukan"}, 401
            return {"status": f"{deleted[0]} berhasil dihapus"}, 200
        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")
            return {'status': "Internal server error"}, 500 