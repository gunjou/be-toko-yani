from flask import logging, request
from flask_restx import Namespace, Resource, fields
from flask_jwt_extended import jwt_required
from sqlalchemy.exc import SQLAlchemyError

from .utils.decorator import role_required
from .query.q_lokasi import *

lokasi_ns = Namespace("lokasi", description="Lokasi related endpoints")

lokasi_model = lokasi_ns.model("Lokasi", {
    "nama_lokasi": fields.String(required=True, description="Nama Lokasi untuk cabang"),
    "tipe": fields.String(required=True, description="Tipe (Toko/Gudang)"),
})

@lokasi_ns.route('/')
class LokasiListResource(Resource):
    @jwt_required()
    def get(self):
        """akses: admin, kasir"""
        try:
            result = get_all_lokasi()
            if not result:
                return {'status': 'error', 'message': 'Tidak ada lokasi yang ditemukan'}, 401
            return result, 200
        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")
            return {'status': "Internal server error"}, 500
        
    @role_required('admin')
    @lokasi_ns.expect(lokasi_model)
    def post(self):
        """akses: admin"""
        payload = request.get_json()
        try:
            new_lokasi = insert_lokasi(payload)
            if not new_lokasi:
                return {"status": "Gagal menambahkan lokasi"}, 401
            return {"data": new_lokasi, "status": f"Lokasi {new_lokasi['nama_lokasi']} berhasil ditambahkan"}, 201
        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")
            return {'status': "Internal server error"}, 500


@lokasi_ns.route('/<int:id>')
class LokasiDetailResource(Resource):
    @jwt_required()
    def get(self, id):
        """akses: admin, kasir"""
        try:
            lokasi = get_lokasi_by_id(id)
            if not lokasi:
                return {'status': 'error', 'message': 'Tidak ada lokasi yang ditemukan'}, 401
            return {'data': lokasi}, 200
        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")
            return {'status': "Internal server error"}, 500
        
    @role_required('admin')
    @lokasi_ns.expect(lokasi_model)
    def put(self, id):
        """akses: admin"""
        payload = request.get_json()
        try:
            updated = update_lokasi(id, payload)
            if not updated:
                return {'status': 'error', "message": "Lokasi tidak ditemukan"}, 401
            return {"status": f"{updated[0]} berhasil diupdate"}, 200
        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")
            return {'status': "Internal server error"}, 500  
        
    @role_required('admin')
    def delete(self, id):
        """akses: admin"""
        try:
            deleted = delete_lokasi(id)
            if not deleted:
                return {'status': 'error', "message": "Lokasi tidak ditemukan"}, 401
            return {"status": f"{deleted[0]} berhasil dihapus"}, 200
        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")
            return {'status': "Internal server error"}, 500 
        