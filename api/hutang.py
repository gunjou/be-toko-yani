from flask import logging, request
from flask_restx import Namespace, Resource, fields
from flask_jwt_extended import jwt_required
from sqlalchemy.exc import SQLAlchemyError

from .query.q_hutang import *

hutang_ns = Namespace("hutang", description="Hutang related endpoints")

hutang_model = hutang_ns.model("Hutang", {
    "id_transaksi": fields.Integer(required=False, description="id transaksi"),
    "id_pelanggan": fields.Integer(required=True, description="id pelanggan"),
    "sisa_hutang": fields.Integer(required=True, description="sisa hutang"),
    "status_hutang": fields.String(required=True, description="status hutang"),
})

@hutang_ns.route('/')
class HutangListResource(Resource):
    @jwt_required()
    def get(self):
        """akses: admin, kasir"""
        try:
            result = get_all_hutang()
            if not result:
                return {'status': 'error', 'message': 'Tidak ada hutang yang ditemukan'}, 401
            return result, 200
        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")
            return {'status': "Internal server error"}, 500
        
    @jwt_required()
    @hutang_ns.expect(hutang_model)
    def post(self):
        """akses: admin, kasir"""
        payload = request.get_json()
        payload["status_hutang"] = payload.get("status_hutang", "").lower()

        try:
            new_hutang = insert_hutang(payload)
            if not new_hutang:
                return {"status": "Gagal menambahkan hutang"}, 401
            return {"data": new_hutang, "status": f"Hutang {new_hutang['sisa_hutang']} {new_hutang['status_hutang']} berhasil ditambahkan"}, 201
        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")
            return {'status': "Internal server error"}, 500


@hutang_ns.route('/<int:id_hutang>')
class HutangDetailResource(Resource):
    @jwt_required()
    def get(self, id_hutang):
        """akses: admin, kasir"""
        try:
            hutang = get_hutang_by_id(id_hutang)
            if not hutang:
                return {'status': 'error', 'message': 'Tidak ada hutang yang ditemukan'}, 401
            return {'data': hutang}, 200
        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")
            return {'status': "Internal server error"}, 500
        
    @jwt_required()
    @hutang_ns.expect(hutang_model)
    def put(self, id_hutang):
        """akses: admin, kasir"""
        payload = request.get_json()
        try:
            updated = update_hutang(id_hutang, payload)
            if not updated:
                return {'status': 'error', "message": "Hutang tidak ditemukan"}, 401
            return {"status": f"Hutang {updated[0]} {updated[1]} berhasil diupdate"}, 200
        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")
            return {'status': "Internal server error"}, 500  
        
    @jwt_required()
    def delete(self, id_hutang):
        """akses: admin, kasir"""
        try:
            deleted = delete_hutang(id_hutang)
            if not deleted:
                return {'status': 'error', "message": "Hutang tidak ditemukan"}, 401
            return {"status": f"Hutang {deleted[0]} berhasil dihapus"}, 200
        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")
            return {'status': "Internal server error"}, 500 
        

@hutang_ns.route('/total')
class HutangListTotalResource(Resource):
    @jwt_required()
    @hutang_ns.param("id_pelanggan", "Filter berdasarkan ID pelanggan", type="integer")
    def get(self):
        """
        akses: admin, kasir; daftar total hutang per pelanggan
        """
        id_pelanggan = request.args.get("id_pelanggan", type=int)
        try:
            data = get_total_hutang_per_pelanggan(id_pelanggan)
            if not data:
                return {'status': 'error', 'message': 'Tidak ada hutang yang ditemukan'}, 404
            return {'status': 'success', 'data': data}, 200
        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")
            return {'status': "Internal server error"}, 500


@hutang_ns.route('/total/<int:id_pelanggan>')
class HutangTotalPerPelangganResource(Resource):
    @jwt_required()
    def get(self, id_pelanggan):
        """akses: admin, kasir; total hutang per pelanggan berdasarkan id"""
        try:
            total = count_total_hutang_by_id(id_pelanggan)
            return {'data': total}, 200
        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")
            return {'status': "Internal server error"}, 500
        

@hutang_ns.route('/bayar')
class BayarHutangResource(Resource):
    @jwt_required()
    @hutang_ns.doc(description="Bayar hutang pelanggan (sebagian atau lunas)")
    @hutang_ns.expect(hutang_ns.model("PembayaranHutang", {
        "id_pelanggan": fields.Integer(required=True, description="ID pelanggan"),
        "jumlah_bayar": fields.Integer(required=True, description="Jumlah yang dibayarkan")
    }))
    def post(self):
        payload = request.get_json()
        try:
            id_pelanggan = payload.get("id_pelanggan")
            jumlah_bayar = payload.get("jumlah_bayar")
            hasil = bayar_hutang(id_pelanggan, jumlah_bayar)

            if not hasil:
                return {"status": "error", "message": "Pembayaran gagal atau tidak ada hutang aktif"}, 400

            return {"status": "success", "message": f"Hutang berhasil dibayar sebagian/lunas", "detail": hasil}, 200
        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")
            return {"status": "Internal server error"}, 500
