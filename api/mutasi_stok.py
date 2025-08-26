from flask import logging, request
from flask_restx import Namespace, Resource, fields
from flask_jwt_extended import jwt_required
from sqlalchemy.exc import SQLAlchemyError

from .utils.decorator import role_required
from .query.q_mutasi_stok import *

mutasi_stok_ns = Namespace("mutasi-stok", description="mutasi stok related endpoints")

mutasi_stok_model = mutasi_stok_ns.model("MutasiStok", {
    "id_produk": fields.Integer(required=True, description="id barang"),
    "id_lokasi_asal": fields.Integer(required=True, description="lokasi asal stok"),
    "id_lokasi_tujuan": fields.Integer(required=True, description="lokasi tujuan stok"),
    "qty": fields.Integer(required=True, description="jumlah stok (quantity)"),
    "keterangan": fields.String(required=False, description="keterangan untuk produk"),
})

@mutasi_stok_ns.route('/')
class MutasiStokListResource(Resource):
    @role_required('admin')
    @mutasi_stok_ns.param("id_produk", "Filter berdasarkan ID produk", type="integer")
    @mutasi_stok_ns.param("id_lokasi_asal", "Filter berdasarkan lokasi asal", type="integer")
    @mutasi_stok_ns.param("id_lokasi_tujuan", "Filter berdasarkan lokasi tujuan", type="integer")
    @mutasi_stok_ns.param("tanggal_awal", "Tanggal awal dalam format YYYY-MM-DD", type="string")
    @mutasi_stok_ns.param("tanggal_akhir", "Tanggal akhir dalam format YYYY-MM-DD", type="string")
    def get(self):
        """
        akses: admin; ambil data stok dengan optional filter
        """
        filters = {
            "id_produk": request.args.get("id_produk", type=int),
            "id_lokasi_asal": request.args.get("id_lokasi_asal", type=int),
            "id_lokasi_tujuan": request.args.get("id_lokasi_tujuan", type=int),
            "tanggal_awal": request.args.get("tanggal_awal"),
            "tanggal_akhir": request.args.get("tanggal_akhir")
        }

        try:
            data = get_all_mutasi_stok(filters)
            if not data:
                return {"status": "error", "message": "Tidak ada mutasi stok ditemukan"}, 404
            return {"status": "success", "data": data}, 200
        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")
            return {"status": "error", "message": "Internal server error"}, 500
        
    @role_required('admin')
    @mutasi_stok_ns.expect(mutasi_stok_model)
    def post(self):
        """akses: admin"""
        payload = request.get_json()
        try:
            result = insert_mutasi_stok(payload)

            if not result["success"]:
                return {"status": result["error"]}, 400

            data = result["data"]
            return {
                "data": data,
                "status": f"Mutasi {data['nama_produk']} sebanyak {data['jumlah']} {data['satuan']} dari {data['lokasi_asal']} ke {data['lokasi_tujuan']} berhasil"
            }, 201

        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")
            return {'status': "Internal server error"}, 500


