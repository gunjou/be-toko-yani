# api/pelanggan.py
from flask import logging, request
from flask_restx import Namespace, Resource, fields
from flask_jwt_extended import jwt_required
from sqlalchemy.exc import SQLAlchemyError

from .query.q_pelanggan import *

pelanggan_ns = Namespace("pelanggan", description="Pelanggan related endpoints")


pelanggan_model = pelanggan_ns.model("Pelanggan", {
    "nama_pelanggan": fields.String(
        required=True,
        description="Nama Pelanggan"
    ),
    "kontak": fields.String(
        required=False,
        description="Kontak (nomor hp)"
    ),
    "alamat": fields.String(
        required=False,
        description="Alamat"
    ),
})

redeem_poin_model = pelanggan_ns.model("RedeemPoin", {

    "id_reward": fields.Integer(
        required=True,
        description="ID reward poin"
    ),

    "qty": fields.Integer(
        required=True,
        description="Jumlah reward"
    )
})


@pelanggan_ns.route('/')
class PelangganListResource(Resource):

    @jwt_required()
    def get(self):
        """akses: admin, kasir"""

        try:

            result = get_all_pelanggan()

            if not result:
                return {
                    'status': 'error',
                    'message': 'Tidak ada pelanggan yang ditemukan'
                }, 404

            return {
                'data': result
            }, 200

        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")

            return {
                'status': 'Internal server error'
            }, 500

    @jwt_required()
    @pelanggan_ns.expect(pelanggan_model)
    def post(self):
        """akses: admin, kasir"""

        payload = request.get_json()

        try:

            new_pelanggan = insert_pelanggan(payload)

            if not new_pelanggan:
                return {
                    "status": "error",
                    "message": "Gagal menambahkan pelanggan"
                }, 400

            return {
                "data": new_pelanggan,
                "status": f"Pelanggan {new_pelanggan['nama_pelanggan']} berhasil ditambahkan"
            }, 201

        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")

            return {
                'status': "Internal server error"
            }, 500


@pelanggan_ns.route('/<int:id>')
class PelangganDetailResource(Resource):

    @jwt_required()
    def get(self, id):
        """akses: admin, kasir"""

        try:

            pelanggan = get_pelanggan_by_id(id)

            if not pelanggan:
                return {
                    'status': 'error',
                    'message': 'Pelanggan tidak ditemukan'
                }, 404

            return {
                'data': pelanggan
            }, 200

        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")

            return {
                'status': "Internal server error"
            }, 500

    @jwt_required()
    @pelanggan_ns.expect(pelanggan_model)
    def put(self, id):
        """akses: admin, kasir"""

        payload = request.get_json()

        try:

            updated = update_pelanggan(id, payload)

            if not updated:
                return {
                    'status': 'error',
                    "message": "Pelanggan tidak ditemukan"
                }, 404

            return {
                "status": f"{updated['nama_pelanggan']} berhasil diupdate"
            }, 200

        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")

            return {
                'status': "Internal server error"
            }, 500

    @jwt_required()
    def delete(self, id):
        """akses: admin, kasir"""

        try:

            deleted = delete_pelanggan(id)

            if not deleted:
                return {
                    'status': 'error',
                    "message": "Pelanggan tidak ditemukan"
                }, 404

            return {
                "status": f"{deleted['nama_pelanggan']} berhasil dihapus"
            }, 200

        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")

            return {
                'status': "Internal server error"
            }, 500


# =========================================================
# GET TOTAL POIN PELANGGAN
# =========================================================

@pelanggan_ns.route('/<int:id>/poin')
class PelangganPoinResource(Resource):

    @jwt_required()
    def get(self, id):
        """akses: admin, kasir"""

        try:

            pelanggan = get_poin_pelanggan(id)

            if not pelanggan:
                return {
                    'status': 'error',
                    'message': 'Pelanggan tidak ditemukan'
                }, 404

            return {
                'data': pelanggan
            }, 200

        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")

            return {
                'status': "Internal server error"
            }, 500


# =========================================================
# GET HISTORI POIN
# =========================================================

@pelanggan_ns.route('/<int:id>/histori-poin')
class PelangganHistoriPoinResource(Resource):

    @jwt_required()
    def get(self, id):
        """akses: admin, kasir"""

        try:

            histori = get_histori_poin_pelanggan(id)

            return {
                'data': histori
            }, 200

        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")

            return {
                'status': "Internal server error"
            }, 500


@pelanggan_ns.route('/<int:id>/redeem-poin')
class PelangganRedeemPoinResource(Resource):

    @jwt_required()
    @pelanggan_ns.expect(redeem_poin_model)
    def post(self, id):
        """
        akses: admin, kasir
        """

        payload = request.get_json()

        try:

            redeemed = redeem_poin(id, payload)

            if not redeemed:

                return {
                    "status": "error",
                    "message": "Gagal redeem poin"
                }, 400

            return {
                "data": redeemed,
                "status": "Redeem poin berhasil"
            }, 200

        except ValueError as ve:

            return {
                "status": "error",
                "message": str(ve)
            }, 400

        except SQLAlchemyError as e:

            logging.error(f"Database error: {str(e)}")

            return {
                'status': "Internal server error"
            }, 500