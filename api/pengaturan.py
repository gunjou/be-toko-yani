# api/pengaturan.py

from flask import logging, request

from flask_restx import Namespace, Resource, fields

from flask_jwt_extended import jwt_required

from sqlalchemy.exc import SQLAlchemyError

from .query.q_pengaturan import *


pengaturan_ns = Namespace(
    "pengaturan",
    description="Pengaturan related endpoints"
)


# =========================================================
# MODEL
# =========================================================

pengaturan_model = pengaturan_ns.model("Pengaturan", {

    "poin_kelipatan": fields.Integer(
        required=True,
        description="Nominal belanja untuk mendapatkan 1 poin"
    )
})


# =========================================================
# GET & UPDATE PENGATURAN
# =========================================================

@pengaturan_ns.route('/')
class PengaturanResource(Resource):

    @jwt_required()
    def get(self):
        """
        akses: admin
        """

        try:

            result = get_pengaturan_poin()

            if not result:

                return {
                    'status': 'error',
                    'message': 'Pengaturan tidak ditemukan'
                }, 404

            return {
                'data': result
            }, 200

        except SQLAlchemyError as e:

            logging.error(f"Database error: {str(e)}")

            return {
                'status': "Internal server error"
            }, 500

    @jwt_required()
    @pengaturan_ns.expect(pengaturan_model)
    def put(self):
        """
        akses: admin
        """

        payload = request.get_json()

        try:

            updated = update_pengaturan_poin(payload)

            if not updated:

                return {
                    'status': 'error',
                    'message': 'Gagal update pengaturan'
                }, 400

            return {
                'data': updated,
                'status': 'Pengaturan berhasil diupdate'
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