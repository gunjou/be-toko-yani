# api/reward_poin.py

from flask import logging, request

from flask_restx import Namespace, Resource, fields

from flask_jwt_extended import jwt_required

from sqlalchemy.exc import SQLAlchemyError

from .query.q_reward_poin import *


reward_poin_ns = Namespace(
    "reward-poin",
    description="Reward poin related endpoints"
)


# =========================================================
# MODEL
# =========================================================

reward_poin_model = reward_poin_ns.model("RewardPoin", {

    "id_produk": fields.Integer(
        required=True,
        description="ID produk"
    ),

    "poin_required": fields.Integer(
        required=True,
        description="Jumlah poin yang dibutuhkan"
    )
})


# =========================================================
# LIST REWARD
# =========================================================

@reward_poin_ns.route('/')
class RewardPoinListResource(Resource):

    @jwt_required()
    def get(self):
        """
        akses: admin, kasir
        """

        try:

            result = get_all_reward_poin()

            if not result:

                return {
                    'status': 'error',
                    'message': 'Tidak ada reward poin ditemukan'
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
    @reward_poin_ns.expect(reward_poin_model)
    def post(self):
        """
        akses: admin
        """

        payload = request.get_json()

        try:

            new_reward = insert_reward_poin(payload)

            if not new_reward:

                return {
                    "status": "error",
                    "message": "Gagal menambahkan reward poin"
                }, 400

            return {
                "data": new_reward,
                "status": "Reward poin berhasil ditambahkan"
            }, 201

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


# =========================================================
# DETAIL REWARD
# =========================================================

@reward_poin_ns.route('/<int:id_reward>')
class RewardPoinDetailResource(Resource):

    @jwt_required()
    def get(self, id_reward):
        """
        akses: admin, kasir
        """

        try:

            reward = get_reward_poin_by_id(id_reward)

            if not reward:

                return {
                    'status': 'error',
                    'message': 'Reward poin tidak ditemukan'
                }, 404

            return {
                'data': reward
            }, 200

        except SQLAlchemyError as e:

            logging.error(f"Database error: {str(e)}")

            return {
                'status': "Internal server error"
            }, 500

    @jwt_required()
    @reward_poin_ns.expect(reward_poin_model)
    def put(self, id_reward):
        """
        akses: admin
        """

        payload = request.get_json()

        try:

            updated = update_reward_poin(id_reward, payload)

            if not updated:

                return {
                    'status': 'error',
                    'message': 'Reward poin tidak ditemukan'
                }, 404

            return {
                "status": f"Reward poin {updated['nama_produk']} berhasil diupdate"
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

    @jwt_required()
    def delete(self, id_reward):
        """
        akses: admin
        """

        try:

            deleted = delete_reward_poin(id_reward)

            if not deleted:

                return {
                    'status': 'error',
                    'message': 'Reward poin tidak ditemukan'
                }, 404

            return {
                "status": f"Reward poin {deleted['nama_produk']} berhasil dihapus"
            }, 200

        except SQLAlchemyError as e:

            logging.error(f"Database error: {str(e)}")

            return {
                'status': "Internal server error"
            }, 500