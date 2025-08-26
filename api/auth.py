from flask import logging, request, jsonify
from flask_restx import Namespace, Resource, fields
from flask_jwt_extended import get_jwt, jwt_required
from sqlalchemy.exc import SQLAlchemyError

from .query.q_auth import login_user
from .utils.blacklist_store import add_to_blacklist
from .utils.decorator import role_required

auth_ns = Namespace("auth", description="Auth related endpoints")

login_model = auth_ns.model("Login", {
    "username": fields.String(required=True),
    "password": fields.String(required=True)
})

# Endpoint untuk cek token valid (akses umum bagi semua yang login)
@auth_ns.route('/protected')
class ProtectedResource(Resource):
    @jwt_required()
    def get(self):
        """akses: admin, kasir"""
        return {'status': 'Token masih aktif'}, 200


# Login endpoint
@auth_ns.route('/login')
class LoginResource(Resource):
    @auth_ns.expect(login_model)
    def post(self):
        """akses: admin, kasir"""
        auth = request.get_json()
        username = auth.get('username')
        password = auth.get('password')

        if not username or not password:
            return {'status': "username dan password tidak boleh kosong"}, 400

        try:
            get_jwt_response = login_user(username, password)
            if get_jwt_response is None:
                return {'status': "Invalid username or password"}, 401
            return get_jwt_response, 200
        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")
            return {'status': "Internal server error"}, 500


# Logout endpoint - bisa diakses siapa pun yang login
@auth_ns.route('/logout')
class LogoutResource(Resource):
    @jwt_required()
    def post(self):
        """akses: admin, kasir"""
        jti = get_jwt().get('jti')
        if jti:
            add_to_blacklist(jti)
            return {'status': "Logout berhasil, token di-blacklist"}, 200
        return {'status': "JTI tidak ditemukan"}, 400


# Contoh endpoint khusus admin
@auth_ns.route('/admin-only')
class AdminOnlyResource(Resource):
    @role_required('admin')
    def get(self):
        """akses: admin"""
        return {'status': "Akses oleh admin berhasil"}, 200


# Contoh endpoint khusus kasir
@auth_ns.route('/kasir-only')
class KasirOnlyResource(Resource):
    @role_required('kasir')
    def get(self):
        """akses: kasir"""
        return {'status': "Akses oleh kasir berhasil"}, 200
