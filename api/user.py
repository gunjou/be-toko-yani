from flask import logging, request
from flask_restx import Namespace, Resource, fields
from flask_jwt_extended import jwt_required
from sqlalchemy.exc import SQLAlchemyError

from .utils.decorator import role_required
from .query.q_user import *

user_ns = Namespace("user", description="User related endpoints")

user_model = user_ns.model("User", {
    "id_lokasi": fields.Integer(required=False, description="ID Lokasi untuk kasir (admin = null)"),
    "username": fields.String(required=True, description="Username untuk user"),
    "password": fields.String(required=True, description="Password untuk user"),
    "role": fields.String(required=True, description="Role (admin/kasir)")
})

@user_ns.route('/')
class UserListResource(Resource):
    @jwt_required()
    def get(self):
        """akses: admin, kasir"""
        try:
            result = get_all_users()
            if not result:
                return {'status': 'error', 'message': 'Tidak ada user yang ditemukan'}, 401
            return {'data': result}, 200
        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")
            return {'status': "Internal server error"}, 500
        
    @role_required('admin')
    @user_ns.expect(user_model)
    def post(self):
        """akses: admin"""
        payload = request.get_json()
        try:
            new_user = insert_user(payload)
            if not new_user:
                return {"status": "Gagal menambahkan user"}, 401
            return {"data": new_user, "status": f"User {new_user['username']} berhasil ditambahkan"}, 201
        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")
            return {'status': "Internal server error"}, 500


@user_ns.route('/<int:id>')
class UserDetailResource(Resource):
    @jwt_required()
    def get(self, id):
        """akses: admin, kasir"""
        try:
            user = get_user_by_id(id)
            if not user:
                return {'status': 'error', 'message': 'Tidak ada user yang ditemukan'}, 401
            return {'data': user}, 200
        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")
            return {'status': "Internal server error"}, 500
        
    @role_required('admin')
    @user_ns.expect(user_model)
    def put(self, id):
        """akses: admin"""
        payload = request.get_json()
        try:
            updated = update_user(id, payload)
            if not updated:
                return {'status': 'error', "message": "User tidak ditemukan"}, 401
            return {"status": f"{updated[0]} berhasil diupdate"}, 200
        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")
            return {'status': "Internal server error"}, 500  
        
    @role_required('admin')
    def delete(self, id):
        """akses: admin"""
        try:
            deleted = delete_user(id)
            if not deleted:
                return {'status': 'error', "message": "User tidak ditemukan"}, 401
            return {"status": f"{deleted[0]} berhasil dihapus"}, 200
        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")
            return {'status': "Internal server error"}, 500 