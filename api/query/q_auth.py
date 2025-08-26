from flask_jwt_extended import create_access_token
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from ..utils.config import get_connection


def login_user(username, password):
    engine = get_connection()
    try:
        with engine.connect() as connection:
            if username == 'admin':
                result = connection.execute(
                    text("""
                        SELECT * 
                        FROM users
                        WHERE username = :username 
                        AND password = :password;
                    """),
                    {"username": username, "password": password}
                ).mappings().fetchone()

                if not result:
                    return None

                access_token = create_access_token(
                    identity=str(result['id_user']),
                    additional_claims={
                        "role": result['role'],
                        "nama": "admin"
                    }
                )

                return {
                    'access_token': access_token,
                    'message': 'login success',
                    'id_user': result['id_user'],
                    'role': result['role'],
                    'id_lokasi': result['id_lokasi'],
                    'nama': 'admin'
                }

            else:
                result = connection.execute(
                    text("""
                        SELECT u.id_user, u.id_lokasi, u.username, u.role, l.nama_lokasi 
                        FROM users u 
                        INNER JOIN lokasi l ON u.id_lokasi = l.id_lokasi 
                        WHERE u.username = :username 
                        AND u.password = :password;
                    """),
                    {"username": username, "password": password}
                ).mappings().fetchone()

                if not result:
                    return None

                access_token = create_access_token(
                    identity=str(result['id_user']),
                    additional_claims={
                        "role": result['role'],
                        "nama": result['username']
                    }
                )

                return {
                    'access_token': access_token,
                    'message': 'login success',
                    'id_user': result['id_user'],
                    'id_kasir': result['id_user'],
                    'role': result['role'],
                    'id_lokasi': result['id_lokasi'],
                    'nama': result['username']
                }
            
    except SQLAlchemyError as e:
        print(f"Error occurred: {str(e)}")
        return {'msg': 'Internal server error'}
