# app/routes.py
from flask import Blueprint, request, jsonify
from app.db_route_handler import get_db_handler

routes_bp = Blueprint('routes', __name__)
db_handler = get_db_handler()

@routes_bp.route('/bank1-json-insert', methods=['POST'])
def bank_json_insert():
    try:
        payload = request.get_json()
        data = payload.get('data', [])
        if not data:
            return jsonify({'status': 'error', 'message': 'No data provided'}), 400

        db_handler.insert_or_update(
            data=data,
            main_table="bank_json",
            history_table="bank_json_history",
            pk="MsgId"
        )

        return jsonify({'status': 'success', 'message': 'Data inserted successfully'}), 200
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500
