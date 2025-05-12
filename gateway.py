from flask import Flask, request, jsonify
from datetime import timedelta, datetime
import os
import redis
import logging

# JWT
from flask_jwt_extended import (
    JWTManager, create_access_token,
    jwt_required, get_jwt_identity
)

from Lab1 import LectureMaterialSearcher, AttendanceFinder 
import neo4j_sync

logging.basicConfig(level=logging.DEBUG)

app = Flask(__name__)

app.config['JWT_SECRET_KEY'] = os.getenv('JWT_SECRET_KEY', 'super-secret-key')
app.config['JWT_ACCESS_TOKEN_EXPIRES'] = timedelta(hours=1)
jwt = JWTManager(app)

HARDCODED_USER = {
    'username': 'user',
    'password': 'user'
}

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "strongpassword")
ES_HOST = os.getenv("ES_HOST", "localhost")
ES_PORT = int(os.getenv("ES_PORT", 9200))
ES_USER = os.getenv("ES_USER", "elastic")
ES_PASS = os.getenv("ES_PASS", "secret")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
PG_CONFIG = {
    'dbname': os.getenv("POSTGRES_DB", "postgres_db"),
    'user': os.getenv("POSTGRES_USER", "postgres_user"),
    'password': os.getenv("POSTGRES_PASSWORD", "postgres_password"),
    'host': os.getenv("POSTGRES_HOST", "localhost"),
    'port': os.getenv("POSTGRES_PORT", 5430),
}

@app.route('/api/auth/login', methods=['POST'])
def login():
    if not request.is_json:
        return jsonify({'msg': 'Missing JSON in request'}), 400

    username = request.json.get('username', None)
    password = request.json.get('password', None)
    if not username or not password:
        return jsonify({'msg': 'Нужно указать имя пользователя и пароль'}), 400

    if username != HARDCODED_USER['username'] or password != HARDCODED_USER['password']:
        return jsonify({'msg': 'Неверные учетные данные'}), 401

    access_token = create_access_token(identity=username)
    return jsonify(access_token=access_token), 200


from datetime import datetime

def is_valid_date(date_str: str) -> bool:
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False
    
@app.route('/api/lab1/report', methods=['POST'])
@jwt_required()
def generate_attendance_report():
    current_user = get_jwt_identity()
    app.logger.debug(f"Запрос отчета от пользователя: {current_user}")

    if not request.is_json:
        return jsonify({'error': 'Request must be JSON'}), 400

    data = request.get_json()
    required_fields = ['term', 'start_date', 'end_date']
    if not all(field in data for field in required_fields):
        return jsonify({
            'error': f"Missing required fields: {required_fields}",
            'received': list(data.keys())
        }), 400

    # Поиск лекций в ElasticSearch
    es_searcher = LectureMaterialSearcher(
        es_host=ES_HOST,
        es_port=ES_PORT,
        es_user=ES_USER,
        es_password=ES_PASS
    )
    lecture_ids = es_searcher.search(data['term'])
    if not lecture_ids:
        return jsonify({'error': 'No lectures found for the term'}), 404

    # Поиск посещаемости в Neo4j
    finder = AttendanceFinder(
        uri=NEO4J_URI,
        user=NEO4J_USER,
        password=NEO4J_PASSWORD
    )
    redis_conn = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    try:
        worst = finder.find_worst_attendees(
            lecture_ids,
            top_n=10,
            start_date=data['start_date'],
            end_date=data['end_date']
        )

        def format_student(record):
            redis_info = redis_conn.hgetall(f"student:{record['studentId']}")
            return {
                **record,
                'redis_info': {
                    'name': redis_info.get('name'),
                    'age': redis_info.get('age'),
                    'mail': redis_info.get('mail'),
                    'group': redis_info.get('group')
                }
            }

        report = {
            'search_term': data['term'],
            'period': f"{data['start_date']} - {data['end_date']}",
            'found_lectures': len(lecture_ids),
            'worst_attendees': [format_student(r) for r in worst]
        }
        return jsonify(report=report, meta={'status': 'success', 'results': len(worst)}), 200

    except Exception as e:
        app.logger.error(f"Error: {e}")
        return jsonify({'error': 'Data processing failed'}), 500

    finally:
        finder.close()
        redis_conn.close()

@app.route('/api/lab2/audience_report', methods=['POST'])
@jwt_required()
def get_audience_report():
    data = request.get_json(force=True)
    year = data.get('year')
    semester = data.get('semester')
    if year is None or semester is None:
        return jsonify({'error': 'Required fields: year, semester'}), 400
    try:
        service = neo4j_sync.SyncService(PG_CONFIG, NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
        report = service.generate_audience_report(year=year, semester=semester)
        return jsonify(report=report, meta={'status': 'success', 'count': len(report)}), 200
    except Exception as e:
        app.logger.error(f"Audience report error: {e}")
        return jsonify({'error': 'Failed to generate audience report'}), 500
    finally:
        try: service.close()
        except: pass

@app.route('/api/lab3/group_report', methods=['POST'])
@jwt_required()
def get_group_report():
    data = request.get_json(force=True)
    group_id = data.get('group_id')
    if group_id is None:
        return jsonify({'error': 'Required field: group_id'}), 400
    try:
        service = neo4j_sync.SyncService(PG_CONFIG, NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
        report = service.generate_group_report(group_id=group_id)
        return jsonify(report=report, meta={'status': 'success', 'group_id': group_id, 'count': len(report)}), 200
    except Exception as e:
        app.logger.error(f"Group report error: {e}")
        return jsonify({'error': 'Failed to generate group report'}), 500
    finally:
        try: service.close()
        except: pass

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
