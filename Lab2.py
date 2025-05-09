from neo4j import GraphDatabase
from datetime import datetime
import neo4j_sync

PG_CONFIG = {
    'dbname': "postgres_db",
    'user': "postgres_user",
    'password': "postgres_password",
    'host': 'localhost',
    'port': 5430,
}

NEO4J_URI = 'bolt://localhost:7687'
NEO4J_USER = 'neo4j'
NEO4J_PASSWORD = 'strongpassword'

if __name__ == '__main__':
    service = neo4j_sync.SyncService(PG_CONFIG, NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    try:
        report = service.generate_audience_report(year=2023, semester=1)

        for entry in report:
            print(f"Курс: {entry['course_name']}")
            print(f"Лекция: {entry['lecture_name']}")
            print(f"Требования: {', '.join(entry['tech_requirements'])}")
            print(f"Студентов: {entry['total_students']}\n")
    finally:
        service.close()