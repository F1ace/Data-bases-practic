from datetime import datetime
from typing import List, Dict
from elasticsearch import Elasticsearch
import redis
from neo4j import GraphDatabase
from elastic_gen_sync import LectureMaterialSearcher
from redis_sync import StudentSearch

ES_HOST = 'localhost'
ES_PORT = 9200
ES_USER = 'elastic'
ES_PASS = 'secret'

class AttendanceReporter:
    def __init__(self, neo4j_driver, es_searcher, student_search):
        self.driver = neo4j_driver
        self.es_searcher = es_searcher
        self.student_search = student_search

    def generate_report(self, term: str, start_date: datetime, end_date: datetime) -> List[Dict]:
        es = Elasticsearch(
            hosts=[f"http://{ES_HOST}:{ES_PORT}"],
            basic_auth=(ES_USER, ES_PASS),
            verify_certs=False
        )
        es_resp = es.search(
                index="lecture_materials",
                query={
                    "multi_match": {
                        "query": term,
                        "fields": ["lecture_name^3", "course_name^2", "content", "keywords"],
                        "type": "best_fields",
                        "fuzziness": "AUTO"
                    }
                },
                size=100
            )

        lecture_ids = [hit['_source']['lecture_id'] for hit in es_resp['hits']['hits']]

        # Step 1: Search lectures containing the term using Elasticsearch
        print(lecture_ids)

        if not lecture_ids:
            return []

        # Step 2: Query Neo4j to calculate attendance percentages
        cypher_query = """
// 1. Студенты в группах
MATCH (s:Student)-[:MEMBER_OF]->(g:Group)

// 2. Сессии этих групп и связанные лекции
MATCH (ss:Session)-[:FOR_GROUP]->(g)
MATCH (l:Lecture)-[:HAS_SESSION]->(ss)

// 3. Фильтрация по списку ID лекций и по диапазону дат (без учёта времени)
WHERE l.id IN $lecture_ids
  AND date(datetime(ss.date)) >= date($start_date)
  AND date(datetime(ss.date)) <= date($end_date)

// 4. Опционально — факт посещения студентом каждой сессии
OPTIONAL MATCH (s)-[a:ATTENDANCE]->(ss)

// 5. Агрегация: общее число сессий и число посещённых
WITH 
  s,
  COUNT(DISTINCT ss) AS total_sessions,
  SUM(
    CASE WHEN a.attended = true THEN 1 
         ELSE 0 
    END
  ) AS attended_sessions

// 6. Только студенты с хотя бы одной сессией
WHERE total_sessions > 0

// 7. Расчёт процента и сортировка
RETURN 
  s.id AS student_id,
  100.0 * attended_sessions / total_sessions AS attendance_percent
ORDER BY attendance_percent ASC
LIMIT 10;
        """

        with self.driver.session() as session:
            result = session.run(
                cypher_query,
                lecture_ids=lecture_ids,
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d")
            )
            student_records = [
                {
                    "student_id": record["student_id"],
                    "attendance_percent": round(record["attendance_percent"], 2)
                }
                for record in result
            ]
            print(student_records)

        report = []
        for record in student_records:
            student_info = self.student_search.get_student_full(record["student_id"])
            if student_info:
                report.append({
                    "student_info": student_info,
                    "attendance_percent": record["attendance_percent"],
                    "report_period": {
                        "start": start_date.strftime("%Y-%m-%d"),
                        "end": end_date.strftime("%Y-%m-%d")
                    },
                    "search_term": term
                })

        return report
    
if __name__ == "__main__":
    neo4j_driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "strongpassword"))
    es_searcher = LectureMaterialSearcher()
    student_search = StudentSearch()

    # Создание репортера
    reporter = AttendanceReporter(neo4j_driver, es_searcher, student_search)

    # Генерация отчета
    report = reporter.generate_report(
        term="системы",
        start_date=datetime(2023, 1, 1),
        end_date=datetime(2023, 12, 31)
    )

    # Вывод результатов
    for entry in report:
        print(f"Student: {entry['student_info']}")
        print(f"Attendance: {entry['attendance_percent']}%")
        print(f"Period: {entry['report_period']['start']} - {entry['report_period']['end']}")
        print(f"Term: {entry['search_term']}\n")