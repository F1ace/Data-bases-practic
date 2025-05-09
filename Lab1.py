from elasticsearch import Elasticsearch
from neo4j import GraphDatabase
from typing import List, Dict
import redis
from typing import List, Dict, Optional

class LectureMaterialSearcher:
    def __init__(self, es_host: str = "localhost", es_port: int = 9200,
                 es_user: str = "elastic", es_password: str = "secret"):
        self.es = Elasticsearch(
            hosts=[f"http://{es_host}:{es_port}"],
            basic_auth=(es_user, es_password),
            verify_certs=False
        )

    def search(self, query: str) -> List[int]:
        response = self.es.search(
            index="lecture_materials",
            query={
                "multi_match": {
                    "query": query,
                    "fields": ["lecture_name^3", "course_name^2", "content", "keywords"],
                    "type": "best_fields",
                    "fuzziness": "AUTO"
                }
            }
        )
        # возвращаем список lecture_id (int)
        return [hit['_source']['lecture_id'] for hit in response['hits']['hits']]

class LectureMaterialSearcher:
    def __init__(self, es_host: str = "localhost", es_port: int = 9200,
                 es_user: str = "elastic", es_password: str = "secret"):
        self.es = Elasticsearch(
            hosts=[f"http://{es_host}:{es_port}"],
            basic_auth=(es_user, es_password),
            verify_certs=False
        )

    def search(self, query: str) -> List[int]:
        response = self.es.search(
            index="lecture_materials",
            query={
                "multi_match": {
                    "query": query,
                    "fields": ["lecture_name^3", "course_name^2", "content", "keywords"],
                    "type": "best_fields",
                    "fuzziness": "AUTO"
                }
            }
        )
        return [hit['_source']['lecture_id'] for hit in response['hits']['hits']]

class AttendanceFinder:
    def __init__(
        self,
        uri: str = 'bolt://localhost:7687',
        user: str = 'neo4j',
        password: str = 'strongpassword'
    ):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def find_worst_attendees(
        self,
        lecture_ids: List[int],
        top_n: int = 10,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> List[Dict]:
        return self._find_attendance(
            lecture_ids,
            limit=top_n,
            worst=True,
            start_date=start_date,
            end_date=end_date
        )

    def get_attendance_summary(
        self,
        lecture_ids: List[int],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> List[Dict]:
        return self._find_attendance(
            lecture_ids,
            limit=None,
            worst=False,
            start_date=start_date,
            end_date=end_date
        )

    def _find_attendance(
        self,
        lecture_ids: List[int],
        limit: Optional[int] = None,
        worst: bool = True,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> List[Dict]:
        if not lecture_ids:
            return []

        query = '''
        UNWIND $lecture_ids AS lid
        MATCH (l:Lecture {postgres_id: lid})<-[:OF_LECTURE]-(e:ScheduleEvent)
        '''

        if start_date is not None and end_date is not None:
            query += '''
        WHERE e.date >= date($start_date) AND e.date <= date($end_date)
            '''
        query += '''
        MATCH (g:Group)-[:SCHEDULED_FOR]->(e)
        MATCH (st:Student)-[:MEMBER_OF]->(g)
        OPTIONAL MATCH (st)-[a:ATTENDED]->(e)
        WITH st.postgres_id AS studentId, st.name AS studentName,
             collect(coalesce(a.attended, false)) AS flags
        WITH studentId, studentName,
             size([f IN flags WHERE f])    AS attendedCount,
             size(flags)                   AS totalCount
        WHERE totalCount > 0
        RETURN studentId, studentName,
               attendedCount,
               totalCount,
               round(toFloat(attendedCount) / totalCount * 100, 2) AS attendancePercent
        '''
        if worst:
            query += '\nORDER BY attendancePercent ASC'
        else:
            query += '\nORDER BY studentName ASC'

        if limit is not None:
            query += '\nLIMIT $limit'

        params: Dict[str, any] = {'lecture_ids': lecture_ids}
        if limit is not None:
            params['limit'] = limit
        if start_date is not None and end_date is not None:
            params['start_date'] = start_date
            params['end_date'] = end_date

        with self.driver.session() as session:
            result = session.run(query, **params)
            return [record.data() for record in result]
        
if __name__ == '__main__':
    term = "физика"
    searcher = LectureMaterialSearcher(es_password="secret")
    lecture_ids = searcher.search(term)
    print(f"Найдены лекции: {lecture_ids}")

    finder = AttendanceFinder()
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)

    start = "2023-09-01"
    end   = "2023-12-31"

    try:
        worst = finder.find_worst_attendees(
            lecture_ids,
            top_n=10,
            start_date=start,
            end_date=end
        )
        if worst:
            print("\n10 студентов с худшей посещаемостью среди обязанных присутствовать:")
            for idx, rec in enumerate(worst, 1):
                redis_info = r.hgetall(f"student:{rec['studentId']}")
                info_str = f"[Redis] Name: {redis_info.get('name')}, Age: {redis_info.get('age')}, Mail: {redis_info.get('mail')}, Group: {redis_info.get('group')}"
                print(f"{idx}. {rec['studentName']} — {rec['attendancePercent']}% ({rec['attendedCount']}/{rec['totalCount']}) {info_str}")
        else:
            print("Нет данных о посетителях для найденных лекций или студенты не обязаны были присутствовать.")

        summary = finder.get_attendance_summary(
            lecture_ids,
            start_date=start,
            end_date=end
        )
        if summary:
            print("\nСводка посещаемости по всем студентам:")
            for rec in summary:
                redis_info = r.hgetall(f"student:{rec['studentId']}")
                info_str = f"[Redis] Name: {redis_info.get('name')}, Age: {redis_info.get('age')}, Mail: {redis_info.get('mail')}, Group: {redis_info.get('group')}"
                print(f"{rec['studentName']}: {rec['attendancePercent']}% ({rec['attendedCount']}/{rec['totalCount']}) {info_str}")
        else:
            print("Нет данных для сводки посещаемости.")

    finally:
        finder.close()
        r.close()