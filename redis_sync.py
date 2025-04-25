import psycopg2
import redis
from typing import Dict, List

def sync_students_to_redis(redis_host: str = 'localhost', redis_port: int = 6379) -> None:

    DB_NAME = "postgres_db"
    DB_USER = "postgres_user"
    DB_PASSWORD = "postgres_password"
    DB_HOST = "localhost"
    DB_PORT = "5430"

    pg_conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    pg_cur = pg_conn.cursor()
    r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
    
    try:
        for key in r.scan_iter("student:*"):
            r.delete(key)
        for key in r.scan_iter("index:student:*"):
            r.delete(key)
        
        pg_cur.execute("""
            SELECT s.id, s.name, s.age, s.mail, g.name as group_name
            FROM Students s
            JOIN St_group g ON s.group_id = g.id
        """)
        students = pg_cur.fetchall()
        
        for student_id, name, age, mail, group_name in students:
            student_key = f"student:{student_id}"
            r.hset(student_key, mapping={
                'id': student_id,
                'name': name,
                'age': age,
                'mail': mail,
                'group': group_name
            })
            
            r.sadd(f"index:student:name:{name.lower()}", student_id)
            
            if mail:
                r.sadd(f"index:student:email:{mail.lower()}", student_id)
            
            r.sadd(f"index:student:group:{group_name.lower()}", student_id)
            
            search_terms = f"{name} {mail} {group_name}".lower().split()
            for term in search_terms:
                r.sadd(f"index:student:search:{term}", student_id)
        
        print(f"Successfully synchronized {len(students)} students to Redis")
        
    except Exception as e:
        print(f"Error during synchronization: {e}")
        raise
    finally:
        pg_cur.close()
        pg_conn.close()
        r.close()

# Example search functions that can be used after syncing
class StudentSearch:
    def get_student_full(self, student_id: int) -> Dict:
        key = f"student:{student_id}"
        student_data = self.r.hgetall(key)
        
        if not student_data:
            raise ValueError(f"Student with id {student_id} not found in Redis.")
        
        return {
            "id": int(student_data["id"]),
            "name": student_data["name"],
            "age": int(student_data["age"]),
            "mail": student_data["mail"],
            "group": student_data["group"]
        }
    def __init__(self, redis_host='localhost', redis_port=6379):
        self.r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
    
    def get_by_id(self, student_id: int) -> Dict:
        """Get student by ID"""
        return self.r.hgetall(f"student:{student_id}")
    
    def search_by_name(self, name: str) -> List[Dict]:
        """Search students by name (case-insensitive partial match)"""
        keys = self.r.keys(f"index:student:name:*{name.lower()}*")
        student_ids = set()
        for key in keys:
            student_ids.update(self.r.smembers(key))
        return [self.r.hgetall(f"student:{id}") for id in student_ids]
    
    def search_by_email(self, email: str) -> List[Dict]:
        """Search students by email (case-insensitive partial match)"""
        keys = self.r.keys(f"index:student:email:*{email.lower()}*")
        student_ids = set()
        for key in keys:
            student_ids.update(self.r.smembers(key))
        return [self.r.hgetall(f"student:{id}") for id in student_ids]
    
    def search_by_group(self, group_name: str) -> List[Dict]:
        """Search students by group name (case-insensitive partial match)"""
        keys = self.r.keys(f"index:student:group:*{group_name.lower()}*")
        student_ids = set()
        for key in keys:
            student_ids.update(self.r.smembers(key))
        return [self.r.hgetall(f"student:{id}") for id in student_ids]
    
    def full_text_search(self, query: str) -> List[Dict]:
        """Full-text search across all student fields"""
        terms = query.lower().split()
        if not terms:
            return []
        first_term = terms[0]
        keys = self.r.keys(f"index:student:search:*{first_term}*")
        student_ids = set()
        for key in keys:
            student_ids.update(self.r.smembers(key))

        for term in terms[1:]:
            term_keys = self.r.keys(f"index:student:search:*{term}*")
            term_ids = set()
            for key in term_keys:
                term_ids.update(self.r.smembers(key))
            student_ids.intersection_update(term_ids)
        
        return [self.r.hgetall(f"student:{id}") for id in student_ids]

if __name__ == "__main__":
    sync_students_to_redis()
    searcher = StudentSearch()
    
    #print("Students named 'Иванов':")
    #print(searcher.search_by_name("Иванов"))
    
    #print("\nStudents in group 'МЕХ-101':")
    #print(searcher.search_by_group("МЕХ-101"))
    
    #print("\nFull text search for 'МЕХ':")
    #print(searcher.full_text_search("МЕХ"))