import psycopg2
from datetime import datetime
from typing import List, Dict

DB_NAME = "postgres_db"
DB_USER = "postgres_user"
DB_PASSWORD = "postgres_password"
DB_HOST = "localhost"
DB_PORT = "5430"

class CourseCapacityReporter:
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        self.cur = self.conn.cursor()

    def generate_capacity_report(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        query = """
        WITH course_period AS (
            SELECT 
                c.id AS course_id,
                c.name AS course_name,
                sch.group_id
            FROM Course_of_lecture c
            JOIN Lecture l ON c.id = l.course_of_lecture_id
            JOIN Schedule sch ON l.id = sch.lecture_id
            WHERE sch.date BETWEEN %(start_date)s AND %(end_date)s
            GROUP BY c.id, c.name, sch.group_id
        ),
        group_student_counts AS (
            SELECT 
                cp.course_id,
                cp.course_name,
                gs.students_count
            FROM course_period cp
            JOIN (
                SELECT group_id, COUNT(*) AS students_count
                FROM Students
                GROUP BY group_id
            ) gs ON cp.group_id = gs.group_id
        ),
        course_students_total AS (
            SELECT 
                course_id,
                course_name,
                SUM(students_count) AS total_students
            FROM group_student_counts
            GROUP BY course_id, course_name
        ),
        lecture_materials AS (
            SELECT 
                c.id AS course_id,
                l.id AS lecture_id,
                l.name AS lecture_name,
                ARRAY_AGG(DISTINCT m.name) FILTER (WHERE m.name IS NOT NULL) AS technical_requirements
            FROM Course_of_lecture c
            JOIN Lecture l ON c.id = l.course_of_lecture_id
            LEFT JOIN Material_of_lecture m ON l.id = m.course_of_lecture_id
            GROUP BY c.id, l.id, l.name
        )
        SELECT 
            cst.course_id,
            cst.course_name,
            lm.lecture_id,
            lm.lecture_name,
            lm.technical_requirements,
            cst.total_students
        FROM course_students_total cst
        JOIN lecture_materials lm ON cst.course_id = lm.course_id;
        """

        self.cur.execute(query, {
            'start_date': start_date,
            'end_date': end_date
        })
        rows = self.cur.fetchall()

        report = []
        course_map = {}
        for row in rows:
            (course_id, course_name, lecture_id, lecture_name, 
             tech_reqs, total_students) = row
            if course_id not in course_map:
                course_map[course_id] = {
                    'course_id': course_id,
                    'course_name': course_name,
                    'total_students': total_students,
                    'lectures': []
                }
                report.append(course_map[course_id])
            
            course_map[course_id]['lectures'].append({
                'lecture_id': lecture_id,
                'lecture_name': lecture_name,
                'technical_requirements': tech_reqs if tech_reqs else []
            })

        return report

    def close(self):
        self.cur.close()
        self.conn.close()

# Пример использования
if __name__ == "__main__":
    reporter = CourseCapacityReporter()
    try:
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 12, 31)
        report = reporter.generate_capacity_report(start_date, end_date)
        
        for course in report:
            print(f"Курс: {course['course_name']} (ID: {course['course_id']})")
            print(f"Общее количество слушателей: {course['total_students']}")
            print("Лекции и технические требования:")
            for lecture in course['lectures']:
                reqs = ", ".join(filter(None, lecture['technical_requirements'])) if lecture['technical_requirements'] else "Нет требований"
                print(f"  Лекция: {lecture['lecture_name']} (ID: {lecture['lecture_id']})")
                print(f"  Требуемые технические средства: {reqs}")
            print("\n" + "="*50 + "\n")
    finally:
        reporter.close()