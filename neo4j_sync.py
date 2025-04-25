import os
import psycopg2
from neo4j import GraphDatabase

PG_DB = "postgres_db"
PG_USER = "postgres_user"
PG_PASSWORD = "postgres_password"
PG_HOST = "localhost"
PG_PORT = "5430"


NEO4J_URI = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
NEO4J_USER = os.getenv('NEO4J_USER', 'neo4j')
NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD', 'strongpassword')

# Establish connections
pg_conn = psycopg2.connect(
    host=PG_HOST, port=PG_PORT, dbname=PG_DB,
    user=PG_USER, password=PG_PASSWORD
)
pg_cursor = pg_conn.cursor()

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def sync_universities(tx):
    pg_cursor.execute("SELECT id, name, location FROM University;")
    for id_, name, location in pg_cursor.fetchall():
        tx.run(
            "MERGE (u:University {id: $id})"
            " SET u.name = $name, u.location = $location",
            id=id_, name=name, location=location
        )

def sync_institutes(tx):
    pg_cursor.execute("SELECT id, name, university_id FROM Institute;")
    for id_, name, uni_id in pg_cursor.fetchall():
        tx.run(
            "MERGE (i:Institute {id: $id})"
            " SET i.name = $name"
            " WITH i"
            " MATCH (u:University {id: $uni_id})"
            " MERGE (u)-[:HAS_INSTITUTE]->(i)",
            id=id_, name=name, uni_id=uni_id
        )

def sync_departments(tx):
    pg_cursor.execute("SELECT id, name, institute_id FROM Department;")
    for id_, name, inst_id in pg_cursor.fetchall():
        tx.run(
            "MERGE (d:Department {id: $id})"
            " SET d.name = $name"
            " WITH d"
            " MATCH (i:Institute {id: $inst_id})"
            " MERGE (i)-[:HAS_DEPARTMENT]->(d)",
            id=id_, name=name, inst_id=inst_id
        )

def sync_specialties(tx):
    pg_cursor.execute("SELECT id, name, department_id FROM Specialty;")
    for id_, name, dept_id in pg_cursor.fetchall():
        tx.run(
            "MERGE (s:Specialty {id: $id})"
            " SET s.name = $name"
            " WITH s"
            " MATCH (d:Department {id: $dept_id})"
            " MERGE (d)-[:HAS_SPECIALTY]->(s)",
            id=id_, name=name, dept_id=dept_id
        )

def sync_groups(tx):
    pg_cursor.execute("SELECT id, name, speciality_id FROM St_group;")
    for id_, name, spec_id in pg_cursor.fetchall():
        tx.run(
            "MERGE (g:Group {id: $id})"
            " SET g.name = $name"
            " WITH g"
            " MATCH (s:Specialty {id: $spec_id})"
            " MERGE (s)-[:HAS_GROUP]->(g)",
            id=id_, name=name, spec_id=spec_id
        )

def sync_courses_and_lectures(tx):
    # Courses
    pg_cursor.execute("SELECT id, name, department_id, specialty_id FROM Course_of_lecture;")
    for id_, name, dept_id, spec_id in pg_cursor.fetchall():
        tx.run(
            "MERGE (c:Course {id: $id})"
            " SET c.name = $name"
            " WITH c"
            " MATCH (d:Department {id: $dept_id}), (s:Specialty {id: $spec_id})"
            " MERGE (d)-[:OFFERS_COURSE]->(c)"
            " MERGE (s)-[:REQUIRES_COURSE]->(c)",
            id=id_, name=name, dept_id=dept_id, spec_id=spec_id
        )
    # Lectures
    pg_cursor.execute("SELECT id, name, course_of_lecture_id FROM Lecture;")
    for id_, name, course_id in pg_cursor.fetchall():
        tx.run(
            "MERGE (l:Lecture {id: $id})"
            " SET l.name = $name"
            " WITH l"
            " MATCH (c:Course {id: $course_id})"
            " MERGE (c)-[:INCLUDES_LECTURE]->(l)",
            id=id_, name=name, course_id=course_id
        )

def sync_sessions(tx):
    pg_cursor.execute("SELECT id, date, lecture_id, group_id FROM Schedule;")
    for id_, date, lec_id, grp_id in pg_cursor.fetchall():
        tx.run(
            "MERGE (ss:Session {id: $id})"
            " SET ss.date = $date"
            " WITH ss"
            " MATCH (l:Lecture {id: $lec_id}), (g:Group {id: $grp_id})"
            " MERGE (l)-[:HAS_SESSION]->(ss)"
            " MERGE (ss)-[:FOR_GROUP]->(g)",
            id=id_, date=date, lec_id=lec_id, grp_id=grp_id
        )

def sync_students(tx):
    pg_cursor.execute("SELECT id, name, age, mail, group_id FROM Students;")
    for id_, name, age, mail, grp_id in pg_cursor.fetchall():
        tx.run(
            "MERGE (st:Student {id: $id})"
            " SET st.name = $name, st.age = $age, st.mail = $mail"
            " WITH st"
            " MATCH (g:Group {id: $grp_id})"
            " MERGE (st)-[:MEMBER_OF]->(g)",
            id=id_, name=name, age=age, mail=mail, grp_id=grp_id
        )

def sync_attendance(tx):
    pg_cursor.execute("SELECT schedule_id, student_id, attended FROM Attendance;")
    attendance_map = {}
    for sched_id, stu_id, att in pg_cursor.fetchall():
        attendance_map.setdefault(sched_id, {})[stu_id] = att

    pg_cursor.execute("SELECT id, group_id FROM Schedule;")
    for sess_id, grp_id in pg_cursor.fetchall():
        pg_cursor.execute(
            "SELECT id FROM Students WHERE group_id = %s;",
            (grp_id,)
        )
        student_ids = [r[0] for r in pg_cursor.fetchall()]

        for stu_id in student_ids:
            attended = attendance_map.get(sess_id, {}).get(stu_id, False)
            tx.run(
                "MATCH (st:Student {id: $stu_id}), (ss:Session {id: $sess_id})"
                " MERGE (st)-[r:ATTENDANCE]->(ss)"
                " SET r.attended = $attended",
                stu_id=stu_id, sess_id=sess_id, attended=attended
            )

def main():
    with driver.session() as session:
        session.execute_write(sync_universities)
        session.execute_write(sync_institutes)
        session.execute_write(sync_departments)
        session.execute_write(sync_specialties)
        session.execute_write(sync_groups)
        session.execute_write(sync_courses_and_lectures)
        session.execute_write(sync_sessions)
        session.execute_write(sync_students)
        session.execute_write(sync_attendance)
    print("Sync completed successfully.")

if __name__ == '__main__':
    main()