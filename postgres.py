import psycopg2
from psycopg2 import sql

DB_NAME = "postgres_db"
DB_USER = "postgres_user"
DB_PASSWORD = "postgres_password"
DB_HOST = "localhost"
DB_PORT = "5430"

conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)
cur = conn.cursor()

try:
    # Таблица University
    cur.execute("""
        CREATE TABLE IF NOT EXISTS University (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            location VARCHAR(100))
    """)

    # Таблица Institute
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Institute (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            university_id INTEGER REFERENCES University(id))
    """)

    # Таблица Department
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Department (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            institute_id INTEGER REFERENCES Institute(id))
    """)

    # Таблица Specialty
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Specialty (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            department_id INTEGER REFERENCES Department(id))
    """)

    # Таблица St_group
    cur.execute("""
        CREATE TABLE IF NOT EXISTS St_group (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            speciality_id INTEGER REFERENCES Specialty(id))
    """)

    # Таблица Course_of_lecture
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Course_of_lecture (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            department_id INTEGER REFERENCES Department(id),
            specialty_id INTEGER REFERENCES Specialty(id))
    """)

    # Таблица Lecture
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Lecture (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            course_of_lecture_id INTEGER REFERENCES Course_of_lecture(id))
    """)

    # Таблица Material_of_lecture
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Material_of_lecture (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            course_of_lecture_id INTEGER REFERENCES Lecture(id))
    """)

    # Таблица Schedule
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Schedule (
            id SERIAL PRIMARY KEY,
            date TIMESTAMP,
            lecture_id INTEGER REFERENCES Lecture(id),
            group_id INTEGER REFERENCES St_group(id))
    """)

    # Таблица Students
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Students (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            age INTEGER,
            mail VARCHAR(100),
            group_id INTEGER REFERENCES St_group(id))
    """)

    # Таблица Attendance
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Attendance (
            id SERIAL PRIMARY KEY,
            student_id INTEGER REFERENCES Students(id),
            schedule_id INTEGER REFERENCES Schedule(id),
            attended BOOLEAN NOT NULL)
    """)

    conn.commit()
    print("Таблицы успешно созданы!")

except Exception as e:
    conn.rollback()
    print(f"Ошибка: {e}")
finally:
    cur.close()
    conn.close()