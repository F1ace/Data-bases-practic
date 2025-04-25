import os
from elasticsearch import Elasticsearch
import psycopg2
from faker import Faker
from typing import Dict, List

def generate_and_sync_lecture_materials(
    es_host: str = "localhost",
    es_port: int = 9200,
    es_user: str = "elastic",
    es_password: str = "secret",
    materials_dir: str = "./lecture_materials"
) -> None:
    """
    Generate and sync synthetic lecture materials to Elasticsearch based on PostgreSQL lecture data.
    Also saves generated materials as text files.
    
    Args:
        pg_conn_params: PostgreSQL connection parameters
        es_host: Elasticsearch host
        es_port: Elasticsearch port
        es_user: Elasticsearch username
        es_password: Elasticsearch password
        materials_dir: Directory to store material text files
    """
    # Initialize Faker for realistic text generation
    fake = Faker("ru_RU")
    Faker.seed(42)  # For reproducible results
    os.makedirs(materials_dir, exist_ok=True)
    
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
    es = Elasticsearch(
        hosts=[f"http://{es_host}:{es_port}"],
        basic_auth=(es_user, es_password),
        verify_certs=False
    )
    
    try:
        # Create Elasticsearch index with simpler Russian language support
        if not es.indices.exists(index="lecture_materials"):
            es.indices.create(
                index="lecture_materials",
                settings={
                    "analysis": {
                        "analyzer": {
                            "russian": {
                                "type": "custom",
                                "tokenizer": "standard",
                                "filter": [
                                    "lowercase",
                                    "stop",
                                    "snowball"
                                ]
                            }
                        },
                        "filter": {
                            "russian_stop": {
                                "type": "stop",
                                "stopwords": "_russian_"
                            },
                            "russian_stemmer": {
                                "type": "snowball",
                                "language": "Russian"
                            }
                        }
                    }
                },
                mappings={
                    "properties": {
                        "lecture_id": {"type": "integer"},
                        "lecture_name": {
                            "type": "text",
                            "analyzer": "russian",
                            "fields": {"keyword": {"type": "keyword"}}
                        },
                        "course_name": {
                            "type": "text",
                            "analyzer": "russian",
                            "fields": {"keyword": {"type": "keyword"}}
                        },
                        "content": {
                            "type": "text",
                            "analyzer": "russian"
                        },
                        "keywords": {"type": "keyword"},
                        "generated_content": {"type": "boolean"},
                        "file_path": {"type": "keyword"}
                    }
                }
            )
        
        # Get all lectures from PostgreSQL with their courses
        pg_cur.execute("""
            SELECT l.id, l.name, c.name as course_name
            FROM Lecture l
            JOIN Course_of_lecture c ON l.course_of_lecture_id = c.id
        """)
        lectures = pg_cur.fetchall()
        
        # Russian academic terms for more realistic content
        academic_terms = [
            # Fundamental concepts
            "теория", "практика", "методология", "исследование",
            "анализ", "синтез", "гипотеза", "эксперимент",
            "формула", "уравнение", "концепция", "парадигма",
            "алгоритм", "модель", "структура", "система",
            
            # Scientific methods
            "наблюдение", "верификация", "фальсификация", "индукция",
            "дедукция", "абстракция", "аксиома", "постулат",
            "корреляция", "регрессия", "статистика", "выборка",
            "репрезентативность", "валидность", "репликация",
            
            # Mathematics
            "интеграл", "дифференциал", "матрица", "вектор",
            "тензор", "топология", "граф", "множество",
            "изоморфизм", "гомоморфизм", "биекция", "инъекция",
            "сюръекция", "тождество", "константа", "переменная",
            
            # Physics
            "квант", "поле", "частица", "волна",
            "энтропия", "энергия", "масса", "заряд",
            "спин", "орбиталь", "валентность", "кристалл",
            "дифракция", "интерференция", "поляризация", "резонанс",
            
            # Computer Science
            "программа", "компилятор", "интерпретатор", "байт",
            "бит", "шифрование", "хеш", "автомат",
            "нейронная сеть", "градиент", "оптимизация", "композиция",
            "инкапсуляция", "наследование", "полиморфизм", "итерация",
            
            # Biology/Chemistry
            "клетка", "организм", "фермент", "катализатор",
            "реакция", "соединение", "молекула", "атом",
            "электрон", "протон", "нейтрон", "изотоп",
            "полимер", "мономер", "липид", "белок",
            
            # Humanities
            "дискурс", "нарратив", "герменевтика", "феномен",
            "ноумен", "гносеология", "онтология", "диалектика",
            "семиотика", "синтагма", "парадигма", "интенция",
            
            # Engineering
            "конструкция", "механизм", "привод", "трансмиссия",
            "устойчивость", "надежность", "прочность", "жесткость",
            "деформация", "напряжение", "усталость", "трение",
            
            # Advanced terms
            "бифуркация", "аттрактор", "фрактал", "энтропия",
            "эмерджентность", "рекурсия", "инвариант", "топос",
            "морфизм", "функтор", "категорность", "гомология",
            
            # Academic processes
            "публикация", "рецензирование", "цитирование", "индексация",
            "аппликация", "аппроксимация", "итерация", "конвергенция",
            "дивергенция", "оптимизация", "максимизация", "минимизация"
        ]
        
        for lecture_id, lecture_name, course_name in lectures:
            # Generate realistic Russian academic content
            content = f"""
            Лекция: {lecture_name}
            Курс: {course_name}
            Преподаватель: {fake.name()}
            
            Основные понятия:
            {fake.paragraph(nb_sentences=8, variable_nb_sentences=True)}
            
            Теоретическая часть:
            {fake.paragraph(nb_sentences=12, variable_nb_sentences=True)}
            
            Практическое применение:
            {fake.paragraph(nb_sentences=10, variable_nb_sentences=True)}
            
            Рекомендуемая литература:
            1. {fake.catch_phrase()} / {fake.name()}
            2. {fake.catch_phrase()} / {fake.name()}
            """
            
            # Add some academic terms to make it more searchable
            for term in academic_terms[:3]:
                content = content.replace(". ", f" {term}. ", 1)
            
            # Generate keywords
            keywords = list(set([
                *course_name.lower().split(),
                *lecture_name.lower().split(),
                *fake.words(nb=3),
                *academic_terms[:2]
            ]))
            
            # Save to text file
            file_name = f"lecture_{lecture_id}.txt"
            file_path = os.path.join(materials_dir, file_name)
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            # Index in Elasticsearch
            doc = {
                "lecture_id": lecture_id,
                "lecture_name": lecture_name,
                "course_name": course_name,
                "content": content,
                "keywords": keywords,
                "generated_content": True,
                "file_path": file_path
            }
            
            es.index(
                index="lecture_materials",
                id=lecture_id,
                document=doc
            )
        
        print(f"Generated and synced {len(lectures)} lecture materials")
        print(f"Text files stored in: {os.path.abspath(materials_dir)}")
        
        # Refresh index
        es.indices.refresh(index="lecture_materials")
    
    except Exception as e:
        print(f"Error during synchronization: {e}")
        raise
    finally:
        pg_cur.close()
        pg_conn.close()
        es.close()

class LectureMaterialSearcher:
    def __init__(self, es_host="localhost", es_port=9200, es_user="elastic", es_password="secret"):
        self.es = Elasticsearch(
            hosts=[f"http://{es_host}:{es_port}"],
            basic_auth=(es_user, es_password),
            verify_certs=False
        )
    
    def search(self, query: str, field: str = None) -> List[Dict]:
        """
        Search lecture materials with full-text capabilities
        
        Args:
            query: Search query
            field: Specific field to search (None searches all text fields)
        """
        search_fields = ["lecture_name^3", "course_name^2", "content", "keywords"] if field is None else [field]
        
        response = self.es.search(
            index="lecture_materials",
            query={
                "multi_match": {
                    "query": query,
                    "fields": search_fields,
                    "type": "best_fields",
                    "fuzziness": "AUTO"
                }
            },
            highlight={
                "fields": {
                    "content": {"fragment_size": 150, "number_of_fragments": 2},
                    "lecture_name": {}
                }
            }
        )
        
        return [{
            **hit["_source"],
            "id": hit["_id"],
            "score": hit["_score"],
            "highlight": hit.get("highlight", {})
        } for hit in response["hits"]["hits"]]
    
    def get_by_lecture_id(self, lecture_id: int) -> Dict:
        """Retrieve material by lecture ID"""
        try:
            response = self.es.get(index="lecture_materials", id=lecture_id)
            return response["_source"]
        except:
            return None
    
    def get_related_materials(self, lecture_id: int) -> List[Dict]:
        """Find related materials using more-like-this query"""
        response = self.es.search(
            index="lecture_materials",
            query={
                "more_like_this": {
                    "fields": ["content", "keywords"],
                    "like": [{"_index": "lecture_materials", "_id": lecture_id}],
                    "min_term_freq": 1,
                    "max_query_terms": 12
                }
            },
            size=5
        )
        return [hit["_source"] for hit in response["hits"]["hits"]]

if __name__ == "__main__":
    
    generate_and_sync_lecture_materials(
        es_password="secret",
        materials_dir="./lecture_materials"
    )
    
    searcher = LectureMaterialSearcher(es_password="secret")
    
    print("\nSearch for 'квантовая физика':")
    for result in searcher.search("квантовая физика"):
        print(f"\nLecture: {result['lecture_name']} (Score: {result['score']:.2f})")
        print(f"Course: {result['course_name']}")
        if 'highlight' in result:
            print("Highlights:", "...".join(result['highlight'].get('content', [])))
    
    print("\nSearch in course names only for 'математика':")
    for result in searcher.search("математика", field="course_name"):
        print(f"{result['course_name']}: {result['lecture_name']}")