import os
from elasticsearch import Elasticsearch

def print_lecture_material(material_id: int, 
                         es_host: str = "localhost",
                         es_port: int = 9200,
                         es_user: str = "elastic",
                         es_password: str = "secret",
                         materials_dir: str = "./lecture_materials"):
    """
    Print lecture material by ID from both text file and Elasticsearch
    
    Args:
        material_id: ID of the lecture material to print
        es_host: Elasticsearch host
        es_port: Elasticsearch port
        es_user: Elasticsearch username
        es_password: Elasticsearch password
        materials_dir: Directory where text files are stored
    """
    # Initialize Elasticsearch client
    es = Elasticsearch(
        hosts=[f"http://{es_host}:{es_port}"],
        basic_auth=(es_user, es_password),
        verify_certs=False
    )
    
    try:
        es_response = es.get(index="lecture_materials", id=material_id)
        es_data = es_response['_source']
        
        print("="*50)
        print(f"LECTURE MATERIAL ID: {material_id}")
        print("="*50)
        print(f"\nLecture: {es_data['lecture_name']}")
        print(f"Course: {es_data['course_name']}")
        print("\nFrom Elasticsearch:")
        print("-"*50)
        print(es_data['content'])
        
    except Exception as e:
        print(f"Error retrieving material {material_id}: {e}")
    finally:
        es.close()

if __name__ == "__main__":
    print_lecture_material(1, es_password="secret")