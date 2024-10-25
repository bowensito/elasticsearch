from elasticsearch import Elasticsearch
import wikipedia
import re

client = Elasticsearch(
    hosts=["https://localhost:9200"], 
    basic_auth=("ignacio", "123456"), verify_certs=False, ssl_show_warn=False)

if client.ping():
    print("Conexión exitosa a Elasticsearch")
else:
    print("Error al conectar a Elasticsearch")

# definir el mapping del indice
editsMapping = {
    "mappings": {
        "properties": {
            'titulo': {'type': 'text'},
            'resumen': {'type': 'text'},
            'nombreEditor': {'type': 'text'},
            'fechaEdicion': {'type': 'date'}
        }
    }
}

# creacion indice y comprobacion de existencia
if not client.indices.exists(index="wikipedia_edits"):
    client.indices.create(index="wikipedia_edits", body=editsMapping)
    print("Índice 'wikipedia_edits' creado exitosamente.")
else:
    print("Índice 'wikipedia_edits' ya existe.")

#consulta para ver ediciones realizadas luego del dia 1 de enero de 2021
consultaAvanzada = {
    "query": {
        "bool": {
            "must": [
                {"match": {"resumen": "science"}}
            ],
            "filter": [
                {"range": {"fechaEdicion": {"gte": "2021-01-01"}}}
            ]
        }
    }
}

# definir busqueda para la consulta avanzada
busqueda = client.search(index="wikipedia_edits", body=consultaAvanzada)

# mostrar resultados de busqueda
print("Resultados de la consulta:")
for hit in busqueda['hits']['hits']:
    print(f"Artículo: {hit['_source']['titulo']}, "
          f"Fecha de edición: {hit['_source']['fechaEdicion']}, "
          f"Resumen: {hit['_source']['resumen']}")

