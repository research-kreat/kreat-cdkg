from sentence_transformers import SentenceTransformer
model = SentenceTransformer("all-MiniLM-L6-v2")


def embed(query):
    query_embedding=model.encode(query).tolist()
    return query_embedding