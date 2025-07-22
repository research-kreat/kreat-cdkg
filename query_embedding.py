from sentence_transformers import SentenceTransformer
model = SentenceTransformer("all-MiniLM-L6-v2")
query_embedding = model.encode("automobile").tolist()
print(query_embedding)