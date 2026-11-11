import time
import numpy as np
from qdrant_client import QdrantClient
from qdrant_client.http import models

print("==================================================")
print("TASK 1: Reproduce Class Example by Hand (2D Points)")
print("==================================================")
# Given 2D points and query
points_2d = np.array([
    [2, 3],
    [1, 2],
    [0, 4]
])
query_2d = np.array([2, 4])

# 1. Euclidean Distance
euclid_dists = np.linalg.norm(points_2d - query_2d, axis=1)
euclid_ranking = np.argsort(euclid_dists)

# 2. Cosine Similarity
dot_prods_2d = np.dot(points_2d, query_2d)
norms_p = np.linalg.norm(points_2d, axis=1)
norm_q = np.linalg.norm(query_2d)
cosine_sims_2d = dot_prods_2d / (norms_p * norm_q)
cosine_ranking = np.argsort(cosine_sims_2d)[::-1]

print(f"Points: {points_2d.tolist()}")
print(f"Query: {query_2d.tolist()}")
print(f"Euclidean Distances: {euclid_dists}")
print(f"Euclidean Ranking (ascending): Point indices {euclid_ranking}")
print(f"Cosine Similarities: {cosine_sims_2d}")
print(f"Cosine Ranking (descending): Point indices {cosine_ranking}")


print("\n==================================================")
print("TASK 2: Brute-Force Top-k Search in NumPy (1,000 vectors)")
print("==================================================")
np.random.seed(42)
dim = 768
n_vectors_1k = 1000
db_1k = np.random.rand(n_vectors_1k, dim)
q_1k = np.random.rand(dim)
k = 5

def numpy_top_k(db, query, k=5):
    # Euclidean (using squared distance for efficiency)
    e_dists = np.sum((db - query) ** 2, axis=1)
    e_top = np.argsort(e_dists)[:k]
    
    # Cosine Similarity
    c_sims = np.dot(db, query) / (np.linalg.norm(db, axis=1) * np.linalg.norm(query))
    c_top = np.argsort(c_sims)[::-1][:k]
    return e_top, c_top, c_sims[c_top]

e_top_idx, c_top_idx, c_top_scores = numpy_top_k(db_1k, q_1k, k)
print(f"Top {k} Euclidean indices: {e_top_idx}")
print(f"Top {k} Cosine similarity indices: {c_top_idx}")


print("\n==================================================")
print("TASK 3: Qdrant Verification (In-Memory)")
print("==================================================")
client = QdrantClient(":memory:")
collection_name = "gemini_mock"

# Create collection configured for Cosine distance
client.recreate_collection(
    collection_name=collection_name,
    vectors_config=models.VectorParams(size=dim, distance=models.Distance.COSINE)
)

# Upload points to Qdrant
points_structs = [
    models.PointStruct(id=i, vector=db_1k[i].tolist(), payload={"index": i})
    for i in range(n_vectors_1k)
]
client.upsert(collection_name=collection_name, points=points_structs)

# Query Qdrant
search_result = client.query_points(
    collection_name=collection_name,
    query=q_1k.tolist(),
    limit=k
).points

qdrant_top_ids = [hit.id for hit in search_result]
print(f"Top {k} Qdrant results (Cosine): {qdrant_top_ids}")
print(f"Do NumPy and Qdrant match? {np.array_equal(c_top_idx, qdrant_top_ids)}")


print("\n==================================================")
print("TASK 4: Vector Normalization & Dot Product Equivalence")
print("==================================================")
def l2_normalize(v):
    norm = np.linalg.norm(v, axis=-1, keepdims=True)
    return v / np.maximum(norm, 1e-12)

db_norm = l2_normalize(db_1k)
q_norm = l2_normalize(q_1k)

# On unit-normalized vectors, dot product equals cosine similarity
dot_product_sims = np.dot(db_norm, q_norm)
normalized_ranking = np.argsort(dot_product_sims)[::-1][:k]

print(f"Top {k} ranking using Dot Product on normalized vectors: {normalized_ranking}")
print(f"Matches Cosine Similarity ranking? {np.array_equal(c_top_idx, normalized_ranking)}")


print("\n==================================================")
print("TASK 5: Benchmarking (1,000 vs 10,000 Vectors)")
print("==================================================")
def benchmark_numpy(n):
    data = np.random.rand(n, dim)
    query = np.random.rand(dim)
    start = time.time()
    _ = np.dot(data, query) / (np.linalg.norm(data, axis=1) * np.linalg.norm(query))
    return time.time() - start

time_1k = benchmark_numpy(1000)
time_10k = benchmark_numpy(10000)

print(f"NumPy Brute-Force Time (1,000 vectors):  {time_1k:.6f} seconds")
print(f"NumPy Brute-Force Time (10,000 vectors): {time_10k:.6f} seconds")
print("\n[README Note on HNSW]:")
print("  - Brute-force scales linearly O(N) by checking every single vector.")
print("  - HNSW (Hierarchical Navigable Small World) builds a multi-layer proximity graph,")
print("    reducing search complexity to O(log N) and enabling fast sub-millisecond retrieval at scale.")