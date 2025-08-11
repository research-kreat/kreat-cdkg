db.industry_taxonomy.aggregate([
  {
    "$vectorSearch": {
      "index": "vector_index",
      "path": "embedding",
      "queryVector": [<array-of-numbers>],
      "numCandidates": <number-of-candidates>,
      "limit": <number-of-results>
    }
  }
])