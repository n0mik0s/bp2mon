query = {
  "sort": [
    {"@timestamp": {"order": "asc"}}
  ],
  "query": {
    "bool": {
      "filter": {
        "range" : {
          "@timestamp" : {
            "gte": "now-21d/d",
            "lte": "now/m"
          }
        }
      }
    }
  }
}