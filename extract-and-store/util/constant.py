DELTA_TIME_UNITS = ["year", "month", "day", "hour", "minute"]
DELTA_TIMES_IN_MINUTE = [525600, 43200]

UNSET_VALUE = "-"

PARTITION_TIME_CONNECTOR = "-"
MAPPING_CHILD_ITEM = {
    "repository": "repositories",
    "pullRequest": "pullRequests",
    "language": "languages"
}


MAPPING_API_NAME = {
    "organization": "organizations",
    "repository": "repositories"
}

COMPRESSION_EXTENSION = {
    None: "",
    "gzip": ".gz",
    "snappy": ".snappy",
    "brotli": ".br",
    "lz4": ".lz4",
    "zstd": ".zst",
}

REPOSITORY = {
    "query": """
query {
	node(id:"{_id_}"){
    ...on Organization{
      repositories(first: 100 , after:"{_end_cursor_}") {
        totalCount
        pageInfo {
            hasNextPage
            endCursor
        }
        nodes {
          name
          createdAt
          description
          id
          isFork
          pushedAt
          updatedAt
          url
        }
      }
    }
  }
}
    """
}

LANGUAGE = {
    "query": """
query {
    node(id:"{_id_}") {
    ...on Repository{
      languages(first: 100, after:"{_end_cursor_}"){
        totalCount
        pageInfo {
          hasNextPage
          endCursor
        }
        nodes{
          color
          id
          name
        }
      }
    }
  }
}
   """
}

