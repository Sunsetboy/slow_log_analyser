"""
Slow log parser / analyser
Usage: python slow_log.py <log_file_path> <limit>
limit is optional, if provided, only top n slow queries will be printed
"""

import re
from dataclasses import dataclass
import sys
from sys import argv

if len(argv) < 2:
    print("Usage: python slow_log.py <log_file_path> <limit>")
    sys.exit(1)

log_file_path = argv[1]

limit = int(argv[2]) if len(argv) > 2 else None


@dataclass
class SlowQuery:
    query_text: str = ""
    duration: float = 0.0
    rows_examined: int = 0

    def __str__(self):
        return f"{self.query_text}\n Took {self.duration} seconds, rows examined: {self.rows_examined}"


# dictionary to store slow queries, key is query text
slow_queries = {}


# open log file and read it line by line
with open(log_file_path, "r") as file:
    current_slow_query = None
    for line_number, line in enumerate(file):
        if line_number < 3:
            continue

        if line.startswith("# Time:"):
            if current_slow_query:
                slow_queries[current_slow_query.query_text] = current_slow_query
            current_slow_query = SlowQuery()
            continue

        if line.startswith("# Query_time:"):
            pattern = r"Query_time:\s([\d.]+).*Rows_examined:\s(\d+)"

            # Search for the pattern in the string
            match = re.search(pattern, line)
            if match:
                query_time = float(match.group(1))
                rows_examined = int(match.group(2))
                current_slow_query.duration = query_time
                current_slow_query.rows_examined = rows_examined
            continue

        if line.startswith("SET timestamp") or line.startswith("# User@Host"):
            continue

        current_slow_query.query_text += line

    if current_slow_query:
        slow_queries[current_slow_query.query_text] = current_slow_query

# sort slow queries by duration
sorted_slow_queries = sorted(
    slow_queries.values(), key=lambda q: q.duration, reverse=True
)

# print sorted slow queries
if limit:
    sorted_slow_queries = sorted_slow_queries[:limit]
    print(f"The top {limit} slow queries are:")
else:
    print("All slow queries are:")

for query in sorted_slow_queries:
    print(query)
    print("==================")
