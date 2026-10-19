"""C1/C2: run the application queries on Neo4j and time them."""

from __future__ import annotations

import argparse
import statistics
import sys
from collections import Counter
from time import perf_counter

from ingest_neo4j import connection_settings

PARAMETERS = {
    "k": 10,
    "category": "Music",
    "min_length": 60,
    "max_length": 300,
    "min_views": 1000,
    "max_views": 100000,
    "min_ratings": 100,
}
# RELATED_TO is the only relationship type, so patterns leave it out.
DEGREE_SUMMARY_QUERY = """
MATCH (v:Video)
WITH COUNT { (v)-->() } AS out_degree, COUNT { (v)<--() } AS in_degree
RETURN avg(out_degree) AS avg_degree, max(out_degree) AS max_out,
       max(in_degree) AS max_in,
       count(CASE WHEN out_degree + in_degree = 0 THEN 1 END) AS isolated
"""
OUT_DEGREE_DISTRIBUTION_QUERY = """
MATCH (v:Video)
RETURN COUNT { (v)-->() } AS out_degree, count(*) AS videos ORDER BY out_degree
"""
IN_DEGREE_DISTRIBUTION_QUERY = """
MATCH (v:Video)
RETURN COUNT { (v)<--() } AS in_degree, count(*) AS videos ORDER BY in_degree
"""
TOP_CATEGORIES_QUERY = """
MATCH (v:Video)
RETURN v.category AS category, count(*) AS videos ORDER BY videos DESC LIMIT $k
"""
TOP_VIEWED_QUERY = """
MATCH (v:Video) WHERE v.views IS NOT NULL
RETURN v.id AS id, v.views AS views ORDER BY v.views DESC LIMIT $k
"""
TOP_RATED_QUERY = """
MATCH (v:Video) WHERE v.ratings >= $min_ratings
RETURN v.id AS id, v.rate AS rate, v.ratings AS ratings
ORDER BY rate DESC, ratings DESC LIMIT $k
"""
TOP_REFERENCED_QUERY = """
MATCH (v:Video)
RETURN v.id AS id, COUNT { (v)<--() } AS in_degree
ORDER BY in_degree DESC, id LIMIT $k
"""
TOP_UPLOADERS_QUERY = """
MATCH (v:Video)
RETURN v.uploader AS uploader, sum(v.views) AS views ORDER BY views DESC LIMIT $k
"""
CATEGORY_LENGTH_COUNT_QUERY = """
MATCH (v:Video)
WHERE v.category = $category AND $min_length <= v.length <= $max_length
RETURN count(*) AS videos, avg(v.views) AS avg_views
"""
VIEWS_RANGE_QUERY = """
MATCH (v:Video) WHERE $min_views <= v.views <= $max_views
RETURN count(*) AS videos, avg(v.rate) AS avg_rate
"""
RECIPROCAL_PAIRS_QUERY = """
MATCH (a:Video)-->(b)-->(a) WHERE a.id < b.id
RETURN count(*) AS pairs
"""
DIRECTED_TRIANGLES_QUERY = """
MATCH (a:Video)-->(b)-->(c)-->(a)
WHERE a.id < b.id AND a.id < c.id AND b <> c
RETURN count(*) AS triangles
"""
SAME_CATEGORY_SHARE_QUERY = """
MATCH (a:Video)-->(b)
RETURN count(*) AS edges,
       avg(CASE WHEN a.category = b.category THEN 1.0 ELSE 0 END) AS share
"""
CROSS_CATEGORY_LINKS_QUERY = """
MATCH (a:Video)-->(b) WHERE a.category <> b.category
RETURN a.category AS source, b.category AS target, count(*) AS links
ORDER BY links DESC LIMIT $k
"""
QUERIES = (
    ("aggregation", "degree_summary", DEGREE_SUMMARY_QUERY),
    ("aggregation", "out_degree_distribution", OUT_DEGREE_DISTRIBUTION_QUERY),
    ("aggregation", "in_degree_distribution", IN_DEGREE_DISTRIBUTION_QUERY),
    ("top_k", "top_categories", TOP_CATEGORIES_QUERY),
    ("top_k", "top_viewed", TOP_VIEWED_QUERY),
    ("top_k", "top_rated", TOP_RATED_QUERY),
    ("top_k", "top_referenced", TOP_REFERENCED_QUERY),
    ("top_k", "top_uploaders", TOP_UPLOADERS_QUERY),
    ("range", "category_length_count", CATEGORY_LENGTH_COUNT_QUERY),
    ("range", "views_range", VIEWS_RANGE_QUERY),
    ("pattern", "reciprocal_pairs", RECIPROCAL_PAIRS_QUERY),
    ("pattern", "directed_triangles", DIRECTED_TRIANGLES_QUERY),
    ("pattern", "same_category_share", SAME_CATEGORY_SHARE_QUERY),
    ("pattern", "cross_category_links", CROSS_CATEGORY_LINKS_QUERY),
)


def read_rows(tx, query):
    result = tx.run(query, PARAMETERS)
    return [record.data() for record in result], result.consume()


def access_operators(plan):
    # Leaf operators show whether a query seeks an index or scans.
    operator = plan["operatorType"].split("@")[0]
    found = [operator] if "Seek" in operator or "Scan" in operator else []
    for child in plan.get("children", []):
        found.extend(access_operators(child))
    return found


def benchmark(session, query, repeats):
    client_ms, server_ms = [], []
    for _ in range(repeats + 1):
        started = perf_counter()
        rows, summary = session.execute_read(read_rows, query)
        client_ms.append((perf_counter() - started) * 1000)
        server_ms.append(summary.result_available_after + summary.result_consumed_after)
    plan = session.run("EXPLAIN " + query, PARAMETERS).consume().plan
    warm_ms = client_ms[1:]
    return rows, {
        "cold_ms": client_ms[0],
        "warm_median_ms": statistics.median(warm_ms),
        "warm_min_ms": min(warm_ms),
        "warm_max_ms": max(warm_ms),
        "server_median_ms": statistics.median(server_ms[1:]),
        "access": access_operators(plan),
    }


def pagerank(nodes, edges, damping=0.85, tolerance=1e-9, max_iterations=200):
    size = len(nodes)
    if size == 0:
        return {}, 0
    index = {node: position for position, node in enumerate(nodes)}
    incoming = [[] for _ in range(size)]
    out_degree = [0] * size
    for source, target in edges:
        incoming[index[target]].append(index[source])
        out_degree[index[source]] += 1
    rank = [1 / size] * size
    for iteration in range(1, max_iterations + 1):
        share = [
            value / degree if degree else 0.0 for value, degree in zip(rank, out_degree)
        ]
        dangling = sum(value for value, degree in zip(rank, out_degree) if not degree)
        base = (1 - damping + damping * dangling) / size
        lookup = share.__getitem__
        updated = [base + damping * sum(map(lookup, sources)) for sources in incoming]
        error = sum(abs(new - old) for new, old in zip(updated, rank))
        rank = updated
        if error < tolerance:
            break
    return dict(zip(nodes, rank)), iteration


def run_pagerank(session, k):
    started = perf_counter()
    nodes = session.run("MATCH (v:Video) RETURN v.id").value()
    edges = session.run("MATCH (a:Video)-->(b) RETURN a.id, b.id").values()
    fetched = perf_counter()
    scores, iterations = pagerank(nodes, edges)
    print(
        f"pagerank: fetch {fetched - started:.2f} s, "
        f"compute {perf_counter() - fetched:.2f} s, {iterations} iterations, "
        f"{len(nodes):,} nodes, {len(edges):,} edges",
        flush=True,
    )
    in_degree = Counter(target for _, target in edges)
    for node in sorted(scores, key=scores.get, reverse=True)[:k]:
        print(f"  {node}  score {scores[node]:.6f}  in-degree {in_degree[node]}")


def run(args):
    from neo4j import GraphDatabase

    uri, auth, database = connection_settings()
    with GraphDatabase.driver(uri, auth=auth, connection_timeout=30) as driver:
        driver.verify_connectivity()
        # Larger pulls cut round trips when PageRank fetches every edge.
        with driver.session(database=database, fetch_size=10_000) as session:
            for group, name, query in QUERIES:
                rows, timing = benchmark(session, query, args.repeats)
                print(
                    f"{name} [{group}]: {len(rows)} rows, "
                    f"cold {timing['cold_ms']:.1f} ms, "
                    f"warm median {timing['warm_median_ms']:.1f} ms "
                    f"({timing['warm_min_ms']:.1f}-{timing['warm_max_ms']:.1f}), "
                    f"server {timing['server_median_ms']:.0f} ms, "
                    f"{', '.join(timing['access'])}",
                    flush=True,
                )
                for row in rows[: args.rows]:
                    print(f"  {row}")
            if not args.skip_pagerank:
                run_pagerank(session, PARAMETERS["k"])


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--repeats", type=int, default=5, help="Warm runs after one cold run"
    )
    parser.add_argument(
        "--rows", type=int, default=3, help="Result rows printed per query"
    )
    parser.add_argument("--skip-pagerank", action="store_true")
    parser.add_argument(
        "--system-trust",
        action="store_true",
        help="Use the operating system's trusted TLS certificates (verification stays enabled)",
    )
    args = parser.parse_args(argv)
    if args.repeats <= 0 or args.rows < 0:
        parser.error("--repeats must be positive and --rows nonnegative")
    try:
        if args.system_trust:
            import truststore

            truststore.inject_into_ssl()
        run(args)
    except Exception as exc:
        print(f"Failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
