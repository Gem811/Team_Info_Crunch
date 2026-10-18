"""Make a deterministic, explicitly edge-sampled copy for validaton"""

import argparse
import csv
import json
import shutil
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--edge-stride", type=int, default=3)
    args = parser.parse_args()
    if args.edge_stride < 1 or args.input.resolve() == args.output.resolve():
        parser.error("Use a positive stride and a distinct output directory")
    args.output.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(args.input / "nodes.csv", args.output / "nodes.csv")
    total = kept = 0
    with (
        (args.input / "edges.csv").open(encoding="utf-8", newline="") as source,
        (args.output / "edges.csv").open(
            "w", encoding="utf-8", newline=""
        ) as destination,
    ):
        reader, writer = csv.reader(source), csv.writer(destination)
        writer.writerow(next(reader))
        for index, row in enumerate(reader):
            total += 1
            if index % args.edge_stride == 0:
                writer.writerow(row)
                kept += 1
    report = {
        "method": "All nodes; every Nth edge in prepared CSV order, starting at index 0",
        "edge_stride": args.edge_stride,
        "input_edges": total,
        "output_edges": kept,
        "limitation": "Not a connected-subgraph or statistically representative sample",
    }
    (args.output / "review_subset.json").write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(report))


if __name__ == "__main__":
    main()
