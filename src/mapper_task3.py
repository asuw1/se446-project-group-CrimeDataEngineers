#!/usr/bin/env python3
import sys
import csv

reader = csv.reader(sys.stdin)

for row in reader:
    try:
        if not row:
            continue

        if row[0] == "ID":
            continue

        if len(row) <= 7:
            continue

        location_description = row[7].strip()

        if location_description:
            print(f"{location_description}\t1")
    except Exception:
        continue