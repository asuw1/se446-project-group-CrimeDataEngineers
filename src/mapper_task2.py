import sys

for raw_line in sys.stdin:
    line = raw_line.strip()

    if not line:
        continue

    # Skip the CSV header row
    if line.startswith("ID,"):
        continue

    columns = line.split(",")

    # Primary Type is column index 5
    if len(columns) <= 5:
        continue

    primary_type = columns[5].strip()

    if primary_type:
        print(f"{primary_type}\t1")