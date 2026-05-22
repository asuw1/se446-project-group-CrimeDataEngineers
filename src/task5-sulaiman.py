import sys
import csv

reader = csv.reader(sys.stdin)

for row in reader:
    if not row:
        continue # this will skip the emptyy lines

    # Skip header
    if row[0].strip() == "ID":
        continue  # skip header row

    # Make sure Arrest column exists
    if len(row) <= 8:
        continue  # make sure column exists

    arrest_status = row[8].strip().lower()  # will get Arrest column

    if arrest_status in ("true", "false"):
        print(f"{arrest_status}\t1")