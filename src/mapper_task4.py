#!/usr/bin/env python3
import sys

# date is the 3rd column in the csv (index 2)
DATE_IDX = 2

for line in sys.stdin:
    line = line.strip()

    if not line:
        continue

    # skip the first row with headers
    if line.startswith("ID,"):
        continue

    parts = line.split(',')

    if len(parts) <= DATE_IDX:
        continue

    date_field = parts[DATE_IDX]

    try:
        # grab the actual date before the time part
        date_only = date_field.split(' ')[0]
        
        # split it up to get month, day, and year
        date_parts = date_only.split('/')
        
        if len(date_parts) == 3:
            year = date_parts[2]
            
            # send the year and a count of 1 to be reduced
            print(f"{year}\t1")
            
    except Exception:
        pass
