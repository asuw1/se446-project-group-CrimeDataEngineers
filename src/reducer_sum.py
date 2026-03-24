#!/usr/bin/env python3
import sys

current_key = None
current_total = 0

for line in sys.stdin:
    line = line.strip()

    try:
        # split by tab since thats what the mapper gives us
        key, count = line.split('\t', 1)
        count = int(count)
    except ValueError:
        continue

    if current_key == key:
        current_total += count
    else:
        # if the key changes, print what we counted so far
        if current_key:
            print(f"{current_key}\t{current_total}")
        current_key = key
        current_total = count

# print the very last one
if current_key:
    print(f"{current_key}\t{current_total}")
