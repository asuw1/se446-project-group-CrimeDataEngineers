import sys

current_key = None
current_count = 0

for line in sys.stdin:
    line = line.strip()

    if not line:
        continue

    parts = line.split("\t")

    if len(parts) != 2:
        continue

    key, value = parts[0], int(parts[1])

    if current_key == key:
        current_count += value
    else:
        if current_key is not None:
            print(f"{current_key}\t{current_count}")
        current_key = key
        current_count = value

# print last key
if current_key is not None:
    print(f"{current_key}\t{current_count}")