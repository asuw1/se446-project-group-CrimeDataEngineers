import sys

current_key = None
current_sum = 0

for line in sys.stdin: # Read mapper output
    line = line.strip()
    if not line:
        continue  # skip empty lines

    key, value = line.split("\t")
    value = int(value)

    if key == current_key:
        current_sum += value  # adds values for the same key
    else:
        if current_key is not None:
            print(f"{current_key}\t{current_sum}")  # prints the previouss result
        current_key = key
        current_sum = value

if current_key is not None:
    print(f"{current_key}\t{current_sum}") # prints the last key result