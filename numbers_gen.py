import sys
import random

number_count = int(sys.argv[1])
output_path = str(sys.argv[2])

with open(output_path, 'w') as f:
    for i in range(number_count):
        number = random.randrange(1e14, 1e16)
        f.write(str(number) + '\n')