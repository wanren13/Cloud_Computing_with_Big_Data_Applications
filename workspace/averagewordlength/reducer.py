#!/usr/bin/env python

from operator import itemgetter
import sys

current_char = None
current_count = 0
length_sum = 0
word = None

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    char, len = line.split('\t', 1)

    # convert count (currently a string) to int
    try:
        len = int(len)
    except ValueError:
        # len was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    if current_char == char:
        current_count += 1
        length_sum += len
    else:
        if current_char:
            # write result to STDOUT
            print '%c\t%.1f' % (current_char, length_sum/float(current_count))
        current_count = 1
        length_sum = len
        current_char = char

if current_char == char:
    print '%c\t%.1f' % (current_char, length_sum/float(current_count))

