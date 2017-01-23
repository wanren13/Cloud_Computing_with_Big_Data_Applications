#!/usr/bin/env python

import sys

for line in sys.stdin:
	line = line.strip()
	words = line.split()
	for word in words:
		print '%c\t%d' % (word[0], len(word))
