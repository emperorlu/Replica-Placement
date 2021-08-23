#!/bin/bash



awk 'BEGIN {max = 0} {if ($1+0 > max+0) max=$1} END {print "Max=", max}' test.txt
awk 'BEGIN {min = 65536} {if ($1+0 < min+0) min=$1} END {print "Min=", min}' test.txt
cat test.txt|awk '{sum+=$1} END {print "Sum= ", sum}'
cat test.txt|awk '{sum+=$1} END {print "Avg= ", sum/NR}'
