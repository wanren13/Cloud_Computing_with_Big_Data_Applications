reset
set term png truecolor
set output "hw9_p1b.png"
set xlabel "User ID"
set ylabel "Average Rating"
# set grid
set boxwidth 2 absolute
# set style fill transparent solid 0.5 noborder
plot "part-r-00000" u 1:2 w boxes lc rgb"green" notitle