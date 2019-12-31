set terminal pdf
set datafile separator ","
set style fill solid border rgb "black"
set output 'delta.pdf'
set auto x
set xrange [0:1]
set auto y
set yrange [0:1]
set zrange [0:1]
set grid
set hidden3d
set dgrid3d 50,50 qnorm 2
set ticslevel 0
set style data lines
set xlabel "Fairness"
set ylabel "c/ci"
set zlabel "Delta"
splot  1-x-y with lines lc "blue" title 'Our delta',\
      .1 with lines lc "red" title 'Threasold'