make
rm *.tbl *.stat
rmtest_create_tables
valgrind  -v --leak-check=full --show-leak-kinds=all rmtest 

