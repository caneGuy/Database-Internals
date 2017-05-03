make
rm *.tbl *.stat
rmtest_create_tables
valgrind  --leak-check=full --show-leak-kinds=all rmtest 

