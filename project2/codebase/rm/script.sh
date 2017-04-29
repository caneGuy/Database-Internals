make
rm .*tbl *.stat
rmtest_create_tables
valgrind --leak-check=full --run-libc-freeres=no --leak-check=yes --show-leak-kinds=all --track-origins=yes -v --show-reachable=yes rmtest_delete_tables > out

