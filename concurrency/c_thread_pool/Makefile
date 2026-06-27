CC = gcc
CFLAGS   = -Wall -Wextra

main_test: main_test.c thread_pool.c
	$(CC) $(CFLAGS) $^ -o $@ -lpthread
	./$@
	rm -rf ./$@