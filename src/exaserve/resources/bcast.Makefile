CC=mpicc
CFLAGS=-O2 -Wall -Wextra -Werror

.PHONY: all clean
all: bcast

bcast: bcast.c
	$(CC) $(CFLAGS) -o $@ $<

clean:
	rm -f bcast *.o
	rm -rf *.dSYM
