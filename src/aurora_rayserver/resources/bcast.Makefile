CC=mpicc
CFLAGS=-O2 -Wall

.PHONY: all clean
all: bcast gather

bcast: bcast.c
	$(CC) $(CFLAGS) -o $@ $<

gather: gather.c
	$(CC) $(CFLAGS) -o $@ $<

clean:
	rm -f bcast gather *.o
	rm -rf *.dSYM
