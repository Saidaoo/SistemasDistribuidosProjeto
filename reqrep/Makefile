CC = gcc
CFLAGS = -Wall -Wextra -std=c99 -pedantic -pthread
LIBS = -lzmq
TARGET = cliente

$(TARGET): cliente.c cliente.h
	$(CC) $(CFLAGS) -o $(TARGET) cliente.c $(LIBS)

clean:
	rm -f $(TARGET)

.PHONY: clean