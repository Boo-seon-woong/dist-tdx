CC := cc
CFLAGS := -D_POSIX_C_SOURCE=200809L -D_DEFAULT_SOURCE -std=c11 -O2 -Wall -Wextra -Iinclude
LDLIBS := -lpthread -lcrypto -libverbs
BIN_DIR := bin
SRC_DIR := src
BENCH_DIR := benchmark

ifneq ($(TDX_REAL_TDCALL),)
CFLAGS += -DTD_TDX_REAL_TDCALL
endif

COMMON_SRCS := \
	$(SRC_DIR)/common.c \
	$(SRC_DIR)/config.c \
	$(SRC_DIR)/tdx.c \
	$(SRC_DIR)/layout.c \
	$(SRC_DIR)/crypto.c \
	$(SRC_DIR)/cluster.c \
	$(SRC_DIR)/transport_tcp.c \
	$(SRC_DIR)/transport_rdma.c

all: $(BIN_DIR)/cn $(BIN_DIR)/mn $(BIN_DIR)/cn_bench $(BIN_DIR)/rdma_min

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

$(BIN_DIR)/cn: $(COMMON_SRCS) $(SRC_DIR)/cn_main.c | $(BIN_DIR)
	$(CC) $(CFLAGS) -o $@ $(COMMON_SRCS) $(SRC_DIR)/cn_main.c $(LDLIBS)

$(BIN_DIR)/mn: $(COMMON_SRCS) $(SRC_DIR)/mn_main.c | $(BIN_DIR)
	$(CC) $(CFLAGS) -o $@ $(COMMON_SRCS) $(SRC_DIR)/mn_main.c $(LDLIBS)

$(BIN_DIR)/cn_bench: $(COMMON_SRCS) $(BENCH_DIR)/cn_bench.c | $(BIN_DIR)
	$(CC) $(CFLAGS) -o $@ $(COMMON_SRCS) $(BENCH_DIR)/cn_bench.c $(LDLIBS) -lm

$(BIN_DIR)/rdma_min: $(SRC_DIR)/common.c $(SRC_DIR)/tdx.c $(BENCH_DIR)/rdma_min.c | $(BIN_DIR)
	$(CC) $(CFLAGS) -o $@ $(SRC_DIR)/common.c $(SRC_DIR)/tdx.c $(BENCH_DIR)/rdma_min.c $(LDLIBS)

clean:
	rm -f $(BIN_DIR)/cn $(BIN_DIR)/mn $(BIN_DIR)/cn_bench $(BIN_DIR)/rdma_min

.PHONY: all clean
