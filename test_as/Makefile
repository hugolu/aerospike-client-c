###############################################################################
##  SETTINGS                                                                 ##
###############################################################################

# make AEROSPIKE=<PATH>
AEROSPIKE := ..

AS_HOST := 127.0.0.1
AS_PORT := 3000
AS_ARGS := -h $(AS_HOST) -p $(AS_PORT)

OS = $(shell uname)
ARCH = $(shell uname -m)
PLATFORM = $(OS)-$(ARCH)

CFLAGS = -std=gnu99 -g -Wall -fPIC -O3
CFLAGS += -fno-common -fno-strict-aliasing
CFLAGS += -march=nocona -DMARCH_$(ARCH)
CFLAGS += -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_GNU_SOURCE

ifeq ($(OS),Darwin)
  CFLAGS += -D_DARWIN_UNLIMITED_SELECT
else
  CFLAGS += -rdynamic
endif

CFLAGS += -I$(AEROSPIKE)/target/$(PLATFORM)/include

LDFLAGS = -lssl -lcrypto -lpthread
ifneq ($(OS),Darwin)
  LDFLAGS += -lrt -ldl
endif

# Use the Lua submodule?  [By default, yes.]
USE_LUAMOD = 1

# Use LuaJIT instead of Lua?  [By default, no.]
USE_LUAJIT = 0

# Permit easy overriding of the default.
ifeq ($(USE_LUAJIT),1)
  USE_LUAMOD = 0
endif

ifeq ($(and $(USE_LUAMOD:0=),$(USE_LUAJIT:0=)),1)
  $(error Only at most one of USE_LUAMOD or USE_LUAJIT may be enabled (i.e., set to 1.))
endif

ifeq ($(USE_LUAJIT),1)
  ifeq ($(OS),Darwin)
    LDFLAGS += -pagezero_size 10000 -image_base 100000000
  endif
else
  ifeq ($(USE_LUAMOD),0)
    # Find where the Lua development package is installed in the build environment.
    ifeq ($(OS),Darwin)
      LUA_LIBPATH = $(or \
	$(wildcard /usr/local/lib/liblua.5.1.dylib), \
	$(wildcard /usr/local/lib/liblua.5.1.a), \
	$(wildcard /usr/local/lib/liblua.dylib), \
	$(wildcard /usr/local/lib/liblua.a), \
	  $(error Cannot find liblua 5.1))
      LUA_LIBDIR = $(dir $(LUA_LIBPATH))
      LUA_LIB = $(patsubst lib%,%,$(basename $(notdir $(LUA_LIBPATH))))
    else
      # Linux
      LUA_LIBPATH = $(or \
	$(wildcard /usr/lib/liblua5.1.so), \
	$(wildcard /usr/lib/liblua5.1.a), \
	$(wildcard /usr/lib/x86_64-linux-gnu/liblua5.1.so), \
	$(wildcard /usr/lib/x86_64-linux-gnu/liblua5.1.a), \
	$(wildcard /usr/lib64/liblua-5.1.so), \
	$(wildcard /usr/lib64/liblua-5.1.a), \
	$(wildcard /usr/lib/liblua.so), \
	$(wildcard /usr/lib/liblua.a), \
	  $(error Cannot find liblua 5.1))
      LUA_LIBDIR = $(dir $(LUA_LIBPATH))
      LUA_LIB = $(patsubst lib%,%,$(basename $(notdir $(LUA_LIBPATH))))
    endif
    LDFLAGS += -L$(LUA_LIBDIR) -l$(LUA_LIB)
  endif
endif

LDFLAGS += -lm

ifeq ($(OS),Darwin)
  CC = clang
else
  CC = gcc
endif

###############################################################################
##  MAIN TARGETS                                                             ##
###############################################################################

TARGETS = test_all test_clear test_write test_read

all: build

.PHONY: build
build: $(TARGETS)

.PHONY: clean
clean:
	@rm -f *.o $(TARGETS)

test_all: test.c
	$(CC) -o $@ $^ $(AEROSPIKE)/target/$(PLATFORM)/lib/libaerospike.a $(LDFLAGS) $(CFLAGS) -DTEST_CLEAR -DTEST_WRITE -DTEST_READ

test_clear: test.c
	$(CC) -o $@ $^ $(AEROSPIKE)/target/$(PLATFORM)/lib/libaerospike.a $(LDFLAGS) $(CFLAGS) -DTEST_CLEAR

test_write: test.c
	$(CC) -o $@ $^ $(AEROSPIKE)/target/$(PLATFORM)/lib/libaerospike.a $(LDFLAGS) $(CFLAGS) -DTEST_WRITE

test_read: test.c
	$(CC) -o $@ $^ $(AEROSPIKE)/target/$(PLATFORM)/lib/libaerospike.a $(LDFLAGS) $(CFLAGS) -DTEST_READ
