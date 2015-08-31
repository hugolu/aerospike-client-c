#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <alloca.h>
#include <assert.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_lstack.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_arraylist_iterator.h>
#include <aerospike/as_error.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_ldt.h>
#include <aerospike/as_list.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_val.h>

#include "example_utils.h"

#define ERROR(FORMAT, ...) printf("[ERROR] " FORMAT "\n", __VA_ARGS__)
#define WARN(FORMAT, ...) printf("[WARN] " FORMAT "\n", __VA_ARGS__)
#define INFO(FORMAT, ...) printf("[INFO] " FORMAT "\n", __VA_ARGS__)

#define MAX(A, B) ((A) > (B) ? (A) : (B))
#define MIN(A, B) ((A) < (B) ? (A) : (B))

#define CHUNK_SIZE  (512*1024)
#define BUFFER_SIZE (8*CHUNK_SIZE)

as_status asc_raw_write(aerospike* p_as, as_key* p_key, uint8_t *buf, uint32_t size);
int asc_raw_read(aerospike* p_as, as_key* p_key, uint8_t *buf, uint32_t size);
as_status asc_raw_size(aerospike* p_as, as_key* p_key, uint32_t *size);

//==========================================================
// Large Stack Data Example
//

uint8_t wbuf[BUFFER_SIZE];
uint8_t rbuf[BUFFER_SIZE];

int
main(int argc, char* argv[])
{
	aerospike as;
    as_status status;
	as_error err;
	uint32_t size;

    uint32_t *ptr = (uint32_t *)wbuf;
    int i;
    for (i = 0; i < BUFFER_SIZE/4; i++) {
        ptr[i] = i;
    }

	// Parse command line arguments.
	if (! example_get_opts(argc, argv, EXAMPLE_BASIC_OPTS)) {
		exit(-1);
	}

	// Connect to the aerospike database cluster.
	example_connect_to_aerospike(&as);

	// Start clean.
    aerospike_key_remove(&as, &err, NULL, &g_key);

    // Write bytes
    status = asc_raw_write(&as, &g_key, wbuf, BUFFER_SIZE);
    assert(status == AEROSPIKE_OK);

	// Count bytes written
    status = asc_raw_size(&as, &g_key, &size);
    assert (status == AEROSPIKE_OK);
	LOG("%d bytes pushed", size);

    // Read bytes
    status = asc_raw_read(&as, &g_key, rbuf, BUFFER_SIZE);
    assert (status == AEROSPIKE_OK);

	// Cleanup and disconnect from the database cluster.
	aerospike_close(&as, &err);
	aerospike_destroy(&as);
	LOG("lstack example successfully completed");

    // compare result
    assert(memcmp(wbuf, rbuf, BUFFER_SIZE)==0);

	return 0;
}

as_status
asc_raw_write(aerospike* p_as, as_key* p_key, uint8_t *buf, uint32_t size)
{
    as_ldt lstack;
    as_bytes *p_bval;
    as_error err;
    as_status status;
    uint32_t cnt = (size+(CHUNK_SIZE-1))/CHUNK_SIZE;
    uint32_t offset;

    // Create a large stack object to use
	as_ldt_init(&lstack, "data", AS_LDT_LSTACK, NULL);

    // Make arraylist
    as_arraylist vals;
    as_arraylist_inita(&vals, cnt);

    p_bval = alloca(cnt * sizeof(as_bytes));
    for (offset = 0; offset < size; offset += CHUNK_SIZE, p_bval++) {
        as_bytes_init_wrap(p_bval, buf + offset, CHUNK_SIZE, false);
        as_arraylist_insert_bytes(&vals, 0, p_bval);
    }

    // Push bytes
    status = aerospike_lstack_push_all(p_as, &err, NULL, p_key, &lstack, (as_list *)&vals);
    if (status != AEROSPIKE_OK) {
        ERROR("aerospike_lstack_push_all() - returned %d - %s", err.code, err.message);
        return status;
    }

    // Write metadata
    as_record rec;
    as_record_inita(&rec, 1);
    as_record_set_int64(&rec, "size", size);
    aerospike_key_put(p_as, &err, NULL, p_key, &rec);

    return AEROSPIKE_OK;
}

as_status
asc_raw_size(aerospike* p_as, as_key* p_key, uint32_t *size)
{
    as_status status;
    as_error err;

    // Read metadata
    as_record *rec = NULL;
    status = aerospike_key_get(p_as, &err, NULL, p_key, &rec);
    if (status != AEROSPIKE_OK) {
        ERROR("aerospike_key_get() returned %d - %s", err.code, err.message);
        return status;
    }
    *size = as_record_get_int64(rec, "size", 0);
    as_record_destroy(rec);

    return AEROSPIKE_OK;
}

as_status
asc_raw_read(aerospike* p_as, as_key* p_key, uint8_t *buf, uint32_t size)
{
    as_ldt lstack;
    as_status status;
    as_error err;
	as_list* p_list = NULL;
    uint32_t lstack_size;
    uint32_t offset, chksize;

    // Create a large stack object to use
	as_ldt_init(&lstack, "data", AS_LDT_LSTACK, NULL);

    // Get stack size
    status = aerospike_lstack_size(p_as, &err, NULL, p_key, &lstack, &lstack_size);
    if (status != AEROSPIKE_OK) {
        ERROR("aerospike_lstack_size() returned %d - %s", err.code, err.message);
        return status;
    }

	// Peek all the values back again.
	status = aerospike_lstack_peek(p_as, &err, NULL, p_key, &lstack, lstack_size, &p_list);
    if (status != AEROSPIKE_OK) {
        ERROR("aerospike_lstack_peek() returned %d - %s", err.code, err.message);
        return status;
    }

    // Read the content
    as_val** p_val = ((const as_arraylist*)p_list)->elements;
    for (offset = 0; offset < size; offset += chksize) {
        const as_bytes* p_bytes = (const as_bytes*)*p_val++;
        chksize = MIN(size - offset, p_bytes->size);
        memcpy(buf + offset, p_bytes->value, chksize);
    }
	as_list_destroy(p_list);
    p_list = NULL;

    return AEROSPIKE_OK;
}
