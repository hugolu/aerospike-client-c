#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
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

#define CHUNK_SIZE  (512*1024)
#define BUFFER_SIZE (8*CHUNK_SIZE)

struct as_bytes_stat_s {
    int64_t count;
    int64_t size;
} as_bytes_stat;

int as_write_bytes(aerospike* p_as, as_key* p_key, uint8_t *buf, int count);
int as_read_bytes(aerospike* p_as, as_key* p_key, uint8_t *buf, int size);
uint32_t as_size(aerospike* p_as, as_key* p_key);

//==========================================================
// Large Stack Data Example
//

uint8_t wbuf[BUFFER_SIZE];
uint8_t rbuf[BUFFER_SIZE];

int
main(int argc, char* argv[])
{
	aerospike as;
	as_error err;
	as_ldt lstack;//, lstack2; 
	uint32_t n_elements;
	//as_arraylist_iterator it;
	//as_list* p_list = NULL;

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
	example_remove_test_record(&as);

	as_ldt_init(&lstack, "mystack", AS_LDT_LSTACK, NULL);
    as_write_bytes(&as, &g_key, wbuf, BUFFER_SIZE);

	// Look at the stack size right now.
    n_elements = as_size(&as, &g_key);
	LOG("%d values pushed", n_elements);

#if 0
	// Peek all the values back again.
	as_ldt_init(&lstack2, "mystack", AS_LDT_LSTACK, NULL);
	assert (aerospike_lstack_peek(&as, &err, NULL, &g_key, &lstack2, n_elements, &p_list) == AEROSPIKE_OK);

	// See if the elements match what we expect.
	as_arraylist_iterator_init(&it, (const as_arraylist*)p_list);
	while (as_arraylist_iterator_has_next(&it)) {
		const as_val* p_val = as_arraylist_iterator_next(&it);

#if 0
		char* p_str = as_val_tostring(p_val);
		LOG("   peek - type = %d, value = %s", as_val_type(p_val), p_str);
		free(p_str);
#endif
	}
	as_list_destroy(p_list);
    p_list = NULL;
#endif
    as_read_bytes(&as, &g_key, rbuf, BUFFER_SIZE);

    assert(memcmp(wbuf, rbuf, BUFFER_SIZE)==0);
	// Destroy the lstack.
	assert (aerospike_lstack_destroy(&as, &err, NULL, &g_key, &lstack) == AEROSPIKE_OK);

	// Cleanup and disconnect from the database cluster.
	example_cleanup(&as);
	LOG("lstack example successfully completed");

	return 0;
}

int
as_write_bytes(aerospike* p_as, as_key* p_key, uint8_t *buf, int count)
{
    as_ldt lstack;
    as_bytes bval;
    as_error err;
    int size = (count+(CHUNK_SIZE-1))/CHUNK_SIZE;
    int i;

	// Create a large stack object to use. No need to destroy lstack if using
	// as_ldt_init() on stack object.
	as_ldt_init(&lstack, "mystack", AS_LDT_LSTACK, NULL);

    // Push bytes
    for (i = 0; i < count; i += CHUNK_SIZE) {
        as_bytes_init_wrap(&bval, buf + i, CHUNK_SIZE, false);
        assert (aerospike_lstack_push(p_as, &err, NULL, p_key, &lstack, (const as_val*)&bval) == AEROSPIKE_OK);
    }

    as_record rec;
    as_record_inita(&rec, 2);
    as_record_set_int64(&rec, "count", count);
    as_record_set_int64(&rec, "size", size);
    printf("1.count=%d, size=%d\n", count, size);
    aerospike_key_put(p_as, &err, NULL, p_key, &rec);
    return 0;
}

uint32_t
as_size(aerospike* p_as, as_key* p_key)
{
    as_ldt lstack;
    as_error err;
	uint32_t n_elements;

	as_ldt_init(&lstack, "mystack", AS_LDT_LSTACK, NULL);
	assert (aerospike_lstack_size(p_as, &err, NULL, p_key, &lstack, &n_elements) == AEROSPIKE_OK);

    as_record *rec = NULL;
    assert (aerospike_key_get(p_as, &err, NULL, p_key, &rec) == AEROSPIKE_OK);
    int64_t size = as_record_get_int64(rec, "size", 0);
    as_record_destroy(rec);
    printf("2.size=%ld\n", size);

    return n_elements;
}

int
as_read_bytes(aerospike* p_as, as_key* p_key, uint8_t *buf, int count)
{
    as_ldt lstack;
    as_error err; int size = (count+(CHUNK_SIZE-1))/CHUNK_SIZE;
	as_list* p_list = NULL;
	as_arraylist_iterator it;

	as_ldt_init(&lstack, "mystack", AS_LDT_LSTACK, NULL);

	// Peek all the values back again.
	assert (aerospike_lstack_peek(p_as, &err, NULL, p_key, &lstack, size, &p_list) == AEROSPIKE_OK);

	// See if the elements match what we expect.
    const as_arraylist* p_array = (const as_arraylist*)p_list;
    int i;
    for (i = 0; i < size; i++) {
        const as_val* p_val = p_array->elements[i];
        const as_bytes* p_bytes = (const as_bytes*)p_val;
		LOG("   [%d] peek - type = %d, size = %d", i, as_val_type(p_val), p_bytes->size);
        memcpy(buf + (size-1-i)*CHUNK_SIZE, p_bytes->value, p_bytes->size);
    }
	as_list_destroy(p_list);
    p_list = NULL;

    return 0;
}
