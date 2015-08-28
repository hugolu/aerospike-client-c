/*******************************************************************************
 * Copyright 2008-2013 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/


//==========================================================
// Includes
//

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


//==========================================================
// Large Stack Data Example
//

int
main(int argc, char* argv[])
{
	aerospike as;
	as_error err;
	as_boolean ldt_exists;
	as_ldt lstack, lstack2; 
	as_integer ival;
	as_string sval;
    as_bytes bval;
	uint32_t n_elements, cap_size;
	as_arraylist_iterator it;
	as_list* p_list = NULL;

	// Parse command line arguments.
	if (! example_get_opts(argc, argv, EXAMPLE_BASIC_OPTS)) {
		exit(-1);
	}

	// Connect to the aerospike database cluster.
	example_connect_to_aerospike(&as);

	// Start clean.
	example_remove_test_record(&as);

	// Create a large stack object to use. No need to destroy lstack if using
	// as_ldt_init() on stack object.
	as_ldt_init(&lstack, "mystack", AS_LDT_LSTACK, NULL);

	// Verify that the LDT is not already there.
	as_boolean_init(&ldt_exists, false);
	assert (aerospike_lstack_ldt_exists(&as, &err, NULL, &g_key, &lstack, &ldt_exists) == AEROSPIKE_OK);
	assert (as_boolean_get(&ldt_exists) == false);
	LOG("verified that lstack ldt is not present");

	// Push a few values onto the stack.
	// No need to destroy sval if using as_string_init() on stack object.

    // Push an integer
	as_integer_init(&ival, 123);
	assert (aerospike_lstack_push(&as, &err, NULL, &g_key, &lstack, (const as_val*)&ival) == AEROSPIKE_OK);

    // Push a string
	as_string_init(&sval, "string stack value", false);
	assert (aerospike_lstack_push(&as, &err, NULL, &g_key, &lstack, (const as_val*)&sval) == AEROSPIKE_OK);

    // Push bytes
    char buf[16] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
    as_bytes_init_wrap(&bval, buf, 16, false);
    assert (aerospike_lstack_push(&as, &err, NULL, &g_key, &lstack, (const as_val*)&bval) == AEROSPIKE_OK);


	// Look at the stack size right now.
	assert (aerospike_lstack_size(&as, &err, NULL, &g_key, &lstack, &n_elements) == AEROSPIKE_OK);
	LOG("%d values pushed", n_elements);

	// Peek a few values back.
	as_ldt_init(&lstack2, "mystack", AS_LDT_LSTACK, NULL);

	assert (aerospike_lstack_peek(&as, &err, NULL, &g_key, &lstack2, 3, &p_list) == AEROSPIKE_OK);

	// See if the elements match what we expect.
	as_arraylist_iterator_init(&it, (const as_arraylist*)p_list);
	while (as_arraylist_iterator_has_next(&it)) {
		const as_val* p_val = as_arraylist_iterator_next(&it);
		char* p_str = as_val_tostring(p_val);
		LOG("   peek - type = %d, value = %s", as_val_type(p_val), p_str);
		free(p_str);
	}
	as_list_destroy(p_list);
	p_list = NULL;

	// Push 3 more items onto the stack. By using as_arraylist_inita() we avoid
	// some but not all internal heap usage, so we must call
	// as_arraylist_destroy().
	as_arraylist vals;
	as_arraylist_inita(&vals, 3);
	as_arraylist_append_int64(&vals, 1000);
	as_arraylist_append_int64(&vals, 2000);
	as_arraylist_append_int64(&vals, 3000);

	assert (aerospike_lstack_push_all(&as, &err, NULL, &g_key, &lstack, (const as_list*)&vals) == AEROSPIKE_OK);
	as_arraylist_destroy(&vals);
	LOG("3 more values pushed");

	// Peek all the values back again.
	as_ldt_init(&lstack2, "mystack", AS_LDT_LSTACK, NULL);
	assert (aerospike_lstack_peek(&as, &err, NULL, &g_key, &lstack2, 10, &p_list) == AEROSPIKE_OK);

	// See if the elements match what we expect.
	as_arraylist_iterator_init(&it, (const as_arraylist*)p_list);
	while (as_arraylist_iterator_has_next(&it)) {
		const as_val* p_val = as_arraylist_iterator_next(&it);
		char* p_str = as_val_tostring(p_val);

		LOG("   peek - type = %d, value = %s", as_val_type(p_val), p_str);
		free(p_str);
	}
	as_list_destroy(p_list);
    p_list = NULL;

	// Set capacity for the lstack.
	assert (aerospike_lstack_set_capacity(&as, &err, NULL, &g_key, &lstack, 10000) == AEROSPIKE_OK);

	// Get capacity from the lstack.
	assert (aerospike_lstack_get_capacity(&as, &err, NULL, &g_key, &lstack, &cap_size) == AEROSPIKE_OK);
	assert (cap_size == 10000);

	// Verify that the LDT is now present.
	as_boolean_init(&ldt_exists, false);
	assert (aerospike_lstack_ldt_exists(&as, &err, NULL, &g_key, &lstack, &ldt_exists) == AEROSPIKE_OK);
	assert (as_boolean_get(&ldt_exists) == true);
	LOG("verified that lstack ldt is present");

	// Destroy the lstack.
	assert (aerospike_lstack_destroy(&as, &err, NULL, &g_key, &lstack) == AEROSPIKE_OK);

	// See if we can still do any lstack operations.
	assert (aerospike_lstack_size(&as, &err, NULL, &g_key, &lstack, &n_elements) != AEROSPIKE_OK);

	// Cleanup and disconnect from the database cluster.
	example_cleanup(&as);
	LOG("lstack example successfully completed");

	return 0;
}
