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

#include <inttypes.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <assert.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_llist.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_arraylist_iterator.h>
#include <aerospike/as_boolean.h>
#include <aerospike/as_error.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_key.h>
#include <aerospike/as_ldt.h>
#include <aerospike/as_list.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_string.h>
#include <aerospike/as_val.h>

#include "example_utils.h"

#define ERROR(_fmt, _args...) printf("[ERROR] %s:%d " _fmt "\n", __FILE__, __LINE__, ##_args)

bool as_llist_exists(aerospike *p_as, as_key *p_key, as_ldt *p_llist);
int as_llist_add_integers(aerospike *p_as, as_ldt *p_llist, int *values, int size);
int as_llist_get_size(aerospike *p_as, as_key *p_key, as_ldt *p_llist);
as_list* as_llist_filter(aerospike *p_as, as_key *p_key, as_ldt *p_llist);
as_list* as_llist_scan(aerospike *p_as, as_key *p_key, as_ldt *p_llist);

typedef void (*cook_as_val_func)(const as_val* p_val);
void cook_as_integer(const as_val* p_val);
void as_arraylist_cook(const as_arraylist* p_list, cook_as_val_func cook);

//==========================================================
// Large List Data Example
//

int
main(int argc, char* argv[])
{
	aerospike as;
	as_ldt llist;
	as_error err;
	as_list* p_list;

	int example_values[4] = { 1, 3, 2, 4 };

	// Parse command line arguments.
	if (! example_get_opts(argc, argv, EXAMPLE_BASIC_OPTS)) {
		exit(-1);
	}

	// Connect to the aerospike database cluster.
	example_connect_to_aerospike(&as);

	// Start clean.
	example_remove_test_record(&as);

	// Create a large list object to use. No need to destroy llist if using
	// as_ldt_init() on stack object.
	as_ldt_init(&llist, "myllist", AS_LDT_LLIST, NULL);

	// Verify that the LDT is not already there.
    assert(as_llist_exists(&as, &g_key, &llist) == false);
	LOG("verified that llist ldt is not present");

	// No need to destroy ival if using as_integer_init() on stack object.
    as_llist_add_integers(&as, &llist, example_values, 4);

	// See how many elements we have in the list now.
    assert(as_llist_get_size(&as, &g_key, &llist) == 4);

	// Get all the values back and print them. Make sure they are ordered.
    p_list = as_llist_scan(&as, &g_key, &llist);
    as_arraylist_cook((const as_arraylist*)p_list, cook_as_integer);
	as_list_destroy(p_list);
	p_list = NULL;

	// Destroy the list.
    assert(as_llist_exists(&as, &g_key, &llist) == true);
	aerospike_llist_destroy(&as, &err, NULL, &g_key, &llist);
    assert(as_llist_exists(&as, &g_key, &llist) == false);
	LOG("llist destroyed and checked");

	// Cleanup and disconnect from the database cluster.
	example_cleanup(&as);
	LOG("llist example successfully completed");

	return 0;
}

/////////////////////////////////////////////////////////////////////////////

bool
as_llist_exists(aerospike *p_as, as_key *p_key, as_ldt *p_llist)
{
	as_error err;
    as_boolean ldt_exists;

	as_boolean_init(&ldt_exists, false);

	if (aerospike_llist_ldt_exists(p_as, &err, NULL, p_key, p_llist,
			&ldt_exists) != AEROSPIKE_OK) {
        ERROR("aerospike_llist_ldt_exists");
	}

    return as_boolean_get(&ldt_exists);
}

int
as_llist_add_integers(aerospike *p_as, as_ldt *p_llist, int *values, int size)
{
	as_error err;
    int i;
#if 0
    for (i = 0; i < size; i++) {
        as_integer ival;
        as_integer_init(&ival, values[i]);
        if (aerospike_llist_add(p_as, &err, NULL, &g_key, p_llist,
                (const as_val*)&ival) != AEROSPIKE_OK) {
            return -1;
        }
    }

#else
    as_arraylist vals;
    as_arraylist_inita(&vals, size);
    for (i = 0; i < size; i++) {
        printf("   insert %d\n", values[i]);
        as_arraylist_append_integer(&vals, as_integer_new(values[i]));
    }
    assert(aerospike_llist_add_all(p_as, &err, NULL, &g_key, p_llist, (as_list*)&vals) == AEROSPIKE_OK);
#endif
    return 0;
}

int
as_llist_get_size(aerospike *p_as, as_key *p_key, as_ldt *p_llist)
{
	as_error err;
	uint32_t n_elements;

	// See how many elements we have in the list now.
	if (aerospike_llist_size(p_as, &err, NULL, p_key, p_llist, &n_elements) !=
			AEROSPIKE_OK) {
        ERROR("aerospike_llist_size");
        return -1;
	}

    return n_elements;
} 

as_list*
as_llist_filter(aerospike *p_as, as_key *p_key, as_ldt *p_llist)
{
	as_error err;
	as_list* p_list = NULL;

    assert(aerospike_llist_filter(p_as, &err, NULL, p_key, p_llist, NULL, NULL, &p_list) == AEROSPIKE_OK);

    return p_list;
}

as_list*
as_llist_scan(aerospike *p_as, as_key *p_key, as_ldt *p_llist)
{
    as_error err;
    as_list *p_list = NULL;

    assert(aerospike_llist_scan(p_as, &err, NULL, p_key, p_llist, &p_list) == AEROSPIKE_OK);

    return p_list;
}

void
as_arraylist_cook(const as_arraylist* p_list, cook_as_val_func cook)
{
	as_arraylist_iterator it;
	as_arraylist_iterator_init(&it, (const as_arraylist*)p_list);

	int item_count = 0;
	while (as_arraylist_iterator_has_next(&it)) {
		const as_val* p_val = as_arraylist_iterator_next(&it);
        cook(p_val);
		item_count++;
	}
}

void
cook_as_integer(const as_val* p_val)
{
    char* p_str = as_val_tostring(p_val);
    int ival;

    // Make sure it's integer type.
    assert(as_val_type(p_val) == AS_INTEGER);
    ival = (int)as_integer_get((const as_integer*)p_val);

    LOG("   element - type = %d, value = %s(%d)", as_val_type(p_val), p_str, ival);
    free(p_str);
}
