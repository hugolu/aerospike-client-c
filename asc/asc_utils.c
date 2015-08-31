#include "asc_utils.h"
#include <alloca.h>

bool asc_init(aerospike *p_as)
{
    as_status status;
    as_error err;

    // Start with default configuration.
    as_config cfg;
    as_config_init(&cfg);
    as_config_add_host(&cfg, HOST, PORT);
    as_config_set_user(&cfg, USER, PASS);
    aerospike_init(p_as, &cfg);

    // Connect to the Aerospike database cluster.
    status = aerospike_connect(p_as, &err);
    if (status != AEROSPIKE_OK) {
        ERROR("aerospike_connect() returned %d - %s", err.code, err.message);
        aerospike_destroy(p_as);
    }

    return (status == AEROSPIKE_OK);
}

bool asc_exit(aerospike *p_as)
{
    as_status status;
    as_error err;

    // Disconnect from the database cluster and clean up the aerospike object.
    status = aerospike_close(p_as, &err);
    if (status != AEROSPIKE_OK) {
        ERROR("aerospike_close() returned %d - %s", err.code, err.message);
    }
    aerospike_destroy(p_as);

    return (status == AEROSPIKE_OK);
}

#ifdef LDT_ENABLE
static bool asc_raw_write(aerospike* p_as, as_key* p_key, uint8_t *buf, uint32_t size)
{
    as_error err;
    as_status status;

    uint32_t lstack_size = (size+(CHUNK_SIZE-1))/CHUNK_SIZE;
    uint32_t offset, chksize;

    // Create a large stack object to use
    as_ldt lstack;
	as_ldt_init(&lstack, "data", AS_LDT_LSTACK, NULL);

    // Make arraylist
    as_arraylist vals;
    as_arraylist_inita(&vals, lstack_size);

    as_bytes *p_bval;
    p_bval = (as_bytes *)alloca(lstack_size * sizeof(as_bytes));
    for (offset = 0; offset < size; offset += chksize, p_bval++) {
        chksize = MIN(size - offset, CHUNK_SIZE);
        as_bytes_init_wrap(p_bval, buf + offset, chksize, false);
        as_arraylist_insert_bytes(&vals, 0, p_bval);
    }

    // Push bytes
#if 1
    // FIXME it's a workaround
    uint32_t i;
    for (i = 0; i < vals.size; i++) {
        status = aerospike_lstack_push(p_as, &err, NULL, p_key, &lstack, vals.elements[i]);
        if (status != AEROSPIKE_OK) {
            ERROR("aerospike_lstack_push() - returned %d - %s", err.code, err.message);
            return false;
        }
    }
#else
    status = aerospike_lstack_push_all(p_as, &err, NULL, p_key, &lstack, (as_list *)&vals);
    if (status != AEROSPIKE_OK) {
        ERROR("aerospike_lstack_push_all() - returned %d - %s", err.code, err.message);
        return false;
    }
#endif

    // Write metadata
    as_record rec;
    as_record_inita(&rec, 1);
    as_record_set_int64(&rec, "size", size);
    aerospike_key_put(p_as, &err, NULL, p_key, &rec);

    return true;
}
#else
static bool asc_raw_write(aerospike* p_as, as_key* p_key, uint8_t *buf, uint32_t size)
{
    as_status status;
    as_error err;

    // Prepare the record
    as_record rec;
    as_record_inita(&rec, 2);
    as_record_set_int64(&rec, "size", size);

    // Write the record to the database.
    as_record_set_raw(&rec, "data", (uint8_t *)buf, size);
    status = aerospike_key_put(p_as, &err, NULL, p_key, &rec);
    if (status != AEROSPIKE_OK) {
        ERROR("aerospike_key_put() returned %d - %s", err.code, err.message);
        return false;
    }

    return true;
}
#endif

#ifdef LDT_ENABLE
static bool asc_raw_read(aerospike* p_as, as_key* p_key, uint8_t *buf, uint32_t size)
{
    as_status status;
    as_error err;

    uint32_t lstack_size;
    uint32_t offset, chksize;

    // Create a large stack object to use
    as_ldt lstack;
	as_ldt_init(&lstack, "data", AS_LDT_LSTACK, NULL);

    // Get stack size
    status = aerospike_lstack_size(p_as, &err, NULL, p_key, &lstack, &lstack_size);
    if (status != AEROSPIKE_OK) {
        ERROR("aerospike_lstack_size() returned %d - %s", err.code, err.message);
        return false;
    }

	// Peek all the values back again.
	as_list* p_list = NULL;
	status = aerospike_lstack_peek(p_as, &err, NULL, p_key, &lstack, lstack_size, &p_list);
    if (status != AEROSPIKE_OK) {
        ERROR("aerospike_lstack_peek() returned %d - %s", err.code, err.message);
        return false;
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

    return true;
}
#else
static bool asc_raw_read(aerospike* p_as, as_key* p_key, uint8_t *buf, uint32_t size)
{
    as_status status;
    as_error err;

    // Read the (whole) test record from the database.
    as_record *rec = NULL;
    status = aerospike_key_get(p_as, &err, NULL, p_key, &rec);
    if (status != AEROSPIKE_OK) {
        ERROR("aerospike_key_get() returned %d - %s", err.code, err.message);
        return false;
    }

    // Read the record
    as_bytes *bytes = as_record_get_bytes(rec, "data");
    memcpy(buf, bytes->value, MIN(bytes->size, size));
    as_record_destroy(rec);

    return true;
}
#endif

bool asc_write(aerospike *p_as, char *ns, char *file, char *buf, uint32_t size)
{
    // Prepare the key
    as_key key;
    as_key_init_str(&key, ns, SET, file);

    return asc_raw_write(p_as, &key, (uint8_t *)buf, size);
}

bool asc_read(aerospike *p_as, char *ns, char *file, char *buf, uint32_t size)
{
    // Prepare the key
    as_key key;
    as_key_init_str(&key, ns, SET, file);

    return asc_raw_read(p_as, &key, (uint8_t *)buf, size);
}

bool asc_size(aerospike *p_as, char *ns, char *file, uint32_t *size)
{
    as_status status;
    as_error err;

    // Prepare the key
    as_key key;
    as_key_init_str(&key, ns, SET, file);

    // Read metadata
    as_record *rec = NULL;
    status = aerospike_key_get(p_as, &err, NULL, &key, &rec);
    if (status != AEROSPIKE_OK) {
        ERROR("aerospike_key_get() returned %d - %s", err.code, err.message);
        return false;
    }
    *size = as_record_get_int64(rec, "size", 0);
    as_record_destroy(rec);

    return true;
}


