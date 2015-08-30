#include "asc_utils.h"

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


bool asc_write(aerospike *p_as, char *ns, char *file, char *buf, uint32_t size)
{
    as_key key;
    as_record rec;
    as_status status;
    as_error err;
    int retry = 5;

    // Prepare the key
    as_key_init_str(&key, ns, SET, file);

    // Prepare the record
    as_record_inita(&rec, 2);
    as_record_set_int64(&rec, "size", size);

#ifdef LDT_ENABLE
    as_ldt lstack;
    as_bytes bval;
    as_ldt_init(&lstack, "data", AS_LDT_LSTACK, NULL);

    uint32_t offset, length;
    for (offset = 0; offset < size; offset += length) {
        length = MIN(CHUNK_SIZE, size - offset);
        as_bytes_init_wrap(&bval, (uint8_t *)buf + offset, length, false);
        status = aerospike_lstack_push(p_as, &err, NULL, &key, &lstack, (const as_val*)&bval);
        if (status != AEROSPIKE_OK) {
            ERROR("aerospike_lstack_push() returned %d - %s", err.code, err.message);
            break;
        }
    }
#else
    as_record_set_raw(&rec, "data", (uint8_t *)buf, size);
#endif

RETRY:
    // Write the record to the database.
    status = aerospike_key_put(p_as, &err, NULL, &key, &rec);
    if (status != AEROSPIKE_OK) {
        ERROR("aerospike_key_put() returned %d - %s", err.code, err.message);
        if (retry-- > 0) {
            sleep(1);
            goto RETRY;
        }
    }

    return (status == AEROSPIKE_OK);
}

bool asc_read(aerospike *p_as, char *ns, char *file, char *buf, uint32_t size)
{
    as_key key;
    as_record *rec = NULL;
    as_status status;
    as_error err;

    // Prepare the key
    as_key_init_str(&key, ns, SET, file);

#ifdef LDT_ENABLE
    as_ldt lstack;
    as_ldt_init(&lstack, "data", AS_LDT_LSTACK, NULL);

    uint32_t cnt;
    status = aerospike_lstack_size(p_as, &err, NULL, &key, &lstack, &cnt);
    if (status != AEROSPIKE_OK) {
        ERROR("aerospike_lstack_size() returned %d - %s", err.code, err.message);
        return false;
    }

    // Peek all the values back again.
    as_list *p_list = NULL;
    status = aerospike_lstack_peek(p_as, &err, NULL, &key, &lstack, cnt, &p_list);
    if (status != AEROSPIKE_OK) {
        ERROR("aerospike_lstack_peek() returned %d - %s", err.code, err.message);
        return false;
    }

    // read lstack
    const as_arraylist *p_array = (const as_arraylist *)p_list;
    uint32_t i;
    for (i = 0; i < cnt; i++) {
        const as_val* p_val = p_array->elements[i];
        const as_bytes* p_bytes = (const as_bytes*)p_val;
        printf("size:%d\n", p_bytes->size);
        memcpy(buf + (cnt-1-i)*CHUNK_SIZE, p_bytes->value, p_bytes->size);
        printf("size:%d\n", p_bytes->size);
    }
    as_list_destroy(p_list);
    p_list = NULL;
#else
    // Read the (whole) test record from the database.
    status = aerospike_key_get(p_as, &err, NULL, &key, &rec);
    if (status != AEROSPIKE_OK) {
        ERROR("aerospike_key_get() returned %d - %s", err.code, err.message);
    } else {
        // Read the record
        as_bytes *bytes = as_record_get_bytes(rec, "data");
        memcpy(buf, bytes->value, MIN(bytes->size, size));
    }
    as_record_destroy(rec);
#endif

    return (status == AEROSPIKE_OK);
}

uint32_t asc_size(aerospike *p_as, char *ns, char *file)
{
    as_key key;
    as_record *rec = NULL;
    as_status status;
    as_error err;
    uint32_t size;

    // Prepare the key
    as_key_init_str(&key, ns, SET, file);

    // Read metadata
    status = aerospike_key_get(p_as, &err, NULL, &key, &rec);
    if (status != AEROSPIKE_OK) {
        ERROR("aerospike_key_get() returned %d - %s", err.code, err.message);
        return -1;
    }

    size = as_record_get_int64(rec, "size", 0);
    as_record_destroy(rec);

    return size;
}

