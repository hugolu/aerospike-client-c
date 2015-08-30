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
    as_record_set_raw(&rec, "data", (uint8_t *)buf, size);

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

bool asc_read(aerospike *p_as, char *ns, char *file, char *buf, uint32_t size)
{
    as_key key;
    as_record *rec = NULL;
    as_status status;
    as_error err;

    // Prepare the key
    as_key_init_str(&key, ns, SET, file);

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

    return (status == AEROSPIKE_OK);
}
