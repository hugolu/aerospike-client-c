#include "as_utils.h"

bool
as_init(aerospike* p_as)
{
	as_error err;
  as_status status;

	// Start with default configuration.
	as_config cfg;
	as_config_init(&cfg);
	as_config_add_host(&cfg, HOST, PORT);
	as_config_set_user(&cfg, "", "");
	aerospike_init(p_as, &cfg);

	// Connect to the Aerospike database cluster.
  status = aerospike_connect(p_as, &err);
	if (status != AEROSPIKE_OK) {
	  ERROR("aerospike_connect() returned %d - %s", err.code, err.message);
		aerospike_destroy(p_as);
	}

  return (status == AEROSPIKE_OK);
}

bool
as_exit(aerospike* p_as)
{
	as_error err;

	// Disconnect from the database cluster and clean up the aerospike object.
	aerospike_close(p_as, &err);
	aerospike_destroy(p_as);

  return true;
}

bool
as_write(aerospike* p_as, as_key* p_key, as_record* p_rec)
{
	as_error err;
  as_status status;

	// Write the record to the database.
  do {
    status = aerospike_key_put(p_as, &err, NULL, p_key, p_rec);
    if (status != AEROSPIKE_OK) {
      WARN("aerospike_key_put() returned %d - %s", err.code, err.message);
    }
  } while (status != AEROSPIKE_OK);

  return (status == AEROSPIKE_OK);
}

bool
as_read(aerospike* p_as, as_key* p_key, as_record** p_rec)
{
	as_error err;
  as_status status;

	// Read the (whole) test record from the database.
  status = aerospike_key_get(p_as, &err, NULL, p_key, p_rec);
  if (status != AEROSPIKE_OK) {
		WARN("aerospike_key_get() returned %d - %s", err.code, err.message);
  }

  return (status == AEROSPIKE_OK);
}

