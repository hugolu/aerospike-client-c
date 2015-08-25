#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <string.h>
#include <libgen.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
//==========================================================
// Defines
//

#define HOST      "127.0.0.1"
#define PORT      3000
#define SET       ""
#define BIN       ""

#define ERROR(FORMAT, ...) printf("[ERROR] " FORMAT "\n", __VA_ARGS__)
#define WARN(FORMAT, ...) printf("[WARN] " FORMAT "\n", __VA_ARGS__)

//==========================================================
// Forward Declarations
//

bool as_init(aerospike* p_as);
bool as_exit(aerospike* p_as);
bool as_write(aerospike* p_as, as_key* p_key, as_record* p_rec);
bool as_read(aerospike* p_as, as_key* p_key, as_record** p_rec);

//==========================================================
// Test
//

int
main(int argc, char *argv[])
{
  aerospike as;
  char *file, *ns, *key;
  void *mem;
  int size;
  int fd;
  int rc;

  if (argc < 3) {
    printf("Usage: %s <ns>:<key> <file>\n", basename(argv[0]));
    exit(-1);
  }
  ns = strtok(argv[1], ":");
  key = strtok(NULL, ":");
  file = argv[2];
  printf ("%s, %s, %s\n", file, ns, key);

  fd = creat(file, 0644);
  if (fd < 0) {
    ERROR("cannot open file %s", file);
    return -1;
  }

  // Perpare the key
  as_key as_key;
  as_key_init_str(&as_key, ns, SET, key);

  // Prepare the record
	as_record *p_rec = NULL;

  // write to database
  as_init(&as);
  as_read(&as, &as_key, &p_rec);
  as_exit(&as);

  size = p_rec->bins.entries[0].value.bytes.size;
  mem = p_rec->bins.entries[0].value.bytes.value;
  printf("%p %d\n", mem, size);

  rc = write(fd, mem, size);
  if (rc < 0) {
    ERROR("cannot write file %s", file);
    return -1;
  }

  as_record_destroy(p_rec);
  close(fd);
  return 0;
}

//==========================================================
// Helpers
//

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
