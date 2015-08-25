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

//==========================================================
// Test
//

int
main(int argc, char *argv[])
{
  aerospike as;
  char *file, *ns, *key;
  struct stat st;
  int fd;
  void *mem;
  int rc;

  if (argc < 3) {
    printf("Usage: %s <file> <ns>:<key>\n", basename(argv[0]));
    exit(-1);
  }
  file = argv[1];
  ns = strtok(argv[2], ":");
  key = strtok(NULL, ":");
  printf ("%s, %s, %s\n", file, ns, key);

  rc = stat(file, &st);
  if (rc < 0) {
    ERROR("cannot stat file %s", file);
    return -1;
  }

  fd = open(file, O_RDONLY);
  if (fd < 0) {
    ERROR("cannot open file %s", file);
    return -1;
  }

  mem = mmap(NULL, st.st_size, PROT_READ, MAP_SHARED, fd, 0);
  if (mem == MAP_FAILED) {
    ERROR("cannot mmap file %s", file);
    return -1;
  }

  // Perpare the key
  as_key as_key;
  as_key_init_str(&as_key, ns, SET, key);

  // Prepare the record
	as_record as_rec;
	as_record_inita(&as_rec, 1);
	as_record_set_raw(&as_rec, BIN, mem, st.st_size);
  printf("size: %d\n", as_rec.bins.entries[0].value.bytes.size);

  // write to database
  as_init(&as);
  as_write(&as, &as_key, &as_rec);
  as_exit(&as);

  munmap(mem, st.st_size);
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
