//==========================================================
// Includes
//

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>

//#include "example_utils.h"
#define LOG(FORMAT, ...) printf("[LOG] " FORMAT "\n", __VA_ARGS__)


//==========================================================
// Defines
//

#define HOST      "127.0.0.1"
#define PORT      3000
#define NAMESPACE "foo"
#define SET       ""//"set"
#define KEY       "key"
#define BIN       "bin"

int g_count;
int g_size;
uint8_t *g_buffer;

//==========================================================
// Forward Declarations
//

void read_config();

bool test_init(aerospike* p_as);
bool test_exit(aerospike* p_as);

bool clean_record(aerospike* p_as, as_key* p_key);
bool write_record(aerospike* p_as, as_key* p_key);
bool read_record(aerospike* p_as, as_key* p_key);

typedef bool(*action)(aerospike* p_as, as_key* p_key);
bool repeat_action(aerospike* p_as, action act);

//==========================================================
// Test
//

int
main(int argc, char* argv[])
{
	aerospike as;

  read_config();

  test_init(&as);
#ifdef TEST_CLEAN
  repeat_action(&as, clean_record);
#endif
#ifdef TEST_WRITE
  repeat_action(&as, write_record);
#endif
#ifdef TEST_READ
  repeat_action(&as, read_record);
#endif
  test_exit(&as);

  return 0;
}


//==========================================================
// Helpers
//

void
read_config()
{
  FILE *fp;
  char buffer[BUFSIZ], *line;

  fp = fopen("test.cfg", "r");
  if (fp == NULL) {
    printf("test.conf not exist\n");
    exit(-1);
  }
  
  line = fgets(buffer, BUFSIZ, fp);
  if (line == NULL) {
    printf("cannot get count\n");
    exit(-1);
  }
  g_count = atoi(line);
  printf("count\t%d\n", g_count);

  line = fgets(buffer, BUFSIZ, fp);
  if (line == NULL) {
    printf("cannot get size\n");
    exit(-1);
  }
  g_size = atoi(line);
  g_buffer = malloc(g_size);
  printf("size\t%d\n", g_size);

  fclose(fp);
}

bool
test_init(aerospike* p_as)
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
		LOG("aerospike_connect() returned %d - %s", err.code, err.message);
		aerospike_destroy(p_as);
	}

  return (status == AEROSPIKE_OK);
}

bool
test_exit(aerospike* p_as)
{
	as_error err;

	// Disconnect from the database cluster and clean up the aerospike object.
	aerospike_close(p_as, &err);
	aerospike_destroy(p_as);

  return true;
}

bool
clean_record(aerospike* p_as, as_key* p_key)
{
  as_error err;

	// Start clean.
	aerospike_key_remove(p_as, &err, NULL, p_key);

  return true;
}


bool
write_record(aerospike* p_as, as_key* p_key)
{
	as_error err;
  as_status status;

  // Prepare the record
	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_raw(&rec, BIN, g_buffer, g_size);

	// Write the record to the database.
  do {
    status = aerospike_key_put(p_as, &err, NULL, p_key, &rec);
#if 1
    if (status != AEROSPIKE_OK) {
      LOG("aerospike_key_put() returned %d - %s", err.code, err.message);
    }
#endif
  } while (status != AEROSPIKE_OK);

  return (status == AEROSPIKE_OK);
}

bool
read_record(aerospike* p_as, as_key* p_key)
{
	as_error err;
  as_status status;
	as_record* p_rec = NULL;

	// Read the (whole) test record from the database.
  status = aerospike_key_get(p_as, &err, NULL, p_key, &p_rec);
  if (status != AEROSPIKE_OK) {
		LOG("aerospike_key_get() returned %d - %s", err.code, err.message);
  }

  as_record_destroy(p_rec);
  return (status == AEROSPIKE_OK);
}

bool
repeat_action(aerospike* p_as, action act)
{
  as_key p_key[1];
  char key_str[20];
  int i;

  for (i = 0; i < g_count; i++) {
    sprintf(key_str, "key%08d", i);
    as_key_init_str(p_key, NAMESPACE, SET, key_str);
    act(p_as, p_key);
  }

  return true;
}
