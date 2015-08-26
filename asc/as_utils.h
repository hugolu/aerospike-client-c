#ifndef __AS_UTILS_H
#define __AS_UTILS_H

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>

#define HOST      "127.0.0.1"
#define PORT      3000
#define SET       ""
#define BIN       ""

#define ERROR(FORMAT, ...) printf("[ERROR] " FORMAT "\n", __VA_ARGS__)
#define WARN(FORMAT, ...) printf("[WARN] " FORMAT "\n", __VA_ARGS__)
#define INFO(FORMAT, ...) printf("[INFO] " FORMAT "\n", __VA_ARGS__)

bool as_init(aerospike* p_as);
bool as_exit(aerospike* p_as);
bool as_write(aerospike* p_as, as_key* p_key, as_record* p_rec);
bool as_read(aerospike* p_as, as_key* p_key, as_record** p_rec);

#endif /* __AS_UTILS_H */
