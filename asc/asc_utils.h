#ifndef __ASC_UTILS_H
#define __ASC_UTILS_H

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

#define LDT_ENABLE
#define CHUNK_SIZE  (512*1024)

#define HOST    "127.0.0.1"
#define PORT    3000
#define USER    ""
#define PASS    ""
#define SET     "file"

#define ERROR(FORMAT, ...) printf("[ERROR] " FORMAT "\n", __VA_ARGS__)
#define WARN(FORMAT, ...) printf("[WARN] " FORMAT "\n", __VA_ARGS__)
#define INFO(FORMAT, ...) printf("[INFO] " FORMAT "\n", __VA_ARGS__)

#define MAX(A, B) (A > B ? A : B)
#define MIN(A, B) (A < B ? A : B)

bool asc_init(aerospike *p_as);
bool asc_exit(aerospike *p_as);
bool asc_write(aerospike *p_as, char *ns, char *file, char *buf, uint32_t size);
bool asc_read(aerospike *p_as, char *ns, char *file, char *buf, uint32_t size);
uint32_t asc_size(aerospike *p_as, char *ns, char *file);

#endif                          /* __ASC_UTILS_H */
