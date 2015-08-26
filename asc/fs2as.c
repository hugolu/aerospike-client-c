#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <string.h>
#include <libgen.h>

#include "as_utils.h"

int fs2as(aerospike *as, char *file, char *ns, char *key);

int main(int argc, char *argv[])
{
    aerospike as;
    char *file, *ns, *key;

    if (argc < 3) {
        printf("Usage: %s <file> <ns>:<key>\n", basename(argv[0]));
        exit(-1);
    }
    file = argv[1];
    ns = strtok(argv[2], ":");
    key = strtok(NULL, ":");

    // write to database
    as_init(&as);
    fs2as(&as, file, ns, key);
    as_exit(&as);

    return 0;
}

int
fs2as(aerospike *as, char *file, char *ns, char *key)
{
    struct stat st;
    int fd, size;
    void *mem;
    int rc;

    rc = stat(file, &st);
    if (rc < 0) {
        ERROR("cannot stat file %s", file);
        return -1;
    }
    size = st.st_size;

    fd = open(file, O_RDONLY);
    if (fd < 0) {
        ERROR("cannot open file %s", file);
        return -1;
    }

    mem = mmap(NULL, size, PROT_READ, MAP_SHARED, fd, 0);
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
    as_record_set_raw(&as_rec, BIN, mem, size);

    // Write to DB
    as_write(as, &as_key, &as_rec);

    munmap(mem, size);
    close(fd);

    return 0;
}
