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

void chkdir(char *path);

int main(int argc, char *argv[])
{
    aerospike as;
    char *ns, *key, *dir, file[512];
    void *mem;
    int size;
    int fd;
    int rc;

    if (argc < 3) {
        printf("Usage: %s <ns>:<key> <dir>\n", basename(argv[0]));
        exit(-1);
    }
    ns = strtok(argv[1], ":");
    key = strtok(NULL, ":");
    dir = argv[2];
    sprintf(file, "%s/%s", dir, key);
    chkdir(file);

    // Perpare the key
    as_key as_key;
    as_key_init_str(&as_key, ns, SET, key);

    // Prepare the record
    as_record *p_rec = NULL;

    // read from database
    as_init(&as);
    as_read(&as, &as_key, &p_rec);
    as_exit(&as);

    size = p_rec->bins.entries[0].value.bytes.size;
    mem = p_rec->bins.entries[0].value.bytes.value;

    // write file
    fd = creat(file, 0644);
    if (fd < 0) {
        ERROR("cannot open file %s", file);
        return -1;
    }
    rc = write(fd, mem, size);
    if (rc < 0) {
        ERROR("cannot write file %s", file);
        return -1;
    }
    close(fd);

    as_record_destroy(p_rec);
    return 0;
}

void chkdir(char *path)
{
    char buf[512], *dir;

    strcpy(buf, path);
    dir = dirname(buf);

    if (strcmp(dir, "/") == 0)
        return;
    if (strchr(dir, '/'))
        chkdir(dir);

    mkdir(dir, 0755);
}
