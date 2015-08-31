#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <string.h>
#include <libgen.h>

#include "asc_utils.h"

void chkdir(char *path);

int main(int argc, char *argv[])
{
    aerospike as;
    char *ns, *key, *dir, file[512];
    void *mem;
    uint32_t size;
    int fd;

    if (argc < 3) {
        printf("Usage: %s <ns>:<key> <dir>\n", basename(argv[0]));
        exit(-1);
    }
    ns = strtok(argv[1], ":");
    key = strtok(NULL, ":");
    dir = argv[2];
    sprintf(file, "%s/%s", dir, key);
    chkdir(file);

    // get file size
    asc_init(&as);
    asc_size(&as, ns, key, &size);

    // create the file
    fd = open(file, O_CREAT|O_TRUNC|O_RDWR, 0644);
    if (fd < 0) {
        ERROR("cannot open file %s", file);
        return -1;
    }
    lseek(fd, size-1, SEEK_CUR);
    write(fd, "\0", 1);
    mem = mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    if (mem == MAP_FAILED) {
        ERROR("cannot mmap file %s", file);
        return -1;
    }

    // read the record
    asc_read(&as, ns, key, mem, size);

    // close
    munmap(mem, size);
    close(fd);
    asc_exit(&as);

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
