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
#include "inotify_utils.h"

void fcreat(char *file);
int fs2as(aerospike *as, char *file, char *ns, char *key);

static int _offset;
static aerospike _as;
static char *_ns;

int main(int argc, char *argv[])
{
    char *dir, root[512];

    if (argc < 3) {
        printf("Usage: %s <dir> <ns>\n", basename(argv[0]));
        exit(-1);
    }
    dir = argv[1];
    _ns = argv[2];

    sprintf(root, "%s/%s", dirname(dir), basename(dir));
    _offset = strlen(root) + 1;

    as_init(&_as);
    watch_dir(root, fcreat);
    as_exit(&_as);

    return 0;
}

void
fcreat(char *file)
{
    char *key = file + _offset;

    INFO("%s is created.", file);
    fs2as(&_as, file, _ns, key);
}

int
fs2as(aerospike *as, char *file, char *ns, char *key)
{
    struct stat st;
    int fd, size;
    uint8_t *mem;
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

    mem = (uint8_t *)mmap(NULL, size, PROT_READ, MAP_SHARED, fd, 0);
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
