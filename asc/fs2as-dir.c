#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <string.h>
#include <libgen.h>
#include <dirent.h>

#include "asc_utils.h"

int fs2as(aerospike *as, char *file, char *ns, char *key);
int scan_dir(char *dir);

static int _offset;
static aerospike _as;
static char *_ns;
static int _total;

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
    _total = 0;

    // write to database
    asc_init(&_as);
    scan_dir(root);
    asc_exit(&_as);

    printf("\ntotal: %d\n", _total);
    return 0;
}

int
scan_dir(char *dir)
{
    DIR *odir;
    struct dirent *dent;
    char path[512];

    if ((odir = opendir(dir)) == NULL) {
        return 0;
    }
    while((dent = readdir(odir)) != NULL) {
        if (strcmp(dent->d_name, ".") == 0 || strcmp(dent->d_name, "..") == 0)
            continue;

        sprintf(path, "%s/%s", dir, dent->d_name);
        if (dent->d_type == DT_DIR) {
            scan_dir(path);
        } else
        if (dent->d_type == DT_REG) {
            char *key = path + _offset;
            printf("insert %s:%s\n", _ns, key);
            fs2as(&_as, path, _ns, key);
            _total += 1;
        }
    }
    closedir(odir);
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

    // Write to DB
    asc_write(as, ns, key, mem, size);

    munmap(mem, size);
    close(fd);

    return 0;
}
