#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libgen.h>
#include "inotify_utils.h"

static int _off;

void fcreat(char *file)
{
    char *key = file + _off;
    printf("key: %s, file:%s\n", key, file);
}

int main(int argc, char **argv)
{
    char root[512];
    char *dir;

    if (argc != 2) {
        fprintf(stderr, "Usage: %s <dir>\n", argv[0]);
        exit(1);
    }
    dir = argv[1];
    sprintf(root, "%s/%s", dirname(dir), basename(dir));
    _off = strlen(root) + 1;

    watch_dir(root, fcreat);
    return 0;
}

