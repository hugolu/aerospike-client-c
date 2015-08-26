#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <unistd.h>
#include <sys/types.h>
#include <sys/inotify.h>
#include <errno.h>
#include <dirent.h>
#include <libgen.h>

#include <map>
#include <string>
using namespace std;

static void watch_add(int fd, char *dir, int mask);
static void watch_mon(int fd);
static void do_action(int fd, struct inotify_event *event);

map < int, string > dirset;
int g_off;

#define MASK        (IN_MODIFY | IN_CREATE | IN_DELETE)
#define NEWDIR      (IN_CREATE | IN_ISDIR)

#define EVENT_SIZE  sizeof(struct inotify_event)
#define BUF_SIZE    ((EVENT_SIZE + 16) << 10)

int main(int argc, char **argv)
{
    char root[512];
    int fd;

    if (argc != 2) {
        fprintf(stderr, "Usage: %s <dir>\n", argv[0]);
        exit(1);
    }
    sprintf(root, "%s/%s", dirname(argv[1]), basename(argv[1]));
    g_off = strlen(root) + 1;

    fd = inotify_init();
    if (fd < 0) {
        perror("inotify_init");
        exit(-1);
    }
    watch_add(fd, root, MASK);
    watch_mon(fd);

    return 0;
}

static void watch_add(int fd, char *dir, int mask)
{
    int wd;
    char subdir[512];
    DIR *odir;
    struct dirent *dent;

    wd = inotify_add_watch(fd, dir, mask);
    dirset.insert(make_pair(wd, string(dir)));

    if ((odir = opendir(dir)) == NULL) {
        perror("fail to open root dir");
        exit(1);
    }
    while ((dent = readdir(odir)) != NULL) {
        if (strcmp(dent->d_name, ".") == 0 || strcmp(dent->d_name, "..") == 0)
            continue;
        if (dent->d_type == DT_DIR) {
            sprintf(subdir, "%s/%s", dir, dent->d_name);
            watch_add(fd, subdir, mask);
        }
    }
    closedir(odir);
}

static void watch_mon(int fd)
{
    int i, length;
    char buf[BUF_SIZE];
    struct inotify_event *event;

    while ((length = read(fd, buf, BUF_SIZE)) >= 0) {
        i = 0;
        while (i < length) {
            event = (struct inotify_event *) (buf + i);
            if (event->len) {
                do_action(fd, event);
            }
            i += EVENT_SIZE + event->len;
        }
    }
    close(fd);
    exit(1);
}

static void do_action(int fd, struct inotify_event *event)
{
    char path[512], *key;

    if (!(event->mask & IN_CREATE))
        return;

    sprintf(path, "%s/%s", dirset.find(event->wd)->second.c_str(), event->name);
    key = path + g_off;

    if ((event->mask & IN_ISDIR)) {
        watch_add(fd, path, MASK);
    } else {
        printf("%s was created.\n", key);
    }
}
