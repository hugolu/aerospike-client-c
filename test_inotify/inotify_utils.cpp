#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/inotify.h>
#include <errno.h>
#include <dirent.h>
#include <libgen.h>

#include "inotify_utils.h"

#include <map>
#include <string>
using namespace std;

static map < int, string > dirset;
static fcreat_fun _fcreat;

#define MASK        (IN_MODIFY | IN_CREATE | IN_DELETE)
#define NEWDIR      (IN_CREATE | IN_ISDIR)
#define EVENT_SIZE  sizeof(struct inotify_event)
#define BUF_SIZE    ((EVENT_SIZE + 16) << 10)

void watch_add(int fd, char *dir, int mask);
void watch_mon(int fd);
void do_action(int fd, struct inotify_event *event);

void watch_dir(char *dir, fcreat_fun fcreat)
{
    int fd;

    _fcreat = fcreat;

    fd = inotify_init();
    if (fd < 0) {
        perror("inotify_init");
        return;
    }
    watch_add(fd, dir, MASK);
    watch_mon(fd);
}

void watch_add(int fd, char *dir, int mask)
{
    int wd;
    char subdir[512];
    DIR *odir;
    struct dirent *dent;

    wd = inotify_add_watch(fd, dir, mask);
    dirset.insert(make_pair(wd, string(dir)));

    if ((odir = opendir(dir)) == NULL) {
        perror("fail to open root dir");
        return;
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

void watch_mon(int fd)
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
}

void do_action(int fd, struct inotify_event *event)
{
    char path[512];

    if (!(event->mask & IN_CREATE))
        return;

    sprintf(path, "%s/%s", dirset.find(event->wd)->second.c_str(), event->name);
    if ((event->mask & IN_ISDIR)) {
        watch_add(fd, path, MASK);
    } else {
        _fcreat(path);
    }
}
