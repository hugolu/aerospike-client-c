#ifndef __INOTIFY_UTILS_H
#define __INOTIFY_UTILS_H

typedef void (*fcreat_fun)(char *file);
void watch_dir(char *root, fcreat_fun fcreat);

#endif /* __INOTIFY_UTILS_H */
