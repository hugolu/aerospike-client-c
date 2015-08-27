#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

//==========================================================
// Defines
//

#define ERROR(FORMAT, ...) printf("[ERROR] " FORMAT "\n", __VA_ARGS__)

int g_count;
int g_size;
char *g_buffer;

//==========================================================
// Forward Declarations
//

void read_config();

int clear_record(char *name);
int write_record(char *name);
int read_record(char *name);

typedef int(*action)(char *name);
int repeat_action(action act);

//==========================================================
// Test
//

int
main(int argc, char *argv[])
{
  read_config();

#ifdef TEST_CLEAR
  repeat_action(clear_record);
#endif
#ifdef TEST_WRITE
  repeat_action(write_record);
#endif
#ifdef TEST_READ
  repeat_action(read_record);
#endif

  return 0;
}

//==========================================================
// Helpers
//

void
read_config()
{
  FILE *fp;
  char buffer[BUFSIZ], *line;

  fp = fopen("test.cfg", "r");
  if (fp == NULL) {
    printf("test.conf not exist\n");
    exit(-1);
  }
  
  line = fgets(buffer, BUFSIZ, fp);
  if (line == NULL) {
    printf("cannot get count\n");
    exit(-1);
  }
  g_count = atoi(line);
  printf("count\t%d\n", g_count);

  line = fgets(buffer, BUFSIZ, fp);
  if (line == NULL) {
    printf("cannot get size\n");
    exit(-1);
  }
  g_size = atoi(line);
  g_buffer = malloc(g_size);
  printf("size\t%d\n", g_size);

  fclose(fp);
}

int
clear_record(char *name)
{
  int rc;

  rc = unlink(name);
  if (rc < 0) {
    ERROR("cannot remove file %s\n", name);
    return -1;
  }

  return 0;
}

int
write_record(char *name)
{
  int fd;
  int rc;

  fd = open(name, O_CREAT | O_WRONLY);
  if (fd < 0) {
    ERROR("cannot open file %s", name);
    return -1;
  }

  rc = write(fd, g_buffer, g_size);
  if (rc < 0) {
    ERROR("cannot write file %s", name);
    return -1;
  }

  close(fd);
  return 0;
}

int
read_record(char *name)
{
  int fd;
  int rc;

  fd = open(name, O_RDONLY);
  if (fd < 0) {
    ERROR("cannot open file %s", name);
    return -1;
  }

  rc = read(fd, g_buffer, g_size);
  if (rc < 0) {
    ERROR("cannot read file %s", name);
    return -1;
  }

  close(fd);
  return 0;
}

int
repeat_action(action act)
{
  char name[20];
  int i;

  for (i = 0; i < g_count; i++) {
    sprintf(name, "foo/file%08d", i);
    if (act(name) < 0)
      return -1;
  }

  return 0;
}

