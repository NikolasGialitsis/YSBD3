#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "bf.h"
#include "sort_file.h"

#define CALL_OR_DIE(call)     \
  {                           \
    SR_ErrorCode code = call; \
    if (code != SR_OK) {      \
      printf("Error\n");      \
      exit(code);             \
    }                         \
  }

int main() {
  int fd_id, fd_name, fd_surname;

  BF_Init(LRU);
  CALL_OR_DIE(SR_Init());
  CALL_OR_DIE(SR_OpenFile("sorted_id.db", &fd_id));
  CALL_OR_DIE(SR_OpenFile("sorted_name.db", &fd_name));
  CALL_OR_DIE(SR_OpenFile("sorted_surname.db", &fd_surname));

  printf("Print all Entries sorted in field id\n");
  CALL_OR_DIE(SR_PrintAllEntries(fd_id))
  printf("Print all Entries sorted in field name\n");
  CALL_OR_DIE(SR_PrintAllEntries(fd_name))
  printf("Print all Entries sorted in field surname\n");
  CALL_OR_DIE(SR_PrintAllEntries(fd_surname))

  CALL_OR_DIE(SR_CloseFile(fd_id))
  CALL_OR_DIE(SR_CloseFile(fd_name))
  CALL_OR_DIE(SR_CloseFile(fd_surname))

  BF_Close();
}
