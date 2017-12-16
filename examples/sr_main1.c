#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "bf.h"
#include "sort_file.h"

const char* names[] = {
  "Yannis",
  "Christofos",
  "Sofia",
  "Marianna",
  "Vagelis",
  "Maria",
  "Iosif",
  "Dionisis",
  "Konstantina",
  "Theofilos"
};

const char* surnames[] = {
  "Ioannidis",
  "Svingos",
  "Karvounari",
  "Rezkalla",
  "Nikolopoulos",
  "Berreta",
  "Koronis",
  "Gaitanis",
  "Oikonomou",
  "Mailis"
};

const char* cities[] = {
  "Athens",
  "San Francisco",
  "Los Angeles",
  "Amsterdam",
  "London",
  "New York",
  "Tokyo",
  "Hong Kong",
  "Munich",
  "Miami"
};

#define CALL_OR_DIE(call)     \
  {                           \
    SR_ErrorCode code = call; \
    if (code != SR_OK) {      \
      printf("Error\n");      \
      exit(code);             \
    }                         \
  }

int main() {
  BF_Init(LRU);

  CALL_OR_DIE(SR_Init());

  int fd;
  CALL_OR_DIE(SR_CreateFile("unsorted_data.db"));
  CALL_OR_DIE(SR_OpenFile("unsorted_data.db", &fd));

  Record record;
  srand(12569874);
  int r;
  printf("Insert Entries\n");
  for (int id = 0; id < 2700; ++id) {
    record.id = id;
    r = rand() % 10;
    memcpy(record.name, names[r], strlen(names[r]) + 1);
    r = rand() % 10;
    memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
    r = rand() % 10;
    memcpy(record.city, cities[r], strlen(cities[r]) + 1);

    CALL_OR_DIE(SR_InsertEntry(fd, record));
  }

  printf("RUN PrintAllEntries\n");
  CALL_OR_DIE(SR_PrintAllEntries(fd));

  CALL_OR_DIE(SR_CloseFile(fd));
  BF_Close();
}
