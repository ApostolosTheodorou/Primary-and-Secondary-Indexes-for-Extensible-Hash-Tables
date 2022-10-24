#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "hash_file.h"
#include "sht_file.h"
#include "structs.h"


#define RECORDS_NUM 590 // you can change it if you want
#define GLOBAL_DEPT 2 // you can change it if you want
#define FILE_NAME "data.db"

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
  "Theofilos",
  "Giorgos",
  "Dimitris"
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
  "Mailis",
  "Michas",
  "Halatsis"
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
    HT_ErrorCode code = call; \
    if (code != HT_OK) {      \
      printf("Error\n");      \
      exit(code);             \
    }                         \
  }

int main() {
  int tupleId, i ;
	UpdateRecordArray updateArray[8];

  BF_Init(LRU);
  
  CALL_OR_DIE(HT_Init());

  int indexDesc;
  CALL_OR_DIE(HT_CreateIndex(FILE_NAME, GLOBAL_DEPT));
  CALL_OR_DIE(HT_OpenIndex(FILE_NAME, &indexDesc)); 

  Record record;
  srand(12569874);
  int r;
  printf("Insert Entries\n");
  for (int id = 0; id < RECORDS_NUM; ++id) {
    // create a record
    record.id = id;
    r = rand() % 12;
    memcpy(record.name, names[r], strlen(names[r]) + 1);
    r = rand() % 12;
    memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
    r = rand() % 10;
    memcpy(record.city, cities[r], strlen(cities[r]) + 1);

    CALL_OR_DIE(HT_InsertEntry(indexDesc, record, &tupleId, updateArray));
    printf("TupleId is: %d\n", tupleId);
    if (updateArray != NULL) {
      for (i= 0 ; i < 8 ; i++){
        printf("Record %d: SURNAME: %s, CITY: %s, OLDTUPLE: %d, NEWTUPLE: %d\n", i, updateArray[i].surname, updateArray[i].city, updateArray[i].oldTupleId, updateArray[i].newTupleId);
      }
    }
  }

  printf("hash_value of Ioannidis in glb 7 is: %d\n", hash_string("Ioannidis", 7));
  printf("hash_value of Ioannidis in glb 2 is: %d\n", hash_string("Ioannidis", 2));
  printf("hash_value of Mailis in glb 7 is: %d\n", hash_string("Mailis", 7));
  printf("hash_value of Ioannou in glb 7 is: %d\n", hash_string("Ioannou", 7));
  printf("hash_value of Nikolopoulos in glb 7 is: %d\n", hash_string("Nikolopoulos", 7));
 

  /*printf("RUN PrintAllEntries\n");
  int id = rand() % RECORDS_NUM;
  CALL_OR_DIE(HT_PrintAllEntries(indexDesc, &id));
  //CALL_OR_DIE(HT_PrintAllEntries(indexDesc, NULL));*/


  CALL_OR_DIE(HT_CloseFile(indexDesc));
  BF_Close();
}
