#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "hash_file.h"
#include "sht_file.h"

#define RECORDS_NUM 40 // you can change it if you want
#define GLOBAL_DEPT 2 // you can change it if you want
#define FILE_NAME "data.db"
#define SECONDARY_FILE_NAME1 "data2.db"
#define SECONDARY_FILE_NAME2 "data3.db"

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
  "San Diego",
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
  char key[20], key2[20];
	UpdateRecordArray updateArray[8];
  int citiesFrequency[10];

  for (i=0 ; i < 10 ; i++) {
    citiesFrequency[i]= 0;
  }


  strcpy(key, "city");

  BF_Init(LRU);
  
  CALL_OR_DIE(HT_Init());

  int indexDesc, sindexDesc1, sindexDesc2;
  
  //Create and open the primary file
  CALL_OR_DIE(HT_CreateIndex(FILE_NAME, GLOBAL_DEPT));
  CALL_OR_DIE(HT_OpenIndex(FILE_NAME, &indexDesc)); 

  //Create 2 secondary files for the primary file (with key the surname)

  //Create and open a secondary file with key value --> surname
  CALL_OR_DIE(SHT_CreateSecondaryIndex(SECONDARY_FILE_NAME1, key, 20, GLOBAL_DEPT, FILE_NAME));
  CALL_OR_DIE(SHT_OpenSecondaryIndex(SECONDARY_FILE_NAME1, &sindexDesc1)); 

  //Create and open another secondary file with key value --> surname
  CALL_OR_DIE(SHT_CreateSecondaryIndex(SECONDARY_FILE_NAME2, key, 20, GLOBAL_DEPT, FILE_NAME));
  CALL_OR_DIE(SHT_OpenSecondaryIndex(SECONDARY_FILE_NAME2, &sindexDesc2)); 


  Record record;
  SecondaryRecord secondary_record;

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
    if (strcmp(key, "surname") == 0) {
       strcpy(secondary_record.index_key, record.surname);
    }
   
    r = rand() % 10;
    citiesFrequency[r]++;
    memcpy(record.city, cities[r], strlen(cities[r]) + 1);
    if (strcmp(key, "city") == 0) {
       strcpy(secondary_record.index_key, record.city);
    }

    strcpy(updateArray[0].city, "NULL");
    CALL_OR_DIE(HT_InsertEntry(indexDesc, record, &tupleId, updateArray));
    secondary_record.tupleId= tupleId;
    if (strcmp(updateArray[0].city, "NULL") != 0) {
        CALL_OR_DIE(SHT_SecondaryUpdateEntry(sindexDesc1, updateArray));
        CALL_OR_DIE(SHT_SecondaryUpdateEntry(sindexDesc2, updateArray));
    }
    CALL_OR_DIE(SHT_SecondaryInsertEntry(sindexDesc1, secondary_record));
    CALL_OR_DIE(SHT_SecondaryInsertEntry(sindexDesc2, secondary_record));
     
  }

  printf("RUN PrintAllEntries FOR PRIMARY FILE\n");
  CALL_OR_DIE(HT_PrintAllEntries(indexDesc, NULL));
  printf("********* PrintAllEntries FOR PRIMARY COMPLETED*******\n\n");
  
  printf("RUN PrintAllEntries FOR 1st SECONDARY FILE, WITH KEY \"Athens\"\n");
  CALL_OR_DIE(SHT_PrintAllEntries(sindexDesc1, "Athens"));
  printf("********* PrintAllEntries FOR 1st SECONDARY (\"Athens\") COMPLETED*******\n\n");


  //Eisagogi neas eggrafis
  record.id= RECORDS_NUM;
  strcpy(record.name, "Leonidas");
  strcpy(record.surname, "Sarlas");
  strcpy(record.city, "Glyfada");
  strcpy(updateArray[0].city, "NULL");

  CALL_OR_DIE(HT_InsertEntry(indexDesc, record, &tupleId, updateArray));

  secondary_record.tupleId= tupleId;
  strcpy(secondary_record.index_key, record.city);
  if (strcmp(updateArray[0].city, "NULL") != 0) {
      CALL_OR_DIE(SHT_SecondaryUpdateEntry(sindexDesc1, updateArray));
      CALL_OR_DIE(SHT_SecondaryUpdateEntry(sindexDesc2, updateArray));
  }
  CALL_OR_DIE(SHT_SecondaryInsertEntry(sindexDesc1, secondary_record));
  CALL_OR_DIE(SHT_SecondaryInsertEntry(sindexDesc2, secondary_record));

  //Find "Sarlas" in the primary file
  int id= RECORDS_NUM;
  printf("Find \"Sarlas\" in the primary file\n");
  CALL_OR_DIE(HT_PrintAllEntries(indexDesc, &id));
  printf("\"Sarlas\" was found in the primary file\n\n");
  //}
  //Find "Sarlas" in the secondary file
  printf("Find \"Sarlas\" in the secondary file\n");
  CALL_OR_DIE(SHT_PrintAllEntries(sindexDesc1, "Glyfada"));
  printf("\"Sarlas\" was found in the secondary file\n\n");

  

  printf("RUN INNER JOIN FOR THE TWO SECONDARY FILES FOR \"Miami\"\n");
  CALL_OR_DIE(SHT_InnerJoin( sindexDesc1, sindexDesc2, "Miami"));
  printf("********* INNER JOIN FOR \"Miami\" COMPLETED*******\n\n");

  printf("RUN INNER JOIN FOR THE TWO SECONDARY FILES FOR WITH NULL\n");
  CALL_OR_DIE(SHT_InnerJoin( sindexDesc1, sindexDesc2, NULL));
  printf("********* INNER JOIN WITH NULL COMPLETED*******\n\n");

  CALL_OR_DIE(SHT_HashStatistics(FILE_NAME));
  CALL_OR_DIE(SHT_HashStatistics(SECONDARY_FILE_NAME1));
  
  printf("\n------------ Cities' Frequency------------\n");
  for (i= 0; i < 10 ; i++) {
    printf("%s: %d\n", cities[i], citiesFrequency[i]);
  }

  CALL_OR_DIE(HT_CloseFile(indexDesc));
  CALL_OR_DIE(SHT_CloseSecondaryIndex(sindexDesc1));
  CALL_OR_DIE(SHT_CloseSecondaryIndex(sindexDesc2));
  BF_Close();

}
