#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#include "bf.h"
#include "hash_file.h"
#include "filetable.h"
#include "structs.h"

#define MAX_OPEN_FILES 20

#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {         \
    BF_PrintError(code);    \
    return HP_ERROR;        \
  }                         \
}


/*Found at: 
https://stackoverflow.com/questions/664014/what-integer-hash-function-are-good-that-accepts-an-integer-hash-key*/

char hash (int x, char global_depth) {
    int random_prime_number= 2654358541 ;
    int random_value= 1238150027;
    char hash_value;

    hash_value= ((x *2654435761) ) % (int) pow(2.0, 7); 
    hash_value= hash_value >> (7- global_depth);
    return hash_value; 
}
HT_ErrorCode HT_Init() {
  //insert code here
  return HT_OK;
}



HT_ErrorCode HT_CreateIndex(const char *filename, int depth) {
  //insert code here
  int filedesc, i;
  int blocksnumber,error;
  char *blockdata;
  BF_Block *block;
  char inserted_char;
  
  BF_Block_Init(&block);
  error = BF_CreateFile(filename);
  if (error) {
      BF_PrintError(error);
      return error;
  }
  error = BF_OpenFile(filename, &filedesc);
  if (error) {
      BF_PrintError(error);
      return error;
  }
    
  error = BF_AllocateBlock(filedesc, block);
  if (error) {
      BF_PrintError(error);
      return error;
  }
  blockdata = BF_Block_GetData (block);

  inserted_char= 'H';
  memcpy (blockdata, &inserted_char, sizeof(char));
  BF_Block_SetDirty (block);
  inserted_char= (char)depth;
  memcpy (blockdata + sizeof(char), &inserted_char, sizeof(char));
  BF_Block_SetDirty (block);

  for (i = 0; i < pow(2, depth); i++)
    {
      inserted_char= (char) i;
      memcpy(blockdata + 2*(i + 1) * sizeof(char), &inserted_char, sizeof(char));
      BF_Block_SetDirty (block);
      inserted_char= (char) 0;
      memcpy(blockdata + 2*(i + 1) * sizeof(char) + 1, &inserted_char, sizeof(char));
      BF_Block_SetDirty (block);
    }



  BF_UnpinBlock (block);
    
  error = BF_CloseFile (filedesc);
  if (error) {
      BF_PrintError(error);
      return error;
  }
  //BF_Close ();

  return HT_OK;
}

HT_ErrorCode HT_OpenIndex(const char *fileName, int *indexDesc){
  //insert code here
  int temp;
  int error = BF_OpenFile(fileName, &temp) ;

  if (error) {
      BF_PrintError(error);
      return error;
  }
  
  *indexDesc = temp;
  insert_filetable(*indexDesc, fileName, "primary");
  
  return HT_OK;
}


HT_ErrorCode HT_CloseFile(int indexDesc) {
  //insert code here
  int error;

  if (indexDesc >= MAX_OPEN_FILES) {
      return HT_ERROR;
  }  
  
  error= BF_CloseFile(indexDesc);
  if (error) {
      BF_PrintError(error);
      return HT_ERROR;
  }
  
  remove_file(indexDesc);

  return HT_OK;
}

HT_ErrorCode HT_InsertEntry(int indexDesc, Record record, int* tupleId, UpdateRecordArray *updateArray) {
  printf("\n\nRecord with ID = %d arrived!\n", record.id);
  //insert code here
  int  error, blocksnumber, i, new_tupleId;;
  BF_Block* block, *new_block, *new_block2, *new_block3;
  char* blockdata, *new_blockdata, *new_blockdata2, *new_blockdata3;
  char global_depth, local_depth, bucket, inserted_char, num_of_records, hash_value,
       position, num_of_buddies, old_hashtable[BF_BLOCK_SIZE], bitstring_id, temp,
       first_buddys_bitstring;
  Record* recordArray;
  UpdateRecordArray *updateArrayNull;

  recordArray= malloc(BF_BLOCK_SIZE/sizeof(Record) * sizeof(Record));
  
  BF_Block_Init(&block);
  BF_Block_Init(&new_block);
  BF_Block_Init(&new_block2);
  BF_Block_Init(&new_block3);

  error= BF_GetBlock(indexDesc, 0, block);
  if (error) {
      BF_PrintError(error);
      return HT_ERROR;
  }

  blockdata= BF_Block_GetData(block);
  global_depth= blockdata[1];

  hash_value= hash(record.id, global_depth);

  printf("\nRecord's hash value is %d\n", hash_value);
  
  //Vres se poio bucket peftei i eggrafi
  bucket= blockdata[hash_value*2 +3]; //+3 epeidi ta prota 2 kelia einai H, global_depth 
                                     //*2 epeidi exoume zeygaria {bitstring id, pointer}
  printf("Record should be inserted in block %d\n(if 0 a new block will be constructed for this record)\n", bucket);

  //An einai i proti eggrafi poy peftei se ayto to bucket
  if (bucket == 0) {
    printf("This is the first record that is inserted in this block!\n");
    error= BF_AllocateBlock(indexDesc, new_block);
    if (error) {
        BF_PrintError(error);
        return HT_ERROR;
    }
    
  

    new_blockdata = BF_Block_GetData (new_block);
    inserted_char= 'D';
    memcpy (new_blockdata, &inserted_char, sizeof(char));
    BF_Block_SetDirty (new_block);
    local_depth= global_depth;
    memcpy (new_blockdata + sizeof(char), &local_depth, sizeof(char));
    num_of_records= 1;
    memcpy (new_blockdata + 2*sizeof(char), &num_of_records, sizeof(char));
    BF_Block_SetDirty (new_block);
    memcpy (new_blockdata + 3*sizeof(char), &record, sizeof(record));
    BF_Block_SetDirty (new_block);

    error = BF_GetBlockCounter(indexDesc, &blocksnumber);
    if (error) {
      BF_PrintError(error);
      return error;
  }
    temp= blocksnumber -1;
    memcpy(blockdata+(hash_value*2 +3), &temp, sizeof(char) );
    //Vres to tupleId tis eggrafis
    *tupleId= (temp-1)*8 + 1;
    
    BF_Block_SetDirty (block);

    printf("Ultimately, record with ID = %d was inserted in block %d\n", record.id, temp);

    insert_record_in_file (indexDesc);
    insert_block_in_file (indexDesc); 

    BF_Block_SetDirty(new_block);
    error= BF_UnpinBlock(block);
    if (error) {
        BF_PrintError(error);
        return HT_ERROR;
    }
    error= BF_UnpinBlock(new_block);
    if (error) {
        BF_PrintError(error);
        return HT_ERROR;
    }
 
  }
  else { //An to bucket sto opoio epese i trexousa eggrafi exei ki alles eggrafes mesa toy
      error = BF_GetBlock(indexDesc, bucket, new_block);
      if (error)
      {
        BF_PrintError (error);
        return HT_ERROR;
      }

     
      new_blockdata = BF_Block_GetData (new_block);
      //An to block xoraei ki alles eggrafes (BF_BLOCK_SIZE/sizeof(Record)= 512/60=8)
      
      if (new_blockdata[2] < BF_BLOCK_SIZE/sizeof(Record)) { 
        
        memcpy(new_blockdata+(new_blockdata[2]*sizeof(Record)+3)*(sizeof(char)), &record, sizeof(Record));
        BF_Block_SetDirty (new_block);
        num_of_records= new_blockdata[2]+1;
      
        memcpy(new_blockdata+2*sizeof(char), &num_of_records, sizeof(char));
        BF_Block_SetDirty (new_block);
        printf("The record with ID = %d was inserted in block (%d)\nNow block %d has %d records\n",
                 record.id, bucket, bucket, new_blockdata[2]);
        insert_record_in_file (indexDesc);
        BF_Block_SetDirty(new_block);

        //Vres to tupleId
        *tupleId= (bucket-1)*8 + num_of_records;

        error= BF_UnpinBlock(new_block);
        if (error) {
            BF_PrintError(error);
            return HT_ERROR;
        }

        error= BF_UnpinBlock(block);
        if (error) {
            BF_PrintError(error);
            return HT_ERROR;
        }
      }
      //An to block einai gemato
      else if (new_blockdata[2] == BF_BLOCK_SIZE/sizeof(Record)) {

          printf("Oops! Block %d is full\n", bucket);
          
          //An to local depth tou bucket einai mikrotero toy global_depth
          if (new_blockdata[1] < global_depth) {
              printf("Block's local_depth(%d)  < global_depth (%d)\n",new_blockdata[1], global_depth);
              //Desmeyetai ena neo block
            
              error= BF_AllocateBlock(indexDesc, new_block2);
              if (error) {
                BF_PrintError(error);
              return HT_ERROR;
              }
          
              //Na doume mipos xreiazetai na paroume to new_blockdata2 me ton olokliromeno tropo
              new_blockdata2= BF_Block_GetData(new_block2);
             
              //Arxikopoiisi toy neou block
              inserted_char= 'D';
              memcpy (new_blockdata2, &inserted_char, sizeof(char));
              BF_Block_SetDirty (new_block2);

              //To local depth 8a alla3ei kai sto block pou spaei kai sto neo block kai 8a ginei +1
              local_depth= new_blockdata[1]+1;

              int difference= (global_depth-(local_depth-1));
              printf("The difference of globl - local is %d= %d-%d\n", difference, global_depth, local_depth);
              //Vres posous buddies exei to bucket pou spaei sta 2 
              num_of_buddies= pow(2.0, difference);

              memcpy (new_blockdata + 1*sizeof(char), &local_depth, sizeof(char));
              BF_Block_SetDirty (new_block);
              
              memcpy (new_blockdata2 + 1*sizeof(char), &local_depth, sizeof(char));
              BF_Block_SetDirty (new_block2);
              
              //Vres to id tou kainouriou block
              error= BF_GetBlockCounter(indexDesc, &blocksnumber);
              if (error) {
                  BF_PrintError(error);
                  return HT_ERROR;
              }
              blocksnumber--;
              printf("Block %d will be separated into blocks %d and %d\n", bucket, bucket, blocksnumber);

              //Vres poios einai o protos apo tous buddies pou exoun bucket 
              //idio me to block pou prepei na mpei i nea eggrafi
              for (i= 2 ; i < 512 ; i+=2) {
                //An to vrikame
                if (blockdata[i+1] == bucket) {
                    first_buddys_bitstring= blockdata[i];
                    break;
                }
              }
              //Enimerosi tou hash table
              //Oi protoi misoi buddies 8a deixnoun sto palio block
              //Oi deyteroi misoi buddies 8a deixnoun sto kainourio block 
              printf("First buddies bitstring= %d (is it 4?),  num_of_buddies= %d(is it 2?)\n", first_buddys_bitstring, num_of_buddies);
              for (i= 0; i < num_of_buddies/2 ; i++) {
                  memcpy(blockdata+(num_of_buddies + 3 + (first_buddys_bitstring+i)*2)*sizeof(char), &blocksnumber, sizeof(char));
                  BF_Block_SetDirty (block);
              }



              //Apo8ikeyoume tis eggrafes tou block pou 8a diaire8ei se enan prosorino pinaka eggrafon
              for (i= 0; i < BF_BLOCK_SIZE/sizeof(Record) ; i++) {
                  memcpy(recordArray+i, new_blockdata+(3+ i * sizeof(Record)), sizeof(Record));
                  //Arxikopoioume ton pinaka update array
                  strcpy( (updateArray)[i].surname, recordArray[i].surname );
                  strcpy( (updateArray)[i].city, recordArray[i].city );
                  (updateArray)[i].oldTupleId= (bucket-1) *8 + (i+1);
              }
              //Adeiazoume to palio block 
              for (i=2 ; i < 512 ; i++) {
                new_blockdata[i]= 0;
                BF_Block_SetDirty (new_block);
              }
              //Vale tin nea eggrafi sto sosto bucket
              hash_value= hash(record.id, global_depth);
              //Vres se poio bucket peftei i nea eggrafi
              bucket= blockdata[hash_value*2 +3]; //+3 epeidi ta prota 2 kelia einai H, global_depth 
                                                  //*2 epeidi exoume zeygaria {bitstring id, pointer}
              error= BF_GetBlock(indexDesc, bucket, new_block3);
              if (error) {
                  BF_PrintError(error);
                  return HT_ERROR;
              }
              insert_record_in_file (indexDesc);
              new_blockdata3= BF_Block_GetData(new_block3);
              num_of_records= 1;
              memcpy(new_blockdata3 + 2*(sizeof(char)), &num_of_records, sizeof(char));
              BF_Block_SetDirty (new_block3);
              memcpy(new_blockdata3 + 3*(sizeof(char)), &record, sizeof(Record)); 
              printf("Ultimatelly record with ID = %d was inserted in block %d\n", record.id, bucket);
              BF_Block_SetDirty (new_block3);
              BF_Block_SetDirty(block);
              BF_Block_SetDirty(new_block);
              BF_Block_SetDirty(new_block2);
              BF_Block_SetDirty(new_block3);
             
              //Vres to tupleId
              *tupleId= (bucket-1)*8 + num_of_records;
             
              error= BF_UnpinBlock(block);
              if (error) {
                  BF_PrintError(error);
                  return HT_ERROR;
              }
             error= BF_UnpinBlock(new_block);
             if (error) {
                 BF_PrintError(error);
                 return HT_ERROR;
             }
             error= BF_UnpinBlock(new_block2);
             if (error) {
                 BF_PrintError(error);
                 return HT_ERROR;
             }
             error= BF_UnpinBlock(new_block3);
             if (error) {
                 BF_PrintError(error);
                 return HT_ERROR;
             }
            
              //Gia ka8e mia apo tis idi yparxouses eggrafes kane 3ana ana8esi sto sosto block
              //kalontas anadromika tin synartasi eisagogis 
              printf("Now lets distribute the records of the block that\n");
              printf("was separated into the old block and the new block:\n");
              printf("-----------------------------------------------------\n");
              for (i= 0 ; i < BF_BLOCK_SIZE/sizeof(Record) ; i++ ) {
                error= HT_InsertEntry(indexDesc, recordArray[i], &new_tupleId, updateArrayNull);
                if (error) {
                  return HT_ERROR;
                }
                //Enimeronoume ton pinaka update array me to new tupple id
                (updateArray)[i].newTupleId= new_tupleId;
                reduce_record_counter (indexDesc);
              }
              printf("-----------------------------------------------------\n");

              insert_block_in_file (indexDesc);

          }
          //An to local depth tou bucket einai iso toy global_depth
          else if(new_blockdata[1] == global_depth) {
              printf("Block's local_depth(%d) == global_depth (%d)\n",new_blockdata[1], global_depth);
              printf("Hash table must be doubled!\n");
              //Apo8ikeyoume ton hash table tou block 0 se enan prosorino pinaka
              memcpy(old_hashtable, blockdata, 512 * sizeof(char)); 

              //Adeizoume ton hash table (ektos tou arxikou H sto keli 0)
              //Enallaktika:  memset(blockdata+1, 0, 511);
              for (i=1 ; i < 512 ; i++) {
                blockdata[i]= 0;
                BF_Block_SetDirty (block);
              }
              //Ay3anoume to global depth kata 1 (diplasiazoume ton pinaka)
              global_depth++;
              memcpy(blockdata+1, &global_depth, sizeof(char));
              BF_Block_SetDirty (block);

              //Eisagoume ston hash table ta bitstring id apo 0 mexri (2^global_depth)-1
              bitstring_id= 0;
              for (i=2 ; i <= 2*pow(2.0, global_depth) ; i+=2) {
                  memcpy(blockdata+i, &bitstring_id, sizeof(char));
                  BF_Block_SetDirty (block);
                  bitstring_id++;
              }
             //Eisagoume ston hash table ton pointer pou antistoixei sto ka8e bucket
              for (i=3 ; i <= 2 * pow(2.0, global_depth)+1 ; i+=2) {
                  memcpy(blockdata+i, old_hashtable+(2 * (blockdata[i-1]/2) + 3) , sizeof(char));
                  BF_Block_SetDirty (block);
              } 

              //Desmeyetai ena neo block
              error= BF_AllocateBlock(indexDesc, new_block2);
              if (error) {
                BF_PrintError(error);
              return HT_ERROR;
              }
              error= BF_GetBlockCounter(indexDesc, &blocksnumber);
              if (error) {
                  BF_PrintError(error);
                  return HT_ERROR;
              }
                            
              //Na doume mipos xreiazetai na paroume to new_blockdata2 me ton olokliromeno tropo
              new_blockdata2= BF_Block_GetData(new_block2);
              //Arxikopoiisi toy neou block
              inserted_char= 'D';
              memcpy (new_blockdata2, &inserted_char, sizeof(char));
              BF_Block_SetDirty (new_block2);
              memcpy(new_blockdata2 + 1, &global_depth, sizeof(char));
              BF_Block_SetDirty (new_block2);
              //Enimerosi tou hash table me ton pointer pou deixnei sto neo block
              blocksnumber--;
              memcpy(blockdata+((hash_value*2+1)*2+3), &blocksnumber, sizeof(char));
              BF_Block_SetDirty (block);


              //Allazoume to local depth kai tou paliou block pou espase
              memcpy(new_blockdata+1, &global_depth, sizeof(char));
              BF_Block_SetDirty (new_block);

              //Apo edo kai kato idio me (a)
             
              //Apo8ikeyoume tis eggrafes tou block pou 8a diaire8ei se enan prosorino pinaka eggrafon
              for (i= 0; i < BF_BLOCK_SIZE/sizeof(Record) ; i++) {
                  memcpy(recordArray+i, new_blockdata+(3+ i * sizeof(Record)), sizeof(Record));
                  //Arxikopoioume ton pinaka update array
                  strcpy( (updateArray)[i].surname, recordArray[i].surname );
                  strcpy( (updateArray)[i].city, recordArray[i].city );
                  (updateArray)[i].oldTupleId= (bucket-1) *8 + (i+1);
              }
              //Vale tin nea eggrafi sto sosto bucket
              hash_value= hash(record.id, global_depth);
              //Vres se poio bucket peftei i nea eggrafi
              bucket= blockdata[hash_value*2 +3]; //+3 epeidi ta prota 2 kelia einai H, global_depth 
                                                  //*2 epeidi exoume zeygaria {bitstring id, pointer}
              error= BF_GetBlock(indexDesc, bucket, new_block3);
              if (error) {
                  BF_PrintError(error);
                  return HT_ERROR;
              }
              insert_record_in_file (indexDesc);
              new_blockdata3= BF_Block_GetData(new_block3);
              num_of_records= 1;
              memcpy(new_blockdata3 + 2*(sizeof(char)), &num_of_records, sizeof(char));
              BF_Block_SetDirty (new_block3);
              memcpy(new_blockdata3 + 3*(sizeof(char)), &record, sizeof(Record)); 
              printf("Ultimately, record with ID = %d was inserted in block %d\n", record.id, bucket );
              BF_Block_SetDirty (new_block3);
              BF_Block_SetDirty(block);
              BF_Block_SetDirty(new_block);
              BF_Block_SetDirty(new_block2);
              BF_Block_SetDirty(new_block3);

              //Vres to tupleId
              *tupleId= (bucket-1)*8 + num_of_records;

              error= BF_UnpinBlock(block);
              if (error) {
                  BF_PrintError(error);
                  return HT_ERROR;
              }
             error= BF_UnpinBlock(new_block);
             if (error) {
                 BF_PrintError(error);
                 return HT_ERROR;
             }
             error= BF_UnpinBlock(new_block2);
             if (error) {
                 BF_PrintError(error);
                 return HT_ERROR;
             }
             error= BF_UnpinBlock(new_block3);
             if (error) {
                 BF_PrintError(error);
                 return HT_ERROR;
             }

            
            
              //Gia ka8e mia apo tis idi yparxouses eggrafes kane 3ana ana8esi sto sosto block
              //kalontas anadromika tin synartasi eisagogis 
               printf("Now lets distribute the records of the block that\n");
              printf("was separated into the old block and the new block:\n");
              printf("-----------------------------------------------------\n");
              for (i= 0 ; i < BF_BLOCK_SIZE/sizeof(Record) ; i++ ) {
                error= HT_InsertEntry(indexDesc, recordArray[i], &new_tupleId, updateArrayNull);
                if (error) {
                  return HT_ERROR;
                }
                //Enimeronoume ton pinaka update array me to new tupple id
                (updateArray)[i].newTupleId= new_tupleId;
                reduce_record_counter (indexDesc);
              }
              printf("-----------------------------------------------------\n");
              insert_block_in_file (indexDesc);

          }
          else {
              printf("Local depth > Global depth ???\n");
              return HT_ERROR;
          }
      }

  }
  free (recordArray);

  
  // BF_GetBlock(indexDesc, 0, block);
  // blockdata= BF_Block_GetData(block);

  return HT_OK;
}

HT_ErrorCode HT_PrintAllEntries(int indexDesc, int *id) {
  //insert code here
  int error, blocksnumber, num_of_records_in_block, i, j, k;
  BF_Block *hashblock, *datablock;
  char* hashblock_data, *datablock_data;
  char hash_value, bucket;
  Record *recordArray;

  BF_Block_Init (&hashblock);
  BF_Block_Init (&datablock);

  //Pairnoume to hash table
  error= BF_GetBlock(indexDesc, 0, hashblock);
  if (error) {
      BF_PrintError(error);
      return HT_ERROR;
  }
  hashblock_data= BF_Block_GetData(hashblock);
 
  //An prepei na typosoume mia eggrafi
  if (id != NULL) {
      //Vres to hash_value tis eggrafis (vres se poio bitstring_id peftei i eggrafi) 
      hash_value= hash(*id, hashblock_data[1]);

      //Vres to bucket to sygkekrimenou bitstring_id
      bucket= hashblock_data[2*hash_value+3];

      error= BF_GetBlock(indexDesc, bucket, datablock);
      if (error) {
          BF_PrintError(error);
          return HT_ERROR;
      }
      datablock_data= BF_Block_GetData(datablock);
      num_of_records_in_block= datablock_data[2];

      recordArray= malloc (num_of_records_in_block * sizeof(Record));
      for (i= 0 ; i < num_of_records_in_block ; i++) {
         memcpy(recordArray+i, datablock_data+(i*sizeof(Record)+3), sizeof(Record));
      }

      for (i= 0 ; i < num_of_records_in_block ; i++) {
          if (recordArray[i].id == *id) {
              printf("Record's id: %d\n", recordArray[i].id);
              printf("Name: %s\n", recordArray[i].name);
              printf("Surame: %s\n", recordArray[i].surname);
              printf("City: %s\n", recordArray[i].city);
              break;
          }
          printf("\n");
      }
      BF_UnpinBlock (datablock);
      BF_UnpinBlock (hashblock);
      free (recordArray);
  } 
  //Print all records
  else { 
      bucket= 0;
      //Gia ka8e bitstring_id
      for (i= 0 ; i < pow(2.0, hashblock_data[1]); i++) {
          //An to bitstring id deixnei pragmati se kapoio block 
          if (hashblock_data[3+i*2] != 0) {
          //An den einai buddy tou proigoumenou to trexon bitstring_id
              if (bucket != hashblock_data[3+i*2]) {
                  //Pare to bucket to sygkekrimenou bitstring_id
                  bucket= hashblock_data[3+i*2];
                  //Pare to block sto opoio deixnei to bucket
                  error= BF_GetBlock(indexDesc, bucket, datablock);
                  if (error) {
                      BF_PrintError(error);
                      return HT_ERROR;
                  }
                  //Pare ta dedomena tou block
                  datablock_data= BF_Block_GetData(datablock);
                  //Vres ton ari8mo ton eggrafon poy einai apo8ikeymenes sto block
                  num_of_records_in_block= datablock_data[2];
                  //Desmeyse enan pinaka pou xoraei tis eggrafes
                  recordArray= malloc (num_of_records_in_block * sizeof(Record));
                  //Perna tis eggrafes ston pinaka
                  for (k= 0 ; k < num_of_records_in_block ; k++) {
                      memcpy(recordArray+k, datablock_data+(k*sizeof(Record)+3), sizeof(Record));
                  }
                  //Gia ka8e eggrafi
                  printf("Bitstring ID: %d (points at Block with Block_ID: %d)\n", i, bucket);
                  for (j= 0 ; j < num_of_records_in_block ; j++) {
                      //Typose ta stoixeia tis
                      printf("Record's id: %d\n", recordArray[j].id);
                      printf("Name: %s\n", recordArray[j].name);
                      printf("Surame: %s\n", recordArray[j].surname);
                      printf("City: %s\n", recordArray[j].city);
                  }
                  printf("\n\n");
                  error= BF_UnpinBlock(datablock);
                  if (error) {
                    BF_PrintError(error);
                    return HT_ERROR;
                  }
                  free (recordArray);
              }
          }
      }
      error= BF_UnpinBlock(hashblock);
      if (error) {
          BF_PrintError(error);
          return HT_ERROR;
      }
  }
  return HT_OK;
}

HT_ErrorCode HashStatistics( char* filename ) 
{ 
    int i, min, max, bucket, num_of_records_in_block, sum_of_local_depths, error, indexDesc;
    BF_Block *hashblock, *datablock;
    char *hashblock_data, *datablock_data;
    
    max = -1;
    min= BF_BLOCK_SIZE/sizeof(Record) + 1;
    sum_of_local_depths= 0;
    indexDesc= get_indexDesc(filename);
    
    BF_Block_Init(&hashblock);
    BF_Block_Init(&datablock);

    error= BF_GetBlock(indexDesc, 0, hashblock);
    if (error) {
        BF_PrintError(error);
        return HT_ERROR;
    }
    hashblock_data= BF_Block_GetData(hashblock);

    //Vres tis ligoteres eggrafes kai tis perissoteres eggrafes apo ola ta blocks 
    bucket= 0;
      //Gia ka8e bitstring_id
      for (i= 0 ; i < pow(2.0, hashblock_data[1]); i++) {
          //An to bitstring id deixnei pragmati se kapoio block 
          if (hashblock_data[3+i*2] != 0) {
          //An den einai buddy tou proigoumenou to trexon bitstring_id
              if (bucket != hashblock_data[3+i*2]) {
                  //Pare to bucket to sygkekrimenou bitstring_id
                  bucket= hashblock_data[3+i*2];
                  //Pare to block sto opoio deixnei to bucket
                  error= BF_GetBlock(indexDesc, bucket, datablock);
                  if (error) {
                      BF_PrintError(error);
                      return HT_ERROR;
                  }
                  //Pare ta dedomena tou block
                  datablock_data= BF_Block_GetData(datablock);
                  //Vres ton ari8mo ton eggrafon poy einai apo8ikeymenes sto block
                  num_of_records_in_block= datablock_data[2];
                  if (num_of_records_in_block < min) {
                    min= num_of_records_in_block;
                  }
                  if (num_of_records_in_block > max) {
                    max = num_of_records_in_block;
                  }
                  sum_of_local_depths+= datablock_data[1]; 
              }
          }
          error= BF_UnpinBlock(datablock);
          if (error) {
            BF_PrintError(error);
            return HT_ERROR;
          }
      }
    error= BF_UnpinBlock(hashblock);
    if (error) {
      BF_PrintError(error);
      return HT_ERROR;
    }
    

    print_statistics(filename, min, max, sum_of_local_depths);
    return HT_OK;
}
  

