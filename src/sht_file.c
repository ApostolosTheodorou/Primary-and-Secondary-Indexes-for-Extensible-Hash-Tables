#ifndef HASH_FILE_H
#define HASH_FILE_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "sht_file.h"
#include "structs.h"
#include "filetable.h"
#include <math.h>

#define MAX_OPEN_FILES 20

#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {         \
    BF_PrintError(code);    \
    return HP_ERROR;        \
  }                         \
}



/* Source: 
http://www.cse.yorku.ca/~oz/hash.html */
char hash_string(char *key, char global_depth)
{
   long long hash_value;
   char *character;

   hash_value = 0;
   for (character = key; *character != '\0'; *character++) {
     //printf("current character is %c with value %d\n", *character, *character);
     // hash_value = 31 * hash_value + *character;
      //printf("Now hash value is: %lld\n", hash_value);
    hash_value = 33 * hash_value + *character;
   }
      
   //printf("After the loop hash_value: %lld\n", hash_value);
   hash_value = hash_value % (int) pow(2.0, 7);
   //printf("After the mod hash_value: %lld\n", hash_value);
   hash_value= hash_value >> (7- global_depth);
  // printf("After the shift hash_value: %lld\n", hash_value);
   
   return hash_value; 
}


HT_ErrorCode SHT_Init() {
  //insert code here
  return HT_OK;
}

HT_ErrorCode SHT_CreateSecondaryIndex(const char *sfileName, char *attrName, int attrLength, int depth, char *fileName ) {
  //insert code here
  int filedesc, i, blocksnumber,error, primary_fileDesc;
  char *blockdata;
  BF_Block *block;
  char inserted_char;
  
  BF_Block_Init(&block);
  error = BF_CreateFile(sfileName);
  if (error) {
      BF_PrintError(error);
      return error;
  }
  error = BF_OpenFile(sfileName, &filedesc);
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

  inserted_char= 'S';
  memcpy (blockdata, &inserted_char, sizeof(char));
  BF_Block_SetDirty (block);
  inserted_char= (char)depth;
  memcpy (blockdata + sizeof(char), &inserted_char, sizeof(char));
  BF_Block_SetDirty (block);
  primary_fileDesc= get_indexDesc(fileName);
  if (primary_fileDesc == -1) {
      printf("The primary file does not exist!\n");
      return HT_ERROR;
  }
  inserted_char= (char) primary_fileDesc;
  memcpy (blockdata + 2*sizeof(char), &inserted_char, sizeof(char));
  if (strcmp(attrName, "surname") == 0) {
    inserted_char= 's';
  }
  else {
      inserted_char= 'c';
  }
  memcpy (blockdata + 3*sizeof(char), &inserted_char, sizeof(char));

  for (i = 0; i < pow(2, depth); i++)
    {
      inserted_char= (char) i;
      memcpy(blockdata + 4 + 2*i *sizeof(char), &inserted_char, sizeof(char));
      BF_Block_SetDirty (block);
      inserted_char= (char) 0;
      memcpy(blockdata + 4 + 2*i * sizeof(char) + 1, &inserted_char, sizeof(char));
      BF_Block_SetDirty (block);
    }

  BF_UnpinBlock (block);
    
  error = BF_CloseFile (filedesc);
  if (error) {
      BF_PrintError(error);
      return error;
  }

  return HT_OK;
}

HT_ErrorCode SHT_OpenSecondaryIndex(const char *sfileName, int *indexDesc  ) {
  //insert code here
  int temp;
  int error = BF_OpenFile(sfileName, &temp) ;

  if (error) {
      BF_PrintError(error);
      return error;
  }
  if (temp >= MAX_OPEN_FILES) {
      printf("Too many open files!\n");
      return HT_ERROR;
  }
  *indexDesc = temp;
  insert_filetable(*indexDesc, sfileName, "secondary");
  return HT_OK;
}

HT_ErrorCode SHT_CloseSecondaryIndex(int indexDesc) {
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

HT_ErrorCode SHT_SecondaryInsertEntry (int indexDesc,SecondaryRecord record  ) {
  printf("\n\nRecord with ID = %s arrived in the secondary file!\n", record.index_key);
  //insert code here
  int  error, blocksnumber, i;
  BF_Block* block, *new_block, *new_block2, *new_block3;
  char* blockdata, *new_blockdata, *new_blockdata2, *new_blockdata3;
  char global_depth, local_depth, bucket, inserted_char, num_of_records, hash_value,
       position, num_of_buddies, old_hashtable[BF_BLOCK_SIZE], bitstring_id, temp,
       first_buddys_bitstring;
  SecondaryRecord* recordArray;

  recordArray= malloc(BF_BLOCK_SIZE/sizeof(SecondaryRecord) * sizeof(SecondaryRecord));
  
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

  hash_value= hash_string(record.index_key, global_depth);

  printf("\nRecord's hash value is %d\n", hash_value);
  
  //Vres se poio bucket peftei i eggrafi
  bucket= blockdata[hash_value*2 +4 +1]; // +4 epeidi ta Bitstring Ids 3ekinane sti 8esi 4, 
                                         // +1 giati to Block Id einai mia 8esi pio kato
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
    memcpy (new_blockdata + 3*sizeof(char), &record, sizeof(SecondaryRecord));
    BF_Block_SetDirty (new_block);

    error = BF_GetBlockCounter(indexDesc, &blocksnumber);
    if (error) {
      BF_PrintError(error);
      return error;
  }
    temp= blocksnumber -1;
    memcpy(blockdata+(hash_value*2 +4 +1), &temp, sizeof(char) );
    BF_Block_SetDirty (block);

    printf("Ultimately, record with ID = %s was inserted in block %d\n", record.index_key, temp);

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
      //An to block xoraei ki alles eggrafes (BF_BLOCK_SIZE/sizeof(Record)= 512/24=21)
      
      if (new_blockdata[2] < BF_BLOCK_SIZE/sizeof(SecondaryRecord)) { 
        memcpy(new_blockdata+(new_blockdata[2]*sizeof(SecondaryRecord)+3)*(sizeof(char)), &record, sizeof(SecondaryRecord));
        BF_Block_SetDirty (new_block);
        num_of_records= new_blockdata[2]+1;
      
        memcpy(new_blockdata+2*sizeof(char), &num_of_records, sizeof(char));
        BF_Block_SetDirty (new_block);
        printf("The record with ID = %s was inserted in block (%d)\nNow block %d has %d records\n",
                 record.index_key, bucket, bucket, new_blockdata[2]);
        insert_record_in_file (indexDesc);
        BF_Block_SetDirty(new_block);

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
      else if (new_blockdata[2] == BF_BLOCK_SIZE/sizeof(SecondaryRecord)) {

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
              for (i= 4 ; i < 512 ; i+=2) {
                //An to vrikame
                if (blockdata[i+1] == bucket) {
                    first_buddys_bitstring= blockdata[i];
                    break;
                }
              }
              //Enimerosi tou hash table
              //Oi protoi misoi buddies 8a deixnoun sto palio block
              //Oi deyteroi misoi buddies 8a deixnoun sto kainourio block 
              for (i= 0; i < num_of_buddies/2 ; i++) {
                  memcpy(blockdata+(num_of_buddies + 5 + (first_buddys_bitstring+i)*2)*sizeof(char), &blocksnumber, sizeof(char));
                  BF_Block_SetDirty (block);
              }



              //Apo8ikeyoume tis eggrafes tou block pou 8a diaire8ei se enan prosorino pinaka eggrafon
              for (i= 0; i < BF_BLOCK_SIZE/sizeof(SecondaryRecord) ; i++) {
                  memcpy(recordArray+i, new_blockdata+(3+ i * sizeof(SecondaryRecord)), sizeof(SecondaryRecord));

              }
              //Adeiazoume to palio block 
              for (i=2 ; i < 512 ; i++) {
                new_blockdata[i]= 0;
                BF_Block_SetDirty (new_block);
              }
              //Vale tin nea eggrafi sto sosto bucket
              hash_value= hash_string(record.index_key, global_depth);
              //Vres se poio bucket peftei i nea eggrafi
              bucket= blockdata[hash_value*2 +4 +1]; 
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
              memcpy(new_blockdata3 + 3*(sizeof(char)), &record, sizeof(SecondaryRecord)); 
              printf("Ultimatelly record with ID = %s was inserted in block %d\n", record.index_key, bucket);
              BF_Block_SetDirty (new_block3);
              BF_Block_SetDirty(block);
              BF_Block_SetDirty(new_block);
              BF_Block_SetDirty(new_block2);
              BF_Block_SetDirty(new_block3);
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
              for (i= 0 ; i < BF_BLOCK_SIZE/sizeof(SecondaryRecord) ; i++ ) {
                error= SHT_SecondaryInsertEntry(indexDesc, recordArray[i]);
                if (error) {
                  return HT_ERROR;
                }
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

              //Adeizoume ton hash table (ektos tou arxikou S sto keli 0)
              //Enallaktika:  memset(blockdata+1, 0, 511);
              for (i=4 ; i < 512 ; i++) {
                blockdata[i]= 0;
                BF_Block_SetDirty (block);
              }
              //Ay3anoume to global depth kata 1 (diplasiazoume ton pinaka)
              global_depth++;
              memcpy(blockdata+1, &global_depth, sizeof(char));
              BF_Block_SetDirty (block);

              //Eisagoume ston hash table ta bitstring id apo 0 mexri (2^global_depth)-1
              bitstring_id= 0;
              for (i=4 ; i <= 2*pow(2.0, global_depth) + 2 ; i+=2) {
                  memcpy(blockdata+i, &bitstring_id, sizeof(char));
                  BF_Block_SetDirty (block);
                  bitstring_id++;
              }
             //Eisagoume ston hash table ton pointer pou antistoixei sto ka8e bucket
              for (i=5 ; i <= 2 * pow(2.0, global_depth)+3 ; i+=2) {
                  memcpy(blockdata+i, old_hashtable+(2 * (blockdata[i-1]/2) + 4 + 1) , sizeof(char));
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
              memcpy(blockdata+((hash_value*2+1)*2+5), &blocksnumber, sizeof(char));
              BF_Block_SetDirty (block);


              //Allazoume to local depth kai tou paliou block pou espase
              memcpy(new_blockdata+1, &global_depth, sizeof(char));
              BF_Block_SetDirty (new_block);

              //Apo edo kai kato idio me (a)
             
              //Apo8ikeyoume tis eggrafes tou block pou 8a diaire8ei se enan prosorino pinaka eggrafon
              for (i= 0; i < BF_BLOCK_SIZE/sizeof(SecondaryRecord) ; i++) {
                  memcpy(recordArray+i, new_blockdata+(3+ i * sizeof(SecondaryRecord)), sizeof(SecondaryRecord));
              }
              //Vale tin nea eggrafi sto sosto bucket
              hash_value= hash_string(record.index_key, global_depth);
              //Vres se poio bucket peftei i nea eggrafi
              bucket= blockdata[hash_value*2 +4 +1];
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
              memcpy(new_blockdata3 + 3*(sizeof(char)), &record, sizeof(SecondaryRecord)); 
              printf("Ultimately, record with ID = %s was inserted in block %d\n", record.index_key, bucket );
              BF_Block_SetDirty (new_block3);
              BF_Block_SetDirty(block);
              BF_Block_SetDirty(new_block);
              BF_Block_SetDirty(new_block2);
              BF_Block_SetDirty(new_block3);
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
              for (i= 0 ; i < BF_BLOCK_SIZE/sizeof(SecondaryRecord) ; i++ ) {
                error= SHT_SecondaryInsertEntry(indexDesc, recordArray[i]);
                if (error) {
                  return HT_ERROR;
                }
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

HT_ErrorCode SHT_SecondaryUpdateEntry (int indexDesc, UpdateRecordArray *updateArray ) {
  //insert code here
  int i, j, error, tuppleId;
  BF_Block* hash_block, *data_block;
  char* hash_data, *blocks_data, hash_value, bucket;
  SecondaryRecord record;

  BF_Block_Init (&hash_block);
  BF_Block_Init (&data_block);

  error= BF_GetBlock(indexDesc, 0, hash_block);
  if (error) {
      BF_PrintError(error);
      return HT_ERROR;
  }
  hash_data= BF_Block_GetData(hash_block);

      for (i=0 ; i < BF_BLOCK_SIZE/sizeof(Record) ; i++) {
          if (hash_data[3] == 's') {
              hash_value= hash_string(updateArray[i].surname, hash_data[1]);
              bucket= hash_data[hash_value*2 +4 +1];
              error= BF_GetBlock(indexDesc, bucket, data_block);
              if (error) {
                  BF_PrintError(error);
                  return HT_ERROR;
              }
              blocks_data= BF_Block_GetData(data_block);
              for (j= 0 ; j < blocks_data[2] ; j++) {
                  memcpy(&record, blocks_data+(j*sizeof(SecondaryRecord)+3), sizeof(SecondaryRecord));
                  if (updateArray[i].oldTupleId == record.tupleId) {
                      record.tupleId= updateArray[i].newTupleId;
                      memcpy(blocks_data+(j*sizeof(SecondaryRecord)+3), &record, sizeof(SecondaryRecord));
                      BF_Block_SetDirty(data_block);
                  }
              }
          }
          else if (hash_data[3] == 'c') {
              hash_value= hash_string(updateArray[i].city, hash_data[1]);
              bucket= hash_data[hash_value*2 +4 +1];
              error= BF_GetBlock(indexDesc, bucket, data_block);
              if (error) {
                  BF_PrintError(error);
                  return HT_ERROR;
              }
              blocks_data= BF_Block_GetData(data_block);
              for (j= 0 ; j < blocks_data[2] ; j++) {
                  memcpy(&record, blocks_data+(j*sizeof(SecondaryRecord)+3), sizeof(SecondaryRecord));
                  if (updateArray[i].oldTupleId == record.tupleId) {
                      record.tupleId= updateArray[i].newTupleId;
                      memcpy(blocks_data+(j*sizeof(SecondaryRecord)+3), &record, sizeof(SecondaryRecord));
                      BF_Block_SetDirty(data_block);
                  }
              }
          }
      }
      error= BF_UnpinBlock(data_block);
      error= BF_UnpinBlock(hash_block);
      if (error) {
          BF_PrintError(error);
          return HT_ERROR;
      }
  return HT_OK;
}

HT_ErrorCode SHT_PrintAllEntries(int sindexDesc, char *index_key ) {
  //insert code here
  int error, blocksnumber, num_of_records_in_block, i, j, k, indexDesc, tuppleId;
  BF_Block *hashblock, *datablock, *primaryFile;
  char* hashblock_data, *datablock_data, *primary_data;
  char hash_value, bucket, primary_bucket, position_in_block;
  SecondaryRecord *recordArray;
  Record primaryRecord;

  BF_Block_Init (&hashblock);
  BF_Block_Init (&datablock);
  BF_Block_Init (&primaryFile);

  //Pairnoume to hash table
  error= BF_GetBlock(sindexDesc, 0, hashblock);
  if (error) {
      BF_PrintError(error);
      return HT_ERROR;
  }
  hashblock_data= BF_Block_GetData(hashblock);
 

  //Vres to hash_value tis eggrafis (vres se poio bitstring_id peftei i eggrafi) 
  hash_value= hash_string(index_key, hashblock_data[1]);

  //Vres to bucket to sygkekrimenou bitstring_id
  bucket= hashblock_data[2*hash_value+4 +1];

  error= BF_GetBlock(sindexDesc, bucket, datablock);
  if (error) {
      BF_PrintError(error);
      return HT_ERROR;
  }
  datablock_data= BF_Block_GetData(datablock);
  num_of_records_in_block= datablock_data[2];

  recordArray= malloc (num_of_records_in_block * sizeof(SecondaryRecord));
  for (i= 0 ; i < num_of_records_in_block ; i++) {
     memcpy(recordArray+i, datablock_data+(i*sizeof(SecondaryRecord)+3), sizeof(SecondaryRecord));
  }

  indexDesc= hashblock_data[2];
  for (i= 0 ; i < num_of_records_in_block ; i++) {
      if (strcmp(recordArray[i].index_key, index_key) == 0) {
          tuppleId= recordArray[i].tupleId;
          if (tuppleId % 8 != 0) {
            primary_bucket= (tuppleId/8) +1;
            position_in_block= tuppleId % 8;
          }
          else {
              primary_bucket= (tuppleId/8);
              position_in_block= 8;
          }
          
          error= BF_GetBlock(indexDesc, primary_bucket, primaryFile);
          if (error) {
              BF_PrintError(error);
              return HT_ERROR;
          }
          printf("TuppleId= %d,  primary_bucket= %d,  position_in_block= %d\n", tuppleId, primary_bucket, position_in_block);
          primary_data= BF_Block_GetData(primaryFile);
          memcpy(&primaryRecord, primary_data+3 + (position_in_block-1) * sizeof(Record), sizeof(Record));
          printf("Record's id: %d\n", primaryRecord.id);
          printf("Name: %s\n", primaryRecord.name);
          printf("Surame: %s\n", primaryRecord.surname);
          printf("City: %s\n", primaryRecord.city);
      }
      printf("\n");
  }
  error= BF_UnpinBlock (datablock);
  if (error) {
      BF_PrintError(error);
      return HT_ERROR;
  }
  error= BF_UnpinBlock (hashblock);
  if (error) {
      BF_PrintError(error);
      return HT_ERROR;
  }
  error= BF_UnpinBlock (primaryFile);
  if (error) {
      BF_PrintError(error);
      return HT_ERROR;
  }
  free (recordArray);

  return HT_OK;
}

HT_ErrorCode SHT_HashStatistics(char *filename ) {
    
    int i, min, max, bucket, num_of_records_in_block, sum_of_local_depths, error, indexDesc;
    BF_Block *hashblock, *datablock;
    char *hashblock_data, *datablock_data;
    
    max = -1;
    min= BF_BLOCK_SIZE/sizeof(SecondaryRecord) + 1;
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
          if (hashblock_data[i*2+4 +1] != 0) {
          //An den einai buddy tou proigoumenou to trexon bitstring_id
              if (bucket != hashblock_data[i*2 +4 +1]) {
                  //Pare to bucket to sygkekrimenou bitstring_id
                  bucket= hashblock_data[i*2 +4 +1];
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

HT_ErrorCode SHT_InnerJoin(int sindexDesc1, int sindexDesc2,  char *index_key ) {
  //insert code here
    //insert code here
  int error, blocksnumber, num_of_records_in_block, i, j, k, l, indexDesc1, indexDesc2, tuppleId;
  BF_Block *hashblock1, *datablock1, *hashblock2, *datablock2, *primaryFile1, *primaryFile2;
  char* hashblock_data1, *datablock_data1, *hashblock_data2, *datablock_data2, *primary_data1, *primary_data2;
  char hash_value1, hash_value2, bucket1, bucket2, primary_bucket, position_in_block, 
       global_depth1, global_depth2, num_of_records_in_block1, num_of_records_in_block2;
  SecondaryRecord *recordArray1, *recordArray2 ;
  Record primaryRecord1, primaryRecord2;

  BF_Block_Init (&hashblock1);
  BF_Block_Init (&datablock1);
  BF_Block_Init (&hashblock2);
  BF_Block_Init (&datablock2);
  BF_Block_Init (&primaryFile1);
  BF_Block_Init (&primaryFile2);


  if (index_key != NULL) {
    //Pairnoume to hash table gia to 1o arxeio 
    error= BF_GetBlock(sindexDesc1, 0, hashblock1);
    if (error) {
        BF_PrintError(error);
        return HT_ERROR;
    }
    hashblock_data1= BF_Block_GetData(hashblock1);

    //Vriskoume to global depth tou 1ou arxeiou
    global_depth1= hashblock_data1[1];

    //Pairnoume to hash table gia to 2o arxeio 
    error= BF_GetBlock(sindexDesc2, 0, hashblock2);
    if (error) {
        BF_PrintError(error);
        return HT_ERROR;
    }
    hashblock_data2= BF_Block_GetData(hashblock2);

    //Vriskoume to global depth tou 1ou arxeiou
    global_depth2= hashblock_data2[1];

    //Pairnoume to hash value tou index key gia ka8e arxeio
    hash_value1= hash_string(index_key, global_depth1);
    hash_value2= hash_string(index_key, global_depth2);

    //Vriskoume to block ka8e arxeiou sto opoio peftoun oi eggrafes me index key
    bucket1= hashblock_data1[2*hash_value1+4 +1];
    bucket2= hashblock_data2[2*hash_value1+4 +1];

    error= BF_GetBlock(sindexDesc1, bucket1, datablock1);
    if (error) {
        BF_PrintError(error);
        return HT_ERROR;
    }
    datablock_data1= BF_Block_GetData(datablock1);
    num_of_records_in_block1= datablock_data1[2];
    recordArray1= malloc (num_of_records_in_block1 * sizeof(SecondaryRecord));
    for (i= 0 ; i < num_of_records_in_block1 ; i++) {
     memcpy(recordArray1+i, datablock_data1+(i*sizeof(SecondaryRecord)+3), sizeof(SecondaryRecord));
    }

    error= BF_GetBlock(sindexDesc2, bucket2, datablock2);
    if (error) {
        BF_PrintError(error);
        return HT_ERROR;
    }
    datablock_data2= BF_Block_GetData(datablock2);
    num_of_records_in_block2= datablock_data2[2];
    recordArray2= malloc (num_of_records_in_block2 * sizeof(SecondaryRecord));
    for (i= 0 ; i < num_of_records_in_block2 ; i++) {
     memcpy(recordArray2+i, datablock_data2+(i*sizeof(SecondaryRecord)+3), sizeof(SecondaryRecord));
    }

    indexDesc1= hashblock_data1[2];
    for (i=0 ; i < num_of_records_in_block1 ; i++) {
        if (strcmp(recordArray1[i].index_key, index_key) == 0 ) {
          tuppleId= recordArray1[i].tupleId;
          if (tuppleId % 8 != 0) {
            primary_bucket= (tuppleId/8) +1;
            position_in_block= tuppleId % 8;
          }
          else {
              primary_bucket= (tuppleId/8);
              position_in_block= 8;
          }
          error= BF_GetBlock(indexDesc1, primary_bucket, primaryFile1);
          if (error) {
              BF_PrintError(error);
              return HT_ERROR;
          }
          //printf("TuppleId= %d,  primary_bucket= %d,  position_in_block= %d\n", tuppleId, primary_bucket, position_in_block);
          primary_data1= BF_Block_GetData(primaryFile1);
          memcpy(&primaryRecord1, primary_data1+3 + (position_in_block-1) * sizeof(Record), sizeof(Record));
         
         indexDesc2= hashblock_data2[2];
         for (j=0; j < num_of_records_in_block2; j++) { 
            if (strcmp(recordArray2[j].index_key, index_key) == 0 ) {
                    tuppleId= recordArray2[j].tupleId;
                    if (tuppleId % 8 != 0) {
                        primary_bucket= (tuppleId/8) +1;
                        position_in_block= tuppleId % 8;
                    }
                    else {
                        primary_bucket= (tuppleId/8);
                        position_in_block= 8;
                    }
                    
                    error= BF_GetBlock(indexDesc2, primary_bucket, primaryFile2);
                    if (error) {
                        BF_PrintError(error);
                        return HT_ERROR;
                    }
                    //printf("TuppleId= %d,  primary_bucket= %d,  position_in_block= %d\n", tuppleId, primary_bucket, position_in_block);
                    primary_data2= BF_Block_GetData(primaryFile2);
                    memcpy(&primaryRecord2, primary_data2+3 + (position_in_block-1) * sizeof(Record), sizeof(Record));
                    printf("%s ",index_key);
                    printf("%d %s %s  ", primaryRecord1.id, primaryRecord1.name, primaryRecord1.city);
                    printf("%d %s %s\n", primaryRecord2.id, primaryRecord2.name, primaryRecord2.city);
            }
        }

      }
    }
    error= BF_UnpinBlock (datablock1);
    if (error) {
        BF_PrintError(error);
        return HT_ERROR;
    }
    error= BF_UnpinBlock (hashblock1);
    if (error) {
        BF_PrintError(error);
        return HT_ERROR;
    }
    error= BF_UnpinBlock (primaryFile1);
    if (error) {
        BF_PrintError(error);
        return HT_ERROR;
    }
    free (recordArray1);
    
    error= BF_UnpinBlock (datablock2);
    if (error) {
        BF_PrintError(error);
        return HT_ERROR;
    }
    error= BF_UnpinBlock (hashblock2);
    if (error) {
        BF_PrintError(error);
        return HT_ERROR;
    }
    error= BF_UnpinBlock (primaryFile2);
    if (error) {
        BF_PrintError(error);
        return HT_ERROR;
    }
    free (recordArray2);

  }

  else {
        //Pairnoume to hash table gia to 1o arxeio 
        error= BF_GetBlock(sindexDesc1, 0, hashblock1);
        if (error) {
            BF_PrintError(error);
            return HT_ERROR;
        }
        hashblock_data1= BF_Block_GetData(hashblock1);

        //Vriskoume to global depth tou 1ou arxeiou
        global_depth1= hashblock_data1[1];

        //Pairnoume to hash table gia to 2o arxeio 
        error= BF_GetBlock(sindexDesc2, 0, hashblock2);
        if (error) {
            BF_PrintError(error);
            return HT_ERROR;
        }
        hashblock_data2= BF_Block_GetData(hashblock2);

        //Vriskoume to global depth tou 1ou arxeiou
        global_depth2= hashblock_data2[1];

      bucket1= 0;
      //Gia ka8e bitstring_id
      for (i= 0 ; i < pow(2.0, global_depth1); i++) {
          //An to bitstring id deixnei pragmati se kapoio block 
          if (hashblock_data1[i*2 +4 +1] != 0) {
          //An den einai buddy tou proigoumenou to trexon bitstring_id
              if (bucket1 != hashblock_data1[i*2+4 +1]) {
                  //Pare to bucket to sygkekrimenou bitstring_id
                  bucket1= hashblock_data1[i*2 +4 +1];
                  //Pare to block sto opoio deixnei to bucket
                  error= BF_GetBlock(sindexDesc1, bucket1, datablock1);
                  if (error) {
                      BF_PrintError(error);
                      return HT_ERROR;
                  }
                  //Pare ta dedomena tou block
                  datablock_data1= BF_Block_GetData(datablock1);
                  //Vres ton ari8mo ton eggrafon poy einai apo8ikeymenes sto block
                  num_of_records_in_block1= datablock_data1[2];
                  //Desmeyse enan pinaka pou xoraei tis eggrafes
                  recordArray1= malloc (num_of_records_in_block1 * sizeof(SecondaryRecord));
                  //Perna tis eggrafes ston pinaka
                  for (k= 0 ; k < num_of_records_in_block1 ; k++) {
                      memcpy(recordArray1+k, datablock_data1+(k*sizeof(SecondaryRecord)+3), sizeof(SecondaryRecord));
                  }
                  //Gia ka8e eggrafi
                  //printf("Bitstring ID: %d (points at Block with Block_ID: %d)\n", i, bucket1);

                  indexDesc1= hashblock_data1[2];
                  for (j= 0 ; j < num_of_records_in_block1 ; j++) {
                      tuppleId= recordArray1[j].tupleId;
                      if (tuppleId % 8 != 0) {
                          primary_bucket= (tuppleId/8) +1;
                          position_in_block= tuppleId % 8;
                      }
                      else {
                        primary_bucket= (tuppleId/8);
                        position_in_block= 8;
                      }
                      error= BF_GetBlock(indexDesc1, primary_bucket, primaryFile1);
                      if (error) {
                        BF_PrintError(error);
                        return HT_ERROR;
                      }
                      primary_data1= BF_Block_GetData(primaryFile1);
                      memcpy(&primaryRecord1, primary_data1+3 + (position_in_block-1) * sizeof(Record), sizeof(Record));
                    
                      hash_value2= hash_string(recordArray1[j].index_key, global_depth2);
                      bucket2= hashblock_data2[i*2 +4 +1];
                      error= BF_GetBlock(sindexDesc2, bucket2, datablock2);
                      if (error) {
                          BF_PrintError(error);
                          return HT_ERROR;
                      }
                      datablock_data2= BF_Block_GetData(datablock2);
                      num_of_records_in_block2= datablock_data2[2];
                      recordArray2= malloc (num_of_records_in_block2 * sizeof(SecondaryRecord));
                      for (l= 0 ; l < num_of_records_in_block2 ; l++) {
                          memcpy(recordArray2+l, datablock_data2+(l*sizeof(SecondaryRecord)+3), sizeof(SecondaryRecord));
                      }
                      indexDesc2= hashblock_data2[2];
                      //strcpy(index_key, recordArray1[j].index_key);
                      for (l=0; l < num_of_records_in_block2; l++) { 
                         if (strcmp(recordArray2[l].index_key, recordArray1[j].index_key) == 0 ) {
                             tuppleId= recordArray2[l].tupleId;
                             if (tuppleId % 8 != 0) {
                                primary_bucket= (tuppleId/8) +1;
                                position_in_block= tuppleId % 8;
                            }
                            else {
                                primary_bucket= (tuppleId/8);
                                position_in_block= 8;
                            }
                            error= BF_GetBlock(indexDesc2, primary_bucket, primaryFile2);
                            if (error) {
                                BF_PrintError(error);
                                return HT_ERROR;
                            }
                            //printf("TuppleId= %d,  primary_bucket= %d,  position_in_block= %d\n", tuppleId, primary_bucket, position_in_block);
                            primary_data2= BF_Block_GetData(primaryFile2);
                            memcpy(&primaryRecord2, primary_data2+3 + (position_in_block-1) * sizeof(Record), sizeof(Record));
                            printf("%s ",recordArray1[j].index_key);
                            printf("%d %s %s  ", primaryRecord1.id, primaryRecord1.name, primaryRecord1.city);
                            printf("%d %s %s\n", primaryRecord2.id, primaryRecord2.name, primaryRecord2.city);
                         }
                     }
                    //free(recordArray2);
                  }
                  printf("\n");
                  
              }
          }
          //free(recordArray1);
      }
      error= BF_UnpinBlock (datablock1);
      if (error) {
         BF_PrintError(error);
         return HT_ERROR;
      }
      error= BF_UnpinBlock (hashblock1);
      if (error) {
            BF_PrintError(error);
            return HT_ERROR;
      }
      error= BF_UnpinBlock (primaryFile1);
      if (error) {
         BF_PrintError(error);
         return HT_ERROR;
      }
      free (recordArray1);
                
      error= BF_UnpinBlock (datablock2);
      if (error) {
        BF_PrintError(error);
        return HT_ERROR;
      }
      error= BF_UnpinBlock (hashblock2);
      if (error) {
          BF_PrintError(error);
          return HT_ERROR;
     }
     error= BF_UnpinBlock (primaryFile2);
     if (error) {
         BF_PrintError(error);
         return HT_ERROR;
     }
     free (recordArray2);
      
    }
  return HT_OK;
  }


#endif // HASH_FILE_H