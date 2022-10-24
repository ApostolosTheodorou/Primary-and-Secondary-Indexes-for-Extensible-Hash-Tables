#include "filetable.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>


static statistics filetable[20];

void insert_filetable (int file_desc, const char *name, char *index_type)
{
    unsigned int size;
    
    if (strcmp(index_type, "primary") == 0) {
        strcpy(filetable[file_desc].type_of_file, "Extendible Hashing File");
    }
    else if (strcmp(index_type, "secondary") == 0) {
        strcpy(filetable[file_desc].type_of_file, "Secondary Extendible Hashing File");
    }
    
    filetable[file_desc].total_num_of_records= 0;
    if (file_desc < 20)
    {
    	size= strlen(name)+1;
        filetable[file_desc].filename = malloc(size * sizeof(char));
        strcpy(filetable[file_desc].filename, name);
    }
}

char* print_filetable(int file_desc)
{
    return filetable[file_desc].filename;
}

void remove_file (int file_desc)
{
    free(filetable[file_desc].filename);
}

void insert_record_in_file (int file_desc) 
{
    filetable[file_desc].total_num_of_records++;
}

void reduce_record_counter (int file_desc)
{
    filetable[file_desc].total_num_of_records--;
}


void insert_block_in_file (int file_desc) 
{
    filetable[file_desc].num_of_buckets++;
}

int get_indexDesc(char* filename) {
    int i, position;

    for (i=0 ; i < 20 ; i++) {
        if (strcmp(filename, filetable[i].filename) == 0){
            position= i;
            return position;
        }
    }
    return -1;
}

void print_statistics(char* filename, int min, int max, int sum_of_local_depths) 
{
    int i, position;
    
    position= get_indexDesc(filename);

    printf("\n___________________________________________________________________________________________________________________\n");
    printf("----------------------------- STATISTICS FOR FILE %s -------------------------------------------------------\n", filename);
    printf(" Number of blocks in file:  %d  (1 for the hash table indexes \
and %d for the real data)\n", filetable[position].num_of_buckets+1, filetable[position].num_of_buckets);
    printf("The maximum number of records in a block is: %d\n", max);
    printf("The minimum number of records in a block is: %d\n", min);
    printf("The total number of records stored in the file is: %d\n", filetable[position].total_num_of_records);
    printf("The average number of records in every block is: %4.2f\n", filetable[position].total_num_of_records/ (double) filetable[position].num_of_buckets);
    printf("The average local depth of every block is: %4.2f\n", sum_of_local_depths/ (double) filetable[position].num_of_buckets);
}