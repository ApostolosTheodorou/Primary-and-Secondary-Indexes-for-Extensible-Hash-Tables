
#ifndef FILETABLE_H
#define FILETABLE_H

void insert_filetable (int file_desc, const char *name, char *index_type);
char* print_filetable (int file_desc);
void remove_file (int file_desc);
void insert_record_in_file (int file_desc);
void reduce_record_counter (int file_desc);
void insert_block_in_file (int file_desc);
void print_statistics(char* name, int min, int max, int sum_of_local_depths);
int get_indexDesc(char* filename);

typedef struct statistics {
    char type_of_file[40];
    int total_num_of_records;
    double avg_num_of_records;
    char* filename;
    double avg_depth;
    int num_of_buckets;
} statistics;

#endif
