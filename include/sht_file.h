#ifndef HASH_FILE_H
#define HASH_FILE_H

#include "hash_file.h"
#include "structs.h"
#include <math.h>

char hash_string(char *key, char global_depth);

HT_ErrorCode SHT_Init();

HT_ErrorCode SHT_CreateSecondaryIndex(
const char *sfileName, /* όνομα αρχείου */
char *attrName, /* όνομα πεδίου-κλειδιού */
int attrLength, /* μήκος πεδίου-κλειδιού */
int depth,/* το ολικό βάθος ευρετηρίου επεκτατού κατακερματισμού */
char *fileName /* όνομα αρχείου πρωτεύοντος ευρετηρίου*/);

HT_ErrorCode SHT_OpenSecondaryIndex(
const char *sfileName, /* όνομα αρχείου */
int *indexDesc /* θέση στον πίνακα με τα ανοιχτά αρχεία που επιστρέφεται */ );

HT_ErrorCode SHT_CloseSecondaryIndex(int indexDesc /* θέση στον πίνακα με τα ανοιχτά αρχεία */ );

HT_ErrorCode SHT_SecondaryInsertEntry (
int indexDesc, /* θέση στον πίνακα με τα ανοιχτά αρχεία */
SecondaryRecord record /* δομή που προσδιορίζει την εγγραφή */ );

HT_ErrorCode SHT_SecondaryUpdateEntry (
int indexDesc, /* θέση στον πίνακα με τα ανοιχτά αρχεία */
UpdateRecordArray *updateArray /* δομή που προσδιορίζει την παλιά εγγραφή */);

HT_ErrorCode SHT_PrintAllEntries(
int sindexDesc, /* θέση στον πίνακα με τα ανοιχτά αρχεία  του αρχείου δευτερεύοντος ευρετηρίου */
char *index_key /* τιμή του πεδίου-κλειδιού προς αναζήτηση */);

HT_ErrorCode SHT_HashStatistics(char *filename /* όνομα του αρχείου που ενδιαφέρει */);

HT_ErrorCode SHT_InnerJoin(
int sindexDesc1, /* θέση στον πίνακα με τα ανοιχτά αρχεία  του αρχείου δευτερεύοντος ευρετηρίου για το πρώτο αρχείο εισόδου */
int sindexDesc2, /* θέση στον πίνακα με τα ανοιχτά αρχεία του αρχείου δευτερεύοντος ευρετηρίου για το δεύτερο αρχείο εισόδου */
char *index_key  /* το κλειδι πανω στο οποιο θα γινει το join. Αν  NULL τοτε επιστρέφεί όλες τις πλειάδες*/);


#endif // HASH_FILE_H
