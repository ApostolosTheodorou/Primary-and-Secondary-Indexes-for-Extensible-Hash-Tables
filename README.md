# Primary & secondary indexes for DB systems

The current project's theme is the implementation of both primary and secondary indexes for records stored in extendible hash tables. The records are structs with standard size and consist of the fields ID, Name, Surname, City ({1034, John, Doe, Miami}). Each file is separated in blocks of 512 byte and all the information exchanged between the RAM and the hard disc is being handled in blocks. Therefore special functions of block level information handling are given as a library. The algorithms implemented manage the hash tables' functions, such as tables' creation and intialization, new records' entries, index updates and other relative functions.   

## Concepts Used

-(Indexes for) Extensible Hash Tables

