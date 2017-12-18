#include "sort_file.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "bf.h"
#include <unistd.h>
#include <errno.h>


#define CALL_OR_DIE(call)     \
  {                           \
    SR_ErrorCode code = call; \
    if (code != SR_OK) {      \
      BF_PrintError(code);    \
      exit(code);             \
    }                         \
  }

/*
 * Η συνάρτηση SR_Init χρησιμοποιείται για την αρχικοποίηση του sort_file.
 * Σε περίπτωση που εκτελεστεί επιτυχώς, επιστρέφεται SR_OK, ενώ σε
 * διαφορετική περίπτωση κάποιος κωδικός λάθους.
 */
SR_ErrorCode SR_Init() {
  // Your code goes here

  return SR_OK;
}


//A Function that is called by HP_InsertEntry to avoid memory leaks
SR_ErrorCode SR_InsertData(char* data,Record record){
    memset(data,0,sizeof(int));
    memcpy(data, &record.id,sizeof(record.id));
    data += sizeof(int);

    memset(data,0,15*sizeof(char));
    memcpy(data , record.name , strlen(record.name)+1);
    data += 15*sizeof(char);

    memset(data,0,20*sizeof(char));
    memcpy(data , record.surname , strlen(record.surname)+1);
    data += 20*sizeof(char);

    memset(data,0,20*sizeof(char));
    memcpy(data , record.city , strlen(record.city)+1);  

    return SR_OK;
}


/*
 * Η συνάρτηση SR_CreateFile χρησιμοποιείται για τη δημιουργία και
 * κατάλληλη αρχικοποίηση ενός άδειου αρχείου ταξινόμησης με όνομα fileName.
 * Σε περίπτωση που εκτελεστεί επιτυχώς, επιστρέφεται SR_OK, ενώ σε
 * διαφορετική περίπτωση κάποιος κωδικός λάθους.
 */

SR_ErrorCode SR_CreateFile(const char *fileName) {
  // Your code goes here
  CALL_OR_DIE(BF_CreateFile(fileName));
  int fileDesc;
  CALL_OR_DIE(BF_OpenFile(fileName,&fileDesc));

  BF_Block* block = NULL;
  BF_Block_Init(&block);
  CALL_OR_DIE(BF_AllocateBlock(fileDesc,block));
  char* data = BF_Block_GetData(block);
  memset(data,(int)0,BF_BLOCK_SIZE);

  char* heapid = "sr_file";
  memcpy(data,heapid,strlen("sr_file")+1);
  BF_Block_SetDirty(block);
  CALL_OR_DIE(BF_UnpinBlock(block));
  BF_Block_Destroy(&block);
  BF_CloseFile(fileDesc);
  return SR_OK;
}

/*
 * Η συνάρτηση SR_OpenFile ανοίγει το αρχείο με όνομα filename και διαβάζει
 * από το πρώτο μπλοκ την πληροφορία που αφορά το αρχείο ταξινόμησης. Επιστρέφει
 * στην μεταβλητή fileDesc τον αναγνωριστικό αριθμό ανοίγματος αρχείου, όπως
 * αυτός επιστράφηκε από το επίπεδο διαχείρισης μπλοκ. Σε περίπτωση που
 * εκτελεστεί επιτυχώς, επιστρέφεται SR_OK, ενώ σε διαφορετική περίπτωση
 * κάποιος κωδικός λάθους. Αν το αρχείο που ανοίχτηκε δεν πρόκειται για αρχείο
 * ταξινόμησης, τότε αυτό θεωρείται επίσης περίπτωση σφάλματος.
 */
SR_ErrorCode SR_OpenFile(const char *fileName, int *fileDesc) {
  // Your code goes here
  CALL_OR_DIE(BF_OpenFile(fileName,fileDesc));
  BF_Block* block = NULL;
  BF_Block_Init(&block);
  CALL_OR_DIE(BF_GetBlock(*fileDesc,0,block));
  char* data = BF_Block_GetData(block);
  if(strcmp(data,"sr_file")==0){
    printf("File:%s openned succesfully with fd=%d\n",fileName,*fileDesc);
  }
  else{
    fprintf(stderr, "Error openning File %s with fd=%d !! File is not a sr_file(found %s)\n",fileName,*fileDesc,data);
    CALL_OR_DIE(BF_UnpinBlock(block));
    BF_Block_Destroy(&block);

    return SR_ERROR;
  }

  BF_Block_SetDirty(block);
  CALL_OR_DIE(BF_UnpinBlock(block));
  BF_Block_Destroy(&block);
  return SR_OK;
}

/*
 * Η συνάρτηση SR_CloseFile κλείνει το αρχείο που προσδιορίζεται από τον
 * αναγνωριστικό αριθμό ανοίγματος fileDesc. Σε περίπτωση που εκτελεστεί
 * επιτυχώς, επιστρέφεται SR_OK, ενώ σε διαφορετική περίπτωση κάποιος
 * κωδικός λάθους.
 */
SR_ErrorCode SR_CloseFile(int fileDesc) {
  // Your code goes here
  BF_CloseFile(fileDesc);
  return SR_OK;
}


/*
 * Η συνάρτηση SR_InsertEntry χρησιμοποιείται για την εισαγωγή μίας
 * εγγραφής στο αρχείο ταξινόμησης. Ο αναγνωριστικός αριθμός ανοίγματος του
 * αρχείου δίνεται με την fileDesc ενώ η εγγραφή προς εισαγωγή προσδιορίζεται
 * από τη δομή record. Η εγγραφή προστίθεται στο τέλος του αρχείου, μετά την
 * τρέχουσα τελευταία εγγραφή. Σε περίπτωση που εκτελεστεί επιτυχώς,
 * επιστρέφεται SR_OK, ενώ σε διαφορετική περίπτωση κάποιος κωδικός λάθους.
 */
SR_ErrorCode SR_InsertEntry(int fileDesc,	Record record) {
  // Your code goes here
  BF_Block* block = NULL;
  char* data = NULL;
  int block_num = 0;

  //get number of blocks in cache

  CALL_OR_DIE(BF_GetBlockCounter(fileDesc,&block_num));
  if(block_num == 1 ){
    //create first block
      
    BF_Block_Init(&block);
    CALL_OR_DIE(BF_AllocateBlock(fileDesc,block));
    
    data = BF_Block_GetData(block);
    memset(data,(int)0,BF_BLOCK_SIZE);
    int one = 1;
    //number of records in the block is one
    memcpy(data,&one,sizeof(int));
    data += sizeof(int);

    SR_InsertData(data,record);
  }
  else{


    block_num--;
    BF_Block_Init(&block);
    CALL_OR_DIE(BF_GetBlock(fileDesc,block_num,block));
  
    
    data = BF_Block_GetData(block);
    
    int record_num = 0;
    memcpy(&record_num,data,sizeof(int));

    
    //if (existing blocks can not store another record )
    if(((record_num+1)*sizeof(Record) + sizeof(char)) >= BF_BLOCK_SIZE){
      
      //Unpin previous block
      CALL_OR_DIE(BF_UnpinBlock(block));
      BF_Block_Destroy(&block);

      //Allocate a new block

      BF_Block_Init(&block);
      CALL_OR_DIE(BF_AllocateBlock(fileDesc,block));
      data = BF_Block_GetData(block);
      memset(data,0,BF_BLOCK_SIZE);
      int one = 1;
  
      memcpy(data,&one,sizeof(int));//number of records in block is now one
      data += sizeof(int);
      SR_InsertData(data,record);
    
      block_num--;
     
    } 
    else{//There is room to store a record within the existing blocks
      int new_rec = ++record_num;//increase block's records by one
      memcpy(data,&new_rec,sizeof(int));
      data += sizeof(int) + (new_rec-1)*sizeof(Record);
      SR_InsertData(data,record);
      
    } 
  }
  //Block changed in cache so it needs to be changed in disk as well
  BF_Block_SetDirty(block);
  CALL_OR_DIE(BF_UnpinBlock(block));
  BF_Block_Destroy(&block);
  return SR_OK;
}




SR_ErrorCode getNextBlocks(int step_num,int bufferSize,int fileDesc,int tempDesc){
  
  int start_block = step_num*bufferSize;
  BF_Block* source_block;
  BF_Block_Init(&source_block);

  BF_Block* dest_block;
  BF_Block_Init(&dest_block);
  int i;
  char* source;
  char* dest;
  int block_num;
  CALL_OR_DIE(BF_GetBlockCounter(fileDesc,&block_num));
  printf("block_num = %d\n",block_num );
  block_num-=1;

  int buffer_pos = 0;
  for( i = start_block ; i < start_block+bufferSize ;i++){
    if(i > block_num)break;
    printf("i = %d\n",i );
    CALL_OR_DIE(BF_GetBlock(fileDesc,i,source_block));
    source = BF_Block_GetData(source_block);

    CALL_OR_DIE(BF_GetBlock(tempDesc,buffer_pos,dest_block));
    dest = BF_Block_GetData(dest_block);
    memcpy(dest,source,BF_BLOCK_SIZE);
    buffer_pos+=1;

    BF_Block_SetDirty(dest_block);
    CALL_OR_DIE(BF_UnpinBlock(source_block));
    CALL_OR_DIE(BF_UnpinBlock(dest_block));
  }


  BF_Block_Destroy(&source_block);
  BF_Block_Destroy(&dest_block);
}



SR_ErrorCode printBuffer(int bufferSize,int tempDesc){
  printf("----- Print Buffer -----\n");
  int i;
  int record_num;
  char* data;
  BF_Block* block;
  BF_Block_Init(&block);
  for( i = 0 ; i < bufferSize ; i++){

    CALL_OR_DIE(BF_GetBlock(tempDesc,i,block));
    data = BF_Block_GetData(block);
    record_num = 0;
    memcpy(&record_num,data,sizeof(int));//read block's number of records
    int j = 0;
    char* charbuf = data + sizeof(int); 
    for(j = 0 ; j < record_num; j++){ //read each record in block
      Record record;
      memset(&record,0,sizeof(Record));
      memcpy(&record,charbuf,sizeof(Record));
      printf("%d,\"%s\",\"%s\",\"%s\"\n",
        record.id, record.name, record.surname, record.city);
      charbuf += sizeof(Record);
   
    }

    printf("\n\n");
  }
}






void QuickSort(int bufferSize,int tempDesc,int start,int end) {
  int pIndex; 
  if(start<end) { 
    pIndex=Partition(bufferSize,tempDesc,start,end);//first iteration:start=  0 , end = buffersize*record
    QuickSort(bufferSize,tempDesc,start,pIndex-1);
    QuickSort(bufferSize,tempDesc,pIndex+1,end); 
  } 
} 



char* getRecord(int fileDesc,int record_num){
  int target_block;
  int bl_rec_capacity;
  char* data;
  int offset ;
  BF_Block* block = NULL;
  BF_Block_Init(&block);


  bl_rec_capacity = BF_BLOCK_SIZE/sizeof(Record);


  target_block = record_num/bl_rec_capacity;
  offset = record_num%bl_rec_capacity;

  printf("record_num = %d\n",record_num );
  printf("bl_rec_capacity = %d\n",bl_rec_capacity );
  printf("target_block %d\n",target_block );


  CALL_OR_DIE(BF_GetBlock(fileDesc,target_block,block));
  data = BF_Block_GetData(block);
  data+=sizeof(int);

  printf("offset = %d\n\n",offset );
  BF_UnpinBlock(block);
  BF_Block_Destroy(&block);
  
  return data+(offset*sizeof(Record));
}


int Partition(int bufferSize,int tempDesc,int start,int end) {
  printf("partition : %d -> %d\n",start,end );
  int i,pIndex;
 
  char temp[sizeof(Record)];
  memset(temp,0,sizeof(Record));

  char temp2[sizeof(Record)];
  memset(temp,0,sizeof(Record));

  char* data;
  char *pivot;
  char *record_data;
  char* endData;
  char* pIndexData;

  pivot = getRecord(tempDesc,end);
  
  Record precord;

  memset(&precord,0,sizeof(Record));
  memcpy(&precord,pivot,sizeof(Record));
  printf("PIVOT (%d->%d) %d,\"%s\",\"%s\",\"%s\"\n\n",
  start,end,precord.id, precord.name, precord.surname, precord.city);

  pIndex=start;
  for(i=start;i<end;i++) {
  
    record_data = getRecord(tempDesc,i); 
    Record record;
    memset(&record,0,sizeof(Record));
    memcpy(&record,record_data,sizeof(Record));
  
    //printf("%d,\"%s\",\"%s\",\"%s\"\n",
      //record.id, record.name, record.surname, record.city);
  

   if(strcmp(record.name,precord.name)<0) {
      memcpy(temp,record_data,sizeof(Record));

      pIndexData = getRecord(tempDesc,pIndex);
      memcpy(record_data,pIndexData,sizeof(Record));
      
      memcpy(pIndexData,temp,sizeof(Record));
    
      pIndex=pIndex+1; 
   }
  }


  memcpy(temp2,pIndexData,sizeof(Record));

  endData = getRecord(tempDesc,end);
  
  memcpy(pIndexData,endData,sizeof(Record));
  memcpy(endData,temp2,sizeof(Record));
  return pIndex; 
}


/*
 * Η συνάρτηση αυτή ταξινομεί ένα BF αρχείο με όνομα input_​fileName ως προς το
 * πεδίο που προσδιορίζεται από το fieldNo χρησιμοποιώντας bufferSize block
 * μνήμης. Το​ fieldNo είναι ο αύξων αριθμός του πεδίου, δηλαδή αν το
 * fieldNo = 0, τότε το πεδίο ως προς το οποίο θέλουμε να κάνουμε ταξινόμηση
 * είναι το id, αν fieldNo = 1, τότε το πεδίο ως προς το οποίο θέλουμε να
 * ταξινομήσουμε είναι το name, κτλ. Η συνάρτηση επιστρέφει SR_OK σε περίπτωση
 * επιτυχίας, ενώ σε διαφορετική περίπτωση κάποιος κωδικός λάθους.
 * Πιο συγκεκριμένα, η λειτουργικότητα που πρέπει να υλοποιηθεί είναι η ακόλουθη:
 *
 *    * Να διαβάζονται οι εγγραφές από το αρχείο input_filename και να
 *      εισάγονται στο νέο αρχείο ταξινομημένες ως προς το πεδίο με αρίθμηση
 *      fieldNo. Η ταξινόμηση θα γίνει με βάση τον αλγόριθμο εξωτερικής
 *      ταξινόμησης (external sort).
 *
 *    * Ο αλγόριθμός θα πρέπει να εκμεταλλεύεται όλα τα block μνήμης που σας
 *      δίνονται από την μεταβλητή bufferSize και μόνον αυτά. Αν αυτά τα block
 *      είναι περισσότερα από BF_BUFFER_SIZE ή μικρότερα από 3 τότε θα
 *      επιστρέφεται κωδικός λάθους.
 */


SR_ErrorCode SR_SortedFile(
  const char* input_filename,
  const char* output_filename,
  int fieldNo,
  int bufferSize
) {
  // Your code goes here



  if((bufferSize > BF_BUFFER_SIZE) || (bufferSize < 3)){
    fprintf(stderr, "invalid input buffer size :%d\n",bufferSize );
    return SR_ERROR;
  }

  int i,j,k,m,p,b;
  errno = 0;
  access("temp",F_OK);

  int file_exists=0;
  if(errno == ENOENT){
    CALL_OR_DIE(BF_CreateFile("temp"));
  }
  else if(errno!=0){
    fprintf(stderr, "Error accessing \"temp\" file\n");
  }
  else file_exists=1;


  int tempDesc;
  CALL_OR_DIE(BF_OpenFile("temp",&tempDesc));


  /*Allocate buffer's blocks in temp file*/
  
  
  int buffer_length;
  if(file_exists == 0){
    for(i = 0 ; i < bufferSize ; i++){
      BF_Block* block = NULL;
      BF_Block_Init(&block);
      CALL_OR_DIE(BF_AllocateBlock(tempDesc,block));
  
      BF_UnpinBlock(block);
      BF_Block_Destroy(&block);
    } 
    CALL_OR_DIE(BF_GetBlockCounter(tempDesc,&buffer_length));
    if(buffer_length!=bufferSize){
      fprintf(stderr, "invalid buffer_length = %d\n",buffer_length);
    }

  }



  
  


  /*Fasi 1 : Quicksort! */


  int inputDesc;
  int outputDesc;

  CALL_OR_DIE(SR_OpenFile(input_filename,&inputDesc));

  CALL_OR_DIE(SR_OpenFile(input_filename,&outputDesc));
  BF_Block* block;
  BF_Block_Init(&block);



  int block_num;


  CALL_OR_DIE(BF_GetBlockCounter(inputDesc,&block_num));
  block_num-=1;

  i = 1;
  j = block_num;//o arithmos twn sunolikwn block me tis eggrafes
  k = bufferSize;
  m = j/k; //arithmos upoomadwn pou tha sximatistoun meta to 1o vima
  if(j%k > 0)m++;

  //quicksort


  int end;
  end = bufferSize*(BF_BLOCK_SIZE/sizeof(Record));
  printf("buffersize = %d\n",bufferSize);
   printBuffer(bufferSize,tempDesc);
  while(i <= m){
    getNextBlocks(i,bufferSize,inputDesc,tempDesc);//metaferei k blocks apo to offset k*i ston buffer 
    //printBuffer(bufferSize,tempDesc);
    printf("%d \n",i );
    QuickSort(bufferSize,tempDesc,0,(bufferSize*(BF_BLOCK_SIZE/sizeof(Record))) -1);
    break;
    i+=1;
  }
   printBuffer(bufferSize,tempDesc);

/*

  //Fasi sugxwneusis
  i = 1;

  double steps = [logk-1(m)]; 
  int p = [logk-1(m)];
  if(steps - p > 0)p++;


  j = m;
  while(i < = p){      
    n = 1;
    q = j/(k-1);                     
                                    //  
    if(j%(k-1) > 0)q++;
    while( n <= q){
       getNextBlocks(i,k-1,q,temp,buffer);
       Merge(buffer,output_filename);
       n+=1;
    }
    j = q;
    i+=1;
  }

  */


  BF_CloseFile(tempDesc);
  BF_CloseFile(outputDesc);
  return SR_OK;
}


/*
 * Η συνάρτηση SR_PrintAllEntries χρησιμοποιείται για την εκτύπωση όλων των
 * εγγραφών που υπάρχουν στο αρχείο ταξινόμησης. Το fileDesc είναι ο αναγνωριστικός
 * αριθμός ανοίγματος του αρχείου, όπως αυτός έχει επιστραφεί από το επίπεδο
 * διαχείρισης μπλοκ. Σε περίπτωση που εκτελεστεί επιτυχώς, επιστρέφεται SR_OK,
 * ενώ σε διαφορετική περίπτωση κάποιος κωδικός λάθους.
 */
SR_ErrorCode SR_PrintAllEntries(int fileDesc) {
  // Your code goes here
 BF_Block* block;
  BF_Block_Init(&block);
  int i ;
  int block_num = 0;
  CALL_OR_DIE(BF_GetBlockCounter(fileDesc,&block_num));


  CALL_OR_DIE(BF_GetBlock(fileDesc,0,block));
  char*data = BF_Block_GetData(block);
  char heapid[10];
  memset(heapid,0,10);
  memcpy(&heapid,data,strlen("heap_file")+1);//read block's number of records
  if(strcmp(heapid,"heap_file")== 0 ){
    printf("Heap File Accessed\n");
  }
  else{
    printf("Error:Not A Heap File\n");
  }


  for(i = 1 ; i < block_num; i++){
    CALL_OR_DIE(BF_GetBlock(fileDesc,i,block));
    char*data = BF_Block_GetData(block);
    int record_num = 0;
    memcpy(&record_num,data,sizeof(int));//read block's number of records
    int j = 0;
    char* buffer = data + sizeof(int); 
    for(j = 0 ; j < record_num; j++){ //read each record in block
      Record record;
      memset(&record,0,sizeof(Record));
      memcpy(&record,buffer,sizeof(Record));
      printf("%d,\"%s\",\"%s\",\"%s\"\n",
        record.id, record.name, record.surname, record.city);
      buffer += sizeof(Record);
    }
    CALL_OR_DIE(BF_UnpinBlock(block));
  }

  BF_Block_Destroy(&block);
  return SR_OK;
}
