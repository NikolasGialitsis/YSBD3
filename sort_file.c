#include "sort_file.h"
#include <stdio.h>
#include <sys/types.h>
#include <stdlib.h>
#include <string.h>
#include "bf.h"
#include <unistd.h>
#include <wait.h>
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
  printf("Initialize SR: Remove all sorted files from previous runs\n");
  pid_t pid;
  char SortedFiles[3][20];
  memset(SortedFiles,0,3*20*sizeof(char));
  memcpy(SortedFiles[0],"sorted_name.db\0",strlen("sorted_name.db")+1);
  memcpy(SortedFiles[1],"sorted_surname.db\0",strlen("sorted_surname.db")+1);
  memcpy(SortedFiles[2],"sorted_id.db\0",strlen("sorted_id.db")+1);

  int i = 0;
  int status;
  for(i = 0 ; i < 3 ; i++){
    printf("\taccess %s\n",SortedFiles[i] );
    fflush(stdout);
    errno = 0;
    access(SortedFiles[i],F_OK); 
    if(errno == ENOENT)continue;
    pid = fork();
  
    if(pid < 0){
      fprintf(stderr, "Fork error\n" );
      exit(-1);
    }
    else if(pid == 0){
      printf("\t\t\tI am child process pid = %d , parent = %d\n",getpid(),getppid() );
      fflush(stdout);
      printf("\t\t\tremoving %s\n",SortedFiles[i]);
      fflush(stdout);
      execlp("rm","rm",SortedFiles[i],(void*)NULL);
      
      printf("Unexpected return from exec: error rm\n");
      fflush(stdout);
      exit(-1);
    }
    else{
      printf("\t\tI am parent process pid = %d \n",getpid());
      fflush(stdout);
      while(wait(NULL) < 0);
    }

  }

  printf("\n\n");
  fflush(stdout);
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


  char* heapid = "sr_file";

  memset(data,0,BF_BLOCK_SIZE);
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
    printf("\nFile:%s openned succesfully with fd=%d\n",fileName,*fileDesc);
  }
  else{
    fprintf(stderr, "\nError openning File %s with fd=%d !! File is not a sr_file(found %s)\n",fileName,*fileDesc,data);
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
    if(((record_num+1)*sizeof(Record) + sizeof(int)) >= BF_BLOCK_SIZE){
      
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


SR_ErrorCode writeBuffer(int bufferSize,int tempDesc,int outputDesc){
 
  BF_Block* block;
  BF_Block_Init(&block);
  off_t offset;
  char* data;
  int record_num =0;
  int i = 0;
  int ret = -1;

  for( i = 0 ; i < bufferSize ; i++){

    CALL_OR_DIE(BF_GetBlock(tempDesc,i,block));

    data = BF_Block_GetData(block);
    CALL_OR_DIE(BF_UnpinBlock(block));

    record_num = 0;
    memcpy(&record_num,data,sizeof(int));//read block's number of records
    if(record_num == 0)break;
    int j = 0;

    char* charbuf = data + sizeof(int);
    for(j = 0 ; j < record_num; j++){ //read each record in block 
      Record record;
      memset(&record,0,sizeof(Record));
      memcpy(&record,charbuf,sizeof(Record));

      if(strcmp(record.name,"") != 0){
        printf("%d,\"%s\",\"%s\",\"%s\"\n",
        record.id, record.name, record.surname, record.city);
      }
      SR_InsertEntry(outputDesc,record);
      charbuf += sizeof(Record);
   
    }
    //printf("\n");
  }
  BF_Block_Destroy(&block);
  return SR_OK;
}

SR_ErrorCode getNextBlocks(int step_num,int bufferSize,int fileDesc,int tempDesc){

  int start_block;
  if(step_num != 1)
    start_block = ((step_num-1)*bufferSize)+1;
  else 
    start_block = 1;
  BF_Block* source_block;
  BF_Block_Init(&source_block);

  BF_Block* dest_block;
  BF_Block_Init(&dest_block);
  int i;
  char* source;
  char* dest;
  int block_num;
  CALL_OR_DIE(BF_GetBlockCounter(fileDesc,&block_num));
  block_num-=1;

  int record_num=0;
  int buffer_pos = 0;
  for( i = start_block ; i < start_block+bufferSize ;i++){
    if(i > block_num){
      int k;
      //printf("%d > %d\n",i,block_num );
      for(k = buffer_pos ; k < bufferSize ;k++){
        CALL_OR_DIE(BF_GetBlock(tempDesc,k,dest_block));
        dest = BF_Block_GetData(dest_block);
        memset(dest,0,BF_BLOCK_SIZE);
        //printf("set Buffer[%d] -->0 \n",k);
        BF_Block_SetDirty(dest_block);
        CALL_OR_DIE(BF_UnpinBlock(dest_block));
      }
      break;
    }
    CALL_OR_DIE(BF_GetBlock(fileDesc,i,source_block));

    source = BF_Block_GetData(source_block);
    record_num = 0;
    memcpy(&record_num,source,sizeof(int));
    CALL_OR_DIE(BF_GetBlock(tempDesc,buffer_pos,dest_block));
    dest = BF_Block_GetData(dest_block);
    memset(dest,0,BF_BLOCK_SIZE);
    if(record_num != 0){
      memset(dest,0,BF_BLOCK_SIZE);
      memcpy(dest,source,BF_BLOCK_SIZE);
    }
    buffer_pos+=1;

    BF_Block_SetDirty(dest_block);
    CALL_OR_DIE(BF_UnpinBlock(source_block));
    CALL_OR_DIE(BF_UnpinBlock(dest_block));
  }


  BF_Block_Destroy(&source_block);
  BF_Block_Destroy(&dest_block);
  return SR_OK;
}



SR_ErrorCode printBuffer(int bufferSize,int tempDesc){
  printf("\n----- Print Buffer -----\n");
  int i;
  int record_num;
  char* data;
  BF_Block* block;
  BF_Block_Init(&block);
  for( i = 0 ; i < bufferSize ; i++){
    printf("\tBuffer[%d]:\n",i );
    CALL_OR_DIE(BF_GetBlock(tempDesc,i,block));

    data = BF_Block_GetData(block);
    CALL_OR_DIE(BF_UnpinBlock(block));

    record_num = 0;
    memcpy(&record_num,data,sizeof(int));//read block's number of records
    int j = 0;
    char* charbuf = data + sizeof(int);
    for(j = 0 ; j < record_num; j++){ //read each record in block
      Record record;
      memset(&record,0,sizeof(Record));
      memcpy(&record,charbuf,sizeof(Record));
     
      printf("\t\t%d,\"%s\",\"%s\",\"%s\"\n",
      record.id, record.name, record.surname, record.city);
      
      charbuf += sizeof(Record);
   
    }
    printf("\n");
  }
  BF_Block_Destroy(&block);
}






void QuickSort(int bufferSize,int tempDesc,int start,int end,int fieldNo) {
  int pIndex; 
  if(start<end) { 
    pIndex=Partition(bufferSize,tempDesc,start,end,fieldNo);//first iteration:start=  0 , end = buffersize*record
    QuickSort(bufferSize,tempDesc,start,pIndex-1,fieldNo);
    QuickSort(bufferSize,tempDesc,pIndex+1,end,fieldNo); 
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

  CALL_OR_DIE(BF_GetBlock(fileDesc,target_block,block));
  data = BF_Block_GetData(block);
  data+=sizeof(int);

  BF_UnpinBlock(block);
  BF_Block_Destroy(&block);
  
  return data+(offset*sizeof(Record));
}

/*Function compare:
  //returns 0 if currec[field] == pivot[field] 
  //returns < 0 if currec[field] < pivot[field]
  //returns > 0 if currec[field] > pivot[field]
*/
int compare(Record* currec ,Record* pivot,int fieldNo){
  
  if (fieldNo == 0){
    if((currec->id) == (pivot->id) ) return 0;
    else if ((currec->id)>(pivot->id)) return 1;
    else return -1;
  }
  else if 
    (fieldNo == 1 )return strcmp(currec->name,pivot->name);
  else if 
    (fieldNo == 2 )return strcmp(currec->surname,pivot->surname);
  else if 
    (fieldNo == 3 )return strcmp(currec->city,pivot->city);
  else{
    fprintf(stderr, "Compare:Error invalid field %d\n",fieldNo );
    exit(-fieldNo);
  }



}

int Partition(int bufferSize,int tempDesc,int start,int end,int fieldNo) {
  //printf("partition : %d -> %d\n",start,end );
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
  pIndex=start;
  for(i=start;i<end;i++) {
  
    record_data = getRecord(tempDesc,i); 
    Record record;
    memset(&record,0,sizeof(Record));
    memcpy(&record,record_data,sizeof(Record));

if (compare(&record,&precord,fieldNo) < 0 ){
      if(pIndex != i){
          memset(temp,0,sizeof(Record));
          memcpy(temp,record_data,sizeof(Record));

          pIndexData = getRecord(tempDesc,pIndex);

          memset(record_data,0,sizeof(Record));
          memcpy(record_data,pIndexData,sizeof(Record));
          
          memset(pIndexData,0,sizeof(Record));
          memcpy(pIndexData,temp,sizeof(Record));
        
      }

      pIndex=pIndex+1; 
   }
  }

  pIndexData = getRecord(tempDesc,pIndex);


  memset(temp2,0,sizeof(Record));
  memcpy(temp2,pIndexData,sizeof(Record));

  endData = getRecord(tempDesc,end);
  
  memset(pIndexData,0,sizeof(Record));
  memcpy(pIndexData,endData,sizeof(Record));

  memset(endData,0,sizeof(Record));
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
  printf("\n");

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
  else {
    pid_t pid = fork();
    if(pid < 0){
      fprintf(stderr, "Fork error\n" );
      exit(-1);
    }
    else if(pid == 0){
      execlp("rm","rm","temp",(void*)NULL);
      printf("Unexpected return from exec: error rm\n");
      exit(-1);
    }
    else{
      while(wait(NULL) < 0);
    }

    CALL_OR_DIE(BF_CreateFile("temp"));
}

  
  int tempDesc;
  CALL_OR_DIE(BF_OpenFile("temp",&tempDesc));


  /*Allocate buffer's blocks in temp file*/
  
  
  int buffer_length;

  for(i = 0 ; i < bufferSize ; i++){
    BF_Block* block = NULL;
    BF_Block_Init(&block);
    CALL_OR_DIE(BF_AllocateBlock(tempDesc,block));
    char* data = BF_Block_GetData(block);
    memset(data,0,BF_BLOCK_SIZE);

    BF_UnpinBlock(block);
    BF_Block_Destroy(&block);
  } 
  CALL_OR_DIE(BF_GetBlockCounter(tempDesc,&buffer_length));
  if(buffer_length!=bufferSize){
    fprintf(stderr, "invalid buffer_length = %d\n",buffer_length);
  }



  /*Fasi 1 : Quicksort! */


  int inputDesc;
  int outputDesc;

  errno = 0;
  access(input_filename,F_OK);
  if(errno != 0){
    fprintf(stderr, "\nError accessing \"%s\" file\n",input_filename);
  }
  
  errno = 0;
  access(output_filename,F_OK);
  if(errno == ENOENT){
    CALL_OR_DIE(SR_CreateFile(output_filename));

  }
  else if(errno!=0){

    fprintf(stderr, "Error accessing \"%s\" file (errno = %d)\n",output_filename,errno);
  }

  CALL_OR_DIE(SR_OpenFile(input_filename,&inputDesc));


  
  CALL_OR_DIE(SR_OpenFile(output_filename,&outputDesc));
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

  int last_rec_id = 0;
  int last_bl_rec=0;//# of records that the last block in buffer contains

  char* data;

  while(i <= m){
    getNextBlocks(i,bufferSize,inputDesc,tempDesc);//metaferei k blocks apo to offset k*i ston buffer 
    int k=0;  
    for(k = 0 ; k < bufferSize ;k++){
      int numofrec=0;

      CALL_OR_DIE(BF_GetBlock(tempDesc,k,block));
      data = BF_Block_GetData(block);
      CALL_OR_DIE(BF_UnpinBlock(block));
      memcpy(&numofrec,data,sizeof(int));//reads block's number of records
      if(numofrec == 0){      
        break;
      }
      last_bl_rec = numofrec;
  
    }

    last_rec_id = (k-1)*(BF_BLOCK_SIZE/sizeof(Record))//n-1 blocks are full
              + last_bl_rec//last records(last block may not be full)
              - 1; //because indexing starts from 0

    QuickSort(bufferSize,tempDesc,0,last_rec_id,fieldNo);
    writeBuffer(bufferSize,tempDesc,outputDesc);
    //printBuffer(bufferSize,tempDesc);
  
    i+=1;

  }

//CALL_OR_DIE(SR_PrintAllEntries(outputDesc));

  //printf("%d = %d / %d\n",m,j,k);
 // printBuffer(bufferSize,tempDesc);

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


  BF_Block_Destroy(&block);
  SR_CloseFile(tempDesc);
  SR_CloseFile(outputDesc);
  SR_CloseFile(inputDesc);
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
  memcpy(&heapid,data,strlen("sr_file")+1);//read block's number of records
  if(strcmp(heapid,"sr_file")== 0 ){
   printf("Heap File Accessed\n");
  }
  else{
    printf("Error:Not A Sort File\n");
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
