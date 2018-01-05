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


SR_ErrorCode Merge(
  int step_num,int group_num,int bufferSize,int tempDesc,int tempOut,int outputDesc,
  int fieldNo,int output_has_content,int* record_num)
{
  //printf("Merge!\n");
  BF_Block* block;
  BF_Block_Init(&block);
  int i = 0;
  int j = 0;
  int block_num = 0;
  CALL_OR_DIE(BF_GetBlockCounter(tempOut,&block_num));

  //printf("----m -e -r -g -e ---\n\n");
  char** data = calloc(bufferSize-1,sizeof(char*));
    
  int NumRecs[bufferSize-1];
  int Index[bufferSize-1];
  int GroupOffset[bufferSize-1];
  int GroupID[bufferSize-1];

  memset(NumRecs,0,(bufferSize-1)*sizeof(int));
  memset(Index,0,(bufferSize-1)*sizeof(int));
  memset(GroupOffset,0,(bufferSize-1)*sizeof(int));
  memset(GroupID,0,(bufferSize-1)*sizeof(int));

  //printf("bufferSize = %d\n",bufferSize );
  int empty_blocks = 0;
  for( i = 0 ; i < bufferSize -1 ; i++){
    CALL_OR_DIE(BF_GetBlock(tempDesc,i,block));
    pinned++;
    data[i] = BF_Block_GetData(block);
    memcpy(NumRecs+i,data[i],sizeof(int));
    //printf("NumRecs[%d] = %d\n",i,NumRecs[i] );
    data[i] += sizeof(int);
    Index[i] = 0; 

    if(NumRecs[i] == 0)empty_blocks++;
    else {
      //printf("NumRecs[%d] = %d\n",i,NumRecs[i] );
      GroupID[i] = i;
    }

    GroupOffset[i] = 0; 
    CALL_OR_DIE(BF_UnpinBlock(block));
    pinned--;
  }

  int k = 0;
  int count = bufferSize-1-empty_blocks;
  for(j = count ; j < bufferSize-1; j++){
    GroupID[j] = k;
    (GroupOffset[k])++; 
    data[j] = MoveGroupIndex(step_num,GroupID[j],group_num,block_num-1,GroupOffset[GroupID[j]],tempOut);
    if(data[j]!= NULL){
      memcpy(NumRecs+j,data[j],sizeof(int));
      data[j]+=sizeof(int); 
    }
    else{
      fprintf(stderr, "unexpected value in Merge:movegroup\n" );
      exit(-1);
    }
    //printf("\t%d:GroupOffset[0] = %d\n",j,GroupOffset[GroupID[0]] );
    k++; 
    k = k % (bufferSize-1-empty_blocks);
  }


  for(i = 0 ; i < bufferSize - 1; i++){
    printf("GroupID[%d] = %d\n",i,GroupID[i] );
    printf("Index[%d] = %d\n",i,Index[i] );
    printf("\n");

  }
  fflush(stdout);
  CALL_OR_DIE(BF_GetBlock(tempDesc,bufferSize-1,block));
  pinned++;

  char * output = BF_Block_GetData(block);
  memset(output,0,BF_BLOCK_SIZE);

  int output_capacity = BF_BLOCK_SIZE - sizeof(int);


  //BF_Block_SetDirty(block);
  CALL_OR_DIE(BF_UnpinBlock(block));

  int min_index = 0;
  int flag = 0;
  Record min;
 
  flag = 1;
  int output_recs = 0;
  while(flag){
    //printf("\n-------------------- pinned = %d ------------------\n\n",pinned );
    flag = 0;
    min_index = 0;
  
    memset(&min,0,sizeof(Record));
    min.id = -1;
    //printf("\n---- Enter Loop: ----\n");
    for(k = 0 ; k < bufferSize-1 ; k++){
     // printf("Index[%d] = %d\n",k,Index[k] );
      if(Index[k] < NumRecs[k]){
        //printf("Index[%d]=%d < NumRecs[%d]=%d \n",k,Index[k],k,NumRecs[k] );
        if(!flag){
          memset(&min,0,sizeof(Record));
          memcpy(&min,data[k],sizeof(Record));
          //printf("\t case 1:first min %d\n",min.id);
          min_index = k;
          flag = 1;
        }
        else{
          
          Record record;


          memset(&record,0,sizeof(Record));
          memcpy(&record,data[k],sizeof(Record));
          //printf("\t case 2:compare %d < %d\n",record.id,min.id);
          if(compare(&record,&min,fieldNo) < 0){
            //printf("\t\t new min %d\n",record.id);
            //printf("compare %d,%s < %d,%s\n",record.id,record.name,min.id,min.name );
            min_index = k;
            memset(&min,0,sizeof(Record));
            memcpy(&min,data[k],sizeof(Record));
          
          }
        }
      }
      else{
        if(GroupOffset[GroupID[k]] < (group_num-1)){
          
          data[k] = MoveGroupIndex(step_num,GroupID[k],group_num,block_num-1,GroupOffset[GroupID[k]]+1,tempOut);
          if(data[k]!= NULL){
            Record record;
            memcpy(NumRecs+k,data[k],sizeof(int));
            data[k]+=sizeof(int);
            memset(&record,0,sizeof(Record));
            memcpy(&record,data[k],sizeof(Record));
         //  printf("case 3: move index %d\n",record.id);
            //exit(0);

            Index[k] = 0; 
            GroupOffset[GroupID[k]]++;
            if(!flag){
              memset(&min,0,sizeof(Record));
              memcpy(&min,&record,sizeof(Record));
           //   printf("\tfirst min %d\n",min.id);
              min_index = k;
              flag = 1;
            } 
            else{
              Record record;
              memset(&record,0,sizeof(Record));
              memcpy(&record,data[k],sizeof(Record));
             // printf("\t compare %d < %d\n",record.id,min.id);
              if(compare(&record,&min,fieldNo) < 0){
               // printf("\t\t new min %d\n",min.id);
              //printf("compare %d,%s < %d,%s\n",record.id,record.name,min.id,min.name );
                min_index = k;
                memset(&min,0,sizeof(Record));
                memcpy(&min,data[k],sizeof(Record));
              }
            }
        
         }
       }


        else{
          if(min_index == k){
           // printf(">> reset <<%d\n",k );
            min_index = -1;
            memset(&min,0,sizeof(Record));
            min.id = -1;
          }
        }
      }
    }

    if(flag){
      if(output_capacity < sizeof(Record)){
       // printf("\nOutput to be flushed!!\n");
     
        
        /*output = BF_Block_GetData(block);
        for(j = 0 ; j <  output_recs; j++){ //read each record in block 
          Record record;
          memset(&record,0,sizeof(Record));
          memcpy(&record,output,sizeof(Record));
          printf("\t\t%d,%s,%s,%s\n",record.id,record.name,record.surname,record.city);

          if( output_has_content == 0) 
            SR_InsertEntry(outputDesc,record);
          else
            InsertOutput(outputDesc,record_num,&record);   
          if(j < output_recs-1)output+=sizeof(Record);             
        }
        output = BF_Block_GetData(block);
        memset(output,0,BF_BLOCK_SIZE);
        */
        output_capacity = BF_BLOCK_SIZE-sizeof(int);

        output_recs = 0;
      }
      //printf("add %d to output\n",min.id );
      
      SR_InsertEntry(outputDesc,min);

      printf("\t\t%d,%s,%s,%s\n",min.id,min.name,min.surname,min.city);


      /*memcpy(output,&min,sizeof(Record));
      output+=sizeof(Record);
      */
      output_recs++;
      Index[min_index]++;
      /*
      //printf("\tIndex[%d] = %d\n",min_index,Index[min_index] );
      */
      if((Index[min_index] == (NumRecs[min_index]))
      &&(GroupOffset[GroupID[min_index]] == (group_num-1))){

        //printf("\treset min\n");
        min_index = -1;
        memset(&min,0,sizeof(Record));
        min.id = -1;
      }
      else{
        //printf("\tslide %d\n",min_index );
        data[min_index]+=sizeof(Record);
       
      }

      output_capacity -= sizeof(Record);
    }
  }

  /*output = BF_Block_GetData(block);
  for(j = 0 ; j <  output_recs; j++){ //read each record in block 
    Record record;
    memset(&record,0,sizeof(Record));
    memcpy(&record,output,sizeof(Record));

    printf("\t\t%d,%s,%s,%s\n",record.id,record.name,record.surname,record.city);
    /*if( output_has_content == 0) 
      SR_InsertEntry(outputDesc,record);
    else
      InsertOutput(outputDesc,record_num,&record);
      
    if(j < output_recs-1)output += sizeof(Record);

  }
  */

  //BF_Block_SetDirty(block);
  //CALL_OR_DIE(BF_UnpinBlock(block));
  BF_Block_Destroy(&block);
  pinned--;

  free(data);
  return SR_OK;

}

SR_ErrorCode clear(int fileDesc) {
  // Your code goes here
  BF_Block* block;
  BF_Block_Init(&block);
  int i ;
  int block_num = 0;
  CALL_OR_DIE(BF_GetBlockCounter(fileDesc,&block_num));


  CALL_OR_DIE(BF_GetBlock(fileDesc,0,block));
  pinned++;
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
      pinned++;
      char*data = BF_Block_GetData(block);
      memset(data,0,BF_BLOCK_SIZE);
      BF_Block_SetDirty(block);
      CALL_OR_DIE(BF_UnpinBlock(block));
      pinned--;
  }

  BF_Block_Destroy(&block);
  return SR_OK;
}

char* MoveGroupIndex(int step_num,int group_id,int group_num,int block_num,int offset,int fileDesc){
  //printf("\tstep_num = %d,group_id = %d,offset = %d\n",step_num,group_id,offset );
  int start_block;
  start_block = step_num;
  BF_Block* block;
  BF_Block_Init(&block);
  int i;
  int next = 0;
  for( i = 0 ; i <= group_id ;i++){
    if(i == 0)next += start_block+offset;
    else{
      
      next += group_num;
    }
  }
  if(next > block_num){
      BF_Block_Destroy(&block);
      return NULL;
  }
  //printf("block_num = %d\n",block_num );
  //printf(">> get block %d\n",next );
  CALL_OR_DIE(BF_GetBlock(fileDesc,next,block));
  pinned++;
  char* data = BF_Block_GetData(block);
  CALL_OR_DIE(BF_UnpinBlock(block));
  pinned--;
  BF_Block_Destroy(&block);
  return data;
}


SR_ErrorCode getNextGroup(int step_num,int bufferSize,int group_num,int block_num,int offset,int fileDesc,int tempDesc){

  int start_block;
  //start_block = ((step_num-1)*(bufferSize+1))+1;
  start_block=  step_num;
  BF_Block* source_block;
  BF_Block_Init(&source_block);

  BF_Block* dest_block;
  BF_Block_Init(&dest_block);
  int i;
  char* source;
  char* dest;



  int record_num=0;
  int buffer_pos = 0;

  int next = 0;
  //printf("getNextGroup :");

  for( i = 0 ; i < bufferSize ;i++){

    if(i == 0)next += start_block+offset;
    
   
    if(next > block_num){
      int k;
      
      for(k = buffer_pos ; k < bufferSize ;k++){
        CALL_OR_DIE(BF_GetBlock(tempDesc,k,dest_block));
        pinned++;
        dest = BF_Block_GetData(dest_block);
        memset(dest,0,BF_BLOCK_SIZE);
        BF_Block_SetDirty(dest_block);
        CALL_OR_DIE(BF_UnpinBlock(dest_block));
        pinned--;
      }
      break;
   
    }
    printf(" %d  ",next);
    CALL_OR_DIE(BF_GetBlock(fileDesc,next,source_block));
    pinned++;
    next += group_num;
    source = BF_Block_GetData(source_block);
    record_num = 0;
    memcpy(&record_num,source,sizeof(int));
    CALL_OR_DIE(BF_GetBlock(tempDesc,buffer_pos,dest_block));
    pinned++;
    dest = BF_Block_GetData(dest_block);
    memset(dest,0,BF_BLOCK_SIZE);
    if(record_num != 0){
      memset(dest,0,BF_BLOCK_SIZE);
      memcpy(dest,source,BF_BLOCK_SIZE);
    }
    buffer_pos+=1;

    BF_Block_SetDirty(dest_block);
    CALL_OR_DIE(BF_UnpinBlock(source_block));
    pinned--;
    CALL_OR_DIE(BF_UnpinBlock(dest_block));
    pinned--;
  }
  printf("\n");

  BF_Block_Destroy(&source_block);
  BF_Block_Destroy(&dest_block);
  return SR_OK;
}

SR_ErrorCode InsertOutput(int fileDesc,int* record_num,Record* record){
  char* data = getRecord(fileDesc,*record_num);
  memset(data,0,sizeof(Record));
  memcpy(data,record,sizeof(Record));
  (*record_num)++;
  return SR_OK;
}

SR_ErrorCode CopyContent(int sourceDesc ,int destDesc){

  printf(">>>> Copy Content >>>>\n");
  int source_blocks;
  int dest_blocks;

  BF_Block* source_block;
  BF_Block* dest_block;
  BF_Block_Init(&source_block);
  BF_Block_Init(&dest_block);

  CALL_OR_DIE(BF_GetBlockCounter(sourceDesc,&source_blocks));
  CALL_OR_DIE(BF_GetBlockCounter(destDesc,&dest_blocks));
  printf("\t source blocks = %d, dest_blocks = %d\n",source_blocks,dest_blocks );
  int i;
  char* source;
  char* dest;



  for( i = 0 ; i < dest_blocks ; i++){
    printf("\tget src block %d\n",i );
    CALL_OR_DIE(BF_GetBlock(sourceDesc,i,source_block));
    pinned++;
    source = BF_Block_GetData(source_block);

    printf("\tget dest block %d\n",i );
    CALL_OR_DIE(BF_GetBlock(destDesc,i,dest_block));
    pinned++;
    dest = BF_Block_GetData(dest_block);


    memset(dest,0,BF_BLOCK_SIZE);
    memcpy(dest,source,BF_BLOCK_SIZE);

    BF_Block_SetDirty(dest_block);
    BF_UnpinBlock(dest_block);
    pinned--;

    BF_UnpinBlock(source_block);
    pinned--;
    

  }

  printf("fill %d\n",source_blocks-dest_blocks );
  for(i = dest_blocks ; i < (source_blocks-dest_blocks ) ; i++){
    BF_Block* block;
    BF_Block_Init(&block);

    CALL_OR_DIE(BF_GetBlock(sourceDesc,i,source_block));
    pinned++;
    source = BF_Block_GetData(source_block);


    CALL_OR_DIE(BF_AllocateBlock(destDesc,block));
    dest = BF_Block_GetData(block);

    memset(dest,0,BF_BLOCK_SIZE);
    memcpy(dest,source,BF_BLOCK_SIZE);


    BF_Block_SetDirty(block);
    BF_UnpinBlock(block);
    pinned--;

    BF_UnpinBlock(source_block);
    pinned--;
    BF_Block_Destroy(&block);
    
  }


  BF_Block_Destroy(&source_block);
  BF_Block_Destroy(&dest_block);
  return SR_OK;
}