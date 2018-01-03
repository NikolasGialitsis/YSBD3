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

  memset(NumRecs,0,(bufferSize-1)*sizeof(int));
  memset(Index,0,(bufferSize-1)*sizeof(int));
  memset(GroupOffset,0,(bufferSize-1)*sizeof(int));

  //printf("bufferSize = %d\n",bufferSize );
  for( i = 0 ; i < bufferSize -1 ; i++){
    CALL_OR_DIE(BF_GetBlock(tempDesc,i,block));
    data[i] = BF_Block_GetData(block);
    memcpy(NumRecs+i,data[i],sizeof(int));
    //printf("NumRecs[%d] = %d\n",i,NumRecs[i] );
    data[i] += sizeof(int);
    Index[i] = 0; 
    GroupOffset[i] = 0; 
    CALL_OR_DIE(BF_UnpinBlock(block));
  }
  CALL_OR_DIE(BF_GetBlock(tempDesc,bufferSize-1,block));
  char * output = BF_Block_GetData(block);
  memset(output,0,BF_BLOCK_SIZE);

  int bl_rec_capacity = (BF_BLOCK_SIZE-sizeof(int))/sizeof(Record);
  int output_capacity = BF_BLOCK_SIZE - sizeof(int);


  int k;
  int min_index = 0;
  int flag = 0;
  Record min;
 
  flag = 1;
  int output_recs = 0;
  while(flag){
    flag = 0;
    min_index = 0;
  
    memset(&min,0,sizeof(Record));
    min.id = -1;
    printf("\n---- Enter Loop: ----\n");
    for(k = 0 ; k < bufferSize-1 ; k++){
      printf("Index[%d] = %d\n",k,Index[k] );
      if(Index[k] < NumRecs[k]){
        //printf("Index[%d]=%d < NumRecs[%d]=%d \n",k,Index[k],k,NumRecs[k] );
        if(!flag){
          memset(&min,0,sizeof(Record));
          memcpy(&min,data[k],sizeof(Record));
          printf("\t case 1:first min\n");
          min_index = k;
          flag = 1;
        }
        else{
          printf("\t case 2:compare\n");
          Record record;


          memset(&record,0,sizeof(Record));
          memcpy(&record,data[k],sizeof(Record));
          if(compare(&record,&min,fieldNo) < 0){
            printf("\t\t new min\n");
            //printf("compare %d,%s < %d,%s\n",record.id,record.name,min.id,min.name );
            min_index = k;
            memset(&min,0,sizeof(Record));
            memcpy(&min,data[k],sizeof(Record));
          
          }
        }
      }
      else{
        if(GroupOffset[k] < (group_num-1)){
          printf("case 3: move index \n");
          data[k] = MoveGroupIndex(step_num,k,group_num,block_num-1,GroupOffset[k]+1,tempOut);
          if(data[k]!= NULL){
            Record record;
            data[k]+=sizeof(int);
            memset(&record,0,sizeof(Record));
            memcpy(&record,data[k],sizeof(Record));
            printf("\treturned %d , %s\n", record.id,record.name);
            //exit(0);

            Index[k] = 0; 
            GroupOffset[k]++;
            if(!flag){
              memset(&min,0,sizeof(Record));
              memcpy(&min,&record,sizeof(Record));
              printf("\tfirst min\n");
              min_index = k;
              flag = 1;
            } 
            else{
              Record record;
              memset(&record,0,sizeof(Record));
              memcpy(&record,data[k],sizeof(Record));
              printf("\t compare\n");
              if(compare(&record,&min,fieldNo) < 0){
                printf("\t\t new min\n");
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
            printf(">> reset <<%d\n",k );
            min_index = -1;
            memset(&min,0,sizeof(Record));
            min.id = -1;
          }
        }
      }
    }

    if(flag){
      if(output_capacity < sizeof(Record)){
        printf("\nOutput to be flushed!!\n");
     
        
        output = BF_Block_GetData(block);
        for(j = 0 ; j <  output_recs; j++){ //read each record in block 
          Record record;
          memset(&record,0,sizeof(Record));
          memcpy(&record,output,sizeof(Record));
          printf("\t\t%d,%s,%s,%s\n",record.id,record.name,record.surname,record.city);

          if( output_has_content == 0) 
            SR_InsertEntry(outputDesc,record);
          else
            InsertOutput(outputDesc,record_num,&record);

         
          output+=sizeof(Record);
         
          
        }



        output = BF_Block_GetData(block);
        memset(output,0,BF_BLOCK_SIZE);
        output_capacity = BF_BLOCK_SIZE-sizeof(int);
        output_recs = 0;
      }
      printf("add %d to output\n",min.id );
      memcpy(output,&min,sizeof(Record));
      output+=sizeof(Record);
      output_recs++;
      Index[min_index]++;
      if((Index[min_index] == (NumRecs[min_index]))
      &&(GroupOffset[min_index] == (group_num-1))){

        printf("\treset min\n");
        min_index = -1;
        memset(&min,0,sizeof(Record));
        min.id = -1;
      }
      else{
        printf("\tslide %d\n",min_index );
        data[min_index]+=sizeof(Record);
              
      }

      output_capacity -= sizeof(Record);
    }
  }

  output = BF_Block_GetData(block);
  for(j = 0 ; j <  output_recs; j++){ //read each record in block 
    Record record;
    memset(&record,0,sizeof(Record));
    memcpy(&record,output,sizeof(Record));
    printf("\t\t%d,%s,%s,%s\n",record.id,record.name,record.surname,record.city);
    if( output_has_content == 0) 
      SR_InsertEntry(outputDesc,record);
    else
      InsertOutput(outputDesc,record_num,&record);
    output += sizeof(Record);
  }



  exit(0);
  BF_Block_SetDirty(block);
  CALL_OR_DIE(BF_UnpinBlock(block));
  BF_Block_Destroy(&block);
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
      memset(data,0,BF_BLOCK_SIZE);
      BF_Block_SetDirty(block);
      CALL_OR_DIE(BF_UnpinBlock(block));
  }

  BF_Block_Destroy(&block);
  return SR_OK;
}

char* MoveGroupIndex(int step_num,int group_id,int group_num,int block_num,int offset,int fileDesc){
 // printf("\tstep_num = %d,group_id = %d,offset = %d\n",step_num,group_id,offset );
  int start_block;
  start_block = step_num;
  BF_Block* block;
  BF_Block_Init(&block);
  int i;
  int next = 0;
  for( i = 0 ; i <= group_id ;i++){
    if(i == 0)next += start_block+offset;
    if(next > block_num)return NULL;
   // printf("next = %d\n", next);
    CALL_OR_DIE(BF_GetBlock(fileDesc,next,block));
    next += group_num;
  }

  char* data = BF_Block_GetData(block);
  CALL_OR_DIE(BF_UnpinBlock(block));
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
  int j;
  int next = 0;
  //printf("getNextGroup :");

  for( i = 0 ; i < bufferSize ;i++){

    if(i == 0)next += start_block+offset;
    
   
    if(next > block_num){
      int k;
      
      for(k = buffer_pos ; k < bufferSize ;k++){
        CALL_OR_DIE(BF_GetBlock(tempDesc,k,dest_block));
        dest = BF_Block_GetData(dest_block);
        memset(dest,0,BF_BLOCK_SIZE);
        BF_Block_SetDirty(dest_block);
        CALL_OR_DIE(BF_UnpinBlock(dest_block));
      }
      break;
   
    }
    printf(" %d  ",next);
    CALL_OR_DIE(BF_GetBlock(fileDesc,next,source_block));
    next += group_num;
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


  int source_blocks;
  int dest_blocks;

  BF_Block* source_block;
  BF_Block* dest_block;
  BF_Block_Init(&source_block);
  BF_Block_Init(&dest_block);

  CALL_OR_DIE(BF_GetBlockCounter(sourceDesc,&source_blocks));
  CALL_OR_DIE(BF_GetBlockCounter(destDesc,&dest_blocks));

  int i;
  char* source;
  char* dest;

  for( i = 0 ; i < dest_blocks ; i++){

    CALL_OR_DIE(BF_GetBlock(sourceDesc,i,source_block));
    source = BF_Block_GetData(source_block);


    CALL_OR_DIE(BF_GetBlock(destDesc,i,dest_block));
    dest = BF_Block_GetData(dest_block);


    memset(dest,0,BF_BLOCK_SIZE);
    memcpy(dest,source,BF_BLOCK_SIZE);

    BF_Block_SetDirty(dest_block);
    BF_UnpinBlock(dest_block);

    BF_UnpinBlock(source_block);
    

  }


  for(i = dest_blocks ; i < (source_blocks-dest_blocks ) ; i++){
    BF_Block* block;
    BF_Block_Init(&block);

    CALL_OR_DIE(BF_GetBlock(sourceDesc,i,source_block));
    source = BF_Block_GetData(source_block);


    CALL_OR_DIE(BF_AllocateBlock(destDesc,block));
    dest = BF_Block_GetData(block);

    memset(dest,0,BF_BLOCK_SIZE);
    memcpy(dest,source,BF_BLOCK_SIZE);


    BF_Block_SetDirty(block);
    BF_UnpinBlock(block);

    BF_UnpinBlock(source_block);
    BF_Block_Destroy(&block);
    
  }


  BF_Block_Destroy(&source_block);
  BF_Block_Destroy(&dest_block);
  return SR_OK;
}