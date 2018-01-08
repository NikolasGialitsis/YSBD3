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
  printf("*record_num = %d\n",*record_num );

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

    data[i] = calloc(BF_BLOCK_SIZE,sizeof(char));
    char* block_data = BF_Block_GetData(block);
    memcpy(data[i],block_data,BF_BLOCK_SIZE);
    memcpy(NumRecs+i,data[i],sizeof(int));
    //printf("NumRecs[%d] = %d\n",i,NumRecs[i] );
    data[i] += sizeof(int);
    Index[i] = 0; 
    GroupOffset[i] = 0; 
    CALL_OR_DIE(BF_UnpinBlock(block));
  }
  //printf("get block\n");
  CALL_OR_DIE(BF_GetBlock(tempDesc,bufferSize-1,block));
  //printf("ok\n");
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
   // printf("\n---- Enter Loop: ----\n");
    for(k = 0 ; k < bufferSize-1 ; k++){
    //  printf("Index[%d] = %d\n",k,Index[k] );
      if(Index[k] < NumRecs[k]){
        //printf("Index[%d]=%d < NumRecs[%d]=%d \n",k,Index[k],k,NumRecs[k] );
        if(!flag){
          memset(&min,0,sizeof(Record));
          memcpy(&min,data[k],sizeof(Record));
        //  printf("\t case 1:first min\n");
          min_index = k;
          flag = 1;
        }
        else{
       //   printf("\t case 2:compare\n");
          Record record;


          memset(&record,0,sizeof(Record));
          memcpy(&record,data[k],sizeof(Record));
          if(compare(&record,&min,fieldNo) < 0){
           // printf("\t\t new min\n");
            //printf("compare %d,%s < %d,%s\n",record.id,record.name,min.id,min.name );
            min_index = k;
            memset(&min,0,sizeof(Record));
            memcpy(&min,data[k],sizeof(Record));
          
          }
        }
      }
      else{
        if(GroupOffset[k] < (group_num-1)){
         // printf("case 3: move index \n");
         // printf("move group\n");
          data[k] = MoveGroupIndex(step_num,bufferSize-1,k,group_num,block_num-1,GroupOffset[k]+1,tempOut);
         // printf("ok\n");
          if(data[k]!= NULL){
            Record record;
            memcpy(NumRecs+k,data[k],sizeof(int));
            data[k]+=sizeof(int);
            memset(&record,0,sizeof(Record));
            memcpy(&record,data[k],sizeof(Record));
           // printf("\treturned %d , %s\n", record.id,record.name);
            //exit(0);

            Index[k] = 0; 
            GroupOffset[k]++;
            if(!flag){
              memset(&min,0,sizeof(Record));
              memcpy(&min,&record,sizeof(Record));
           //   printf("\tfirst min\n");
              min_index = k;
              flag = 1;
            } 
            else{
              Record record;
              memset(&record,0,sizeof(Record));
              memcpy(&record,data[k],sizeof(Record));
           //   printf("\t compare\n");
              if(compare(&record,&min,fieldNo) < 0){
              //  printf("\t\t new min\n");
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
          //  printf(">> reset <<%d\n",k );
            min_index = -1;
            memset(&min,0,sizeof(Record));
            min.id = -1;
          }
        }
      }
    }

    if(flag){
      if(output_capacity < sizeof(Record)){
      //  printf("\nOutput to be flushed!!\n");
    
        output = BF_Block_GetData(block);
        for(j = 0 ; j <  output_recs; j++){ //read each record in block 
          Record record;
          memset(&record,0,sizeof(Record));
          memcpy(&record,output,sizeof(Record));
          //printf("\t\t%d,%s,%s,%s\n",record.id,record.name,record.surname,record.city);
          if( output_has_content == 0) 
            SR_InsertEntry(outputDesc,record);
          else
            InsertOutput(outputDesc,record_num,&record);
          (*record_num)++;
          output+=sizeof(Record);
         
          
        }



        output = BF_Block_GetData(block);
        memset(output,0,BF_BLOCK_SIZE);
        output_capacity = BF_BLOCK_SIZE-sizeof(int);
        output_recs = 0;
      }
      memcpy(output,&min,sizeof(Record));
      output+=sizeof(Record);
      output_recs++;
      Index[min_index]++;
      if((Index[min_index] == (NumRecs[min_index]))
      &&(GroupOffset[min_index] == (group_num-1))){

        min_index = -1;
        memset(&min,0,sizeof(Record));
        min.id = -1;
      }
      else{
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
    //printf("\t\t%d,%s,%s,%s\n",record.id,record.name,record.surname,record.city);
    if( output_has_content == 0) 
      SR_InsertEntry(outputDesc,record);
    else
      InsertOutput(outputDesc,record_num,&record);
    (*record_num)++;
    output += sizeof(Record);
  }

  //for(i = 0 ; i < bufferSize-1;i++) if(data[i])free(data[i]);
  if(data)free(data);
  BF_Block_SetDirty(block);
  CALL_OR_DIE(BF_UnpinBlock(block));
  BF_Block_Destroy(&block);
  printf("--\n");
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

char* MoveGroupIndex(int step_num,int bufferSize,int group_id,int group_num,int block_num,int offset,int fileDesc){
  //printf("\tstep_num = %d,group_id = %d,offset = %d\n",step_num,group_id,offset );
  int start_block;
  start_block = ((step_num-1)*(group_num*bufferSize))+1;
  //printf("\t\tstart = %d\n\n",start_block );
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
 // printf("get block %d\n",next );
  CALL_OR_DIE(BF_GetBlock(fileDesc,next,block));
 // printf("ok\n");
 // printf("%d\n",next );
  pinned++;
  char* data = BF_Block_GetData(block);
  CALL_OR_DIE(BF_UnpinBlock(block));
  pinned--;
  BF_Block_Destroy(&block);
  return data;
}


SR_ErrorCode getNextGroup(int step_num,int bufferSize,int group_num,int block_num,int fileDesc,int tempDesc){

  int start_block;
  start_block = ((step_num-1)*(group_num*bufferSize))+1;

  BF_Block* source_block;
  BF_Block_Init(&source_block);

  BF_Block* dest_block;
  BF_Block_Init(&dest_block);
  int i;
  char* source;
  char* dest;



  int record_num=0;
  int buffer_pos = 0;

  int next = start_block;
  //printf("getNextGroup :");

  for( i = 0 ; i < bufferSize ;i++){

    
   
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
 // printf("insert %d\n",*record_num );
  int target_block;
  int bl_rec_capacity;
  char* data;
  int offset ;
  BF_Block* block = NULL;
  BF_Block_Init(&block);
  bl_rec_capacity = (BF_BLOCK_SIZE-sizeof(int))/sizeof(Record);
  target_block = (*record_num)/(bl_rec_capacity);
  target_block++;
  //printf("target_block = %d\n",target_block );
  CALL_OR_DIE(BF_GetBlock(fileDesc,target_block,block));
  data = BF_Block_GetData(block);
  int numrecs = 0;
  memcpy(&numrecs,data,sizeof(int));
  numrecs++;
  memcpy(data,&numrecs,sizeof(int));
  data+=sizeof(int);
  offset = (*record_num)%(bl_rec_capacity)  ;
  data+=(offset*sizeof(Record));
  memset(data,0,sizeof(Record));
  memcpy(data,record,sizeof(Record));
  
  BF_Block_SetDirty(block);
  BF_UnpinBlock(block);
  BF_Block_Destroy(&block);
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
 // printf("\t source blocks = %d, dest_blocks = %d\n",source_blocks,dest_blocks );
  int i;
  char* source;
  char* dest;



  //printf("source = %d ,dest = %d\n",source_blocks,dest_blocks );
  int min = dest_blocks;
  if(source_blocks < dest_blocks)min = source_blocks;
  
  for( i = 0 ; i < min ; i++){
    //printf("\tget src block %d\n",i );
    CALL_OR_DIE(BF_GetBlock(sourceDesc,i,source_block));
    pinned++;
    source = BF_Block_GetData(source_block);

   // printf("\tget dest block %d\n",i );
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
 // printf("ok\n");
//  fflush(stdout);
  //printf("fill %d\n",source_blocks-dest_blocks );
 // printf("min = %d\n",min );
  if(min < dest_blocks){
   // printf("%d -> %d \n",min,min+dest_blocks-source_blocks);
    for( i = min ; i < min + dest_blocks-source_blocks ; i++){
    //  printf("inside\n");
      BF_Block* block;
      BF_Block_Init(&block); 

      CALL_OR_DIE(BF_AllocateBlock(sourceDesc,block));
      pinned++;

      BF_Block_SetDirty(block); 
      BF_UnpinBlock(block);
      pinned--;
      BF_Block_Destroy(&block);
    }
    BF_Block_Destroy(&source_block);
    BF_Block_Destroy(&dest_block);
    return SR_OK;
  } 
  for(i = min; i < (source_blocks-dest_blocks ) ; i++){
    BF_Block* block;
    BF_Block_Init(&block);

    CALL_OR_DIE(BF_AllocateBlock(destDesc,block));
    dest = BF_Block_GetData(block);
    
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
