#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "mpi.h"

int main( int argc, char *argv[])
{

	 MPI_Init(&argc, &argv);
	
	 int myrank, size; //size will take care of number of processes 
		 
	 double sTime, eTime, time,max_time;

	 	  

	  MPI_Comm_rank(MPI_COMM_WORLD, &myrank) ;
	  MPI_Comm_size(MPI_COMM_WORLD, &size);

        //counting the number of rows and columns
        
	FILE *mf=fopen("test.csv", "r");
	if(mf==NULL)
	{
		perror("Unable to open the file");
		exit(1);
	}
	
	char line[20000];
	
	int r=0, c;
	
	while(fgets(line,sizeof(line), mf))
	{
		char *token;
		
		c=0;
		
		token=strtok(line, ",");
		
		while(token!=NULL)
		{
			
			c++;
			token= strtok(NULL, ",");
		}
		
		r++;
		
	}
	
	fclose(mf);
	
	r=r-1, c=c-2;
	//printf("The number of row and column is is %d %d \n", r, c);
	
	

       //declaring the matrix of size m[r][c]
	
	double **mat = (double **)malloc(r * sizeof(double *));
    	
        for (int i=0; i<r; i++)
        mat[i] = (double *)malloc(c * sizeof(double));
	
	
	
	
	// Previous regions code
	
	mf=fopen("test.csv", "r");
	if(mf==NULL)
	{
		perror("Unable to open the file");
		exit(1);
	}
	
	
	int i=0,j=0;
	
	while(fgets(line,sizeof(line), mf))
	{
		char *token1;
		
		j=0;
		
		token1=strtok(line, ",");
		
		while(token1!=NULL)
		{
			if(i>0 && j>1)
		        {
				double d;
	   			sscanf(token1, "%lf", &d);
				//printf(" %lf",d);
				mat[i-1][j-2] = d;
			}
			
			token1= strtok(NULL, ",");
			
			j++;
		}
		
		i++;
		//printf("\n");
	}
	
	fclose(mf);
	
	//Printing the matrix
	
	/*
	for (int i = 0; i <  r; i++)
	{
      		for (int j = 0; j < c; j++)
      		{
         		printf("%lf ", mat[i][j]);
         	}
         	
         	printf("\n");
         }
         */
         
         
         //To find the minimum of every year, we must at first declare an array having the size as total number of columns
	
	 double* arr;
	 arr = (double*)malloc(c * sizeof(double));
	 
	 double min;
	 
	 for (int j = 0; j <  c; j++)
	 {
	  	min=1000;
      		for (int i = 0; i < r; i++)
      		{
         		if(mat[i][j]<min)
         		min=mat[i][j];
         	}
         	
         	arr[j]=min;
          }
	
	
	// checking the minimum value of 1960
	// printf("%lf \n", arr[0]);
	
	//Now we need to find the global minimum
	
	min=1000;
	
	for(int i=0;i<c;i++)
	{
		if(arr[i]<min)
		min=arr[i];
	}
	
	//printf("The golbal minimum is %lf \n",min);
	
	
	//Now we will send from one matrix to another using MPI_Pack
	
	// For that we will need buffers which will have same number of rows as number of columns
	
	double snd_buf[4][r];
	double recv_buf[4][r];
	double buf[4][r];
	
	//two store the minimum elements when we have 2 processes
	double arr2[c];
	
	//total size of the data that is beign sent
	int outs=r*8;
	
	int position=0;
	
	
	//since we will be sending maximum c/2 number of columns so we have taken c
	MPI_Request request[c];
  	MPI_Request request1[c];
  	MPI_Status status[c];
	
	//packing and sending the data
	if(myrank==0)
		{
			//we will send the half of the matrix to process 2
			for(int j=4;j<c;j++)
			{
				position=0; //reassigning position after each and every send
				
				for(int i=0;i<r;i++)
				{
					MPI_Pack(&mat[i][j], 1 , MPI_DOUBLE,snd_buf[j-4],outs,&position,MPI_COMM_WORLD);
				}
			}
			
			
			//sending all the buffers
			for(int j=4;j<c;j++)
			{
				MPI_Send (snd_buf[j-4], 10 , MPI_PACKED, 1 /*dest*/ , j /*tag*/ , MPI_COMM_WORLD);
			}
			
			
			
		} 	
	
	
	
	//receiving the data
	
	if(myrank==1)
		{
		
			 for(j=4;j<c;j++)
			 {		
				 MPI_Recv(recv_buf[j-4], 10, MPI_PACKED, 0 /*src*/ , j /*tag*/, MPI_COMM_WORLD,&status[j]);
				
			 }
			
			
			
			for(int j=4; j<c;j++)
			{
				position=0;
				 for(int i=0;i<r;i++)
				 {
				 	MPI_Unpack(recv_buf[j-4],80,&position,&buf[j-4][i], 1, MPI_DOUBLE, MPI_COMM_WORLD);
				 }
			}
			
			
						
			//min1 will store the minimum for rank1
			double min1=1000;
			
			for(int j=4;j<c;j++)
			{
				min1=1000;
				
				for(int i=0;i<r;i++)
				 {
				 	if(buf[j-4][i]<min1)
				 	min1=buf[j-4][i];
				 }
				 
				 arr2[j]=min1;
				 
				 
			}
			
		}
		
		
		
		MPI_Barrier(MPI_COMM_WORLD);
		
		
		//now we will print the minimum array using rank0 i.e the root
		
		
		//checking whether the packing in snd_buff is taking place correctly or not
		if(myrank==1)
		{
			for(int j=4;j<c;j++)
			{
				
				
				for(int i=0;i<r;i++)
				 {
				 	printf("%lf ",buf[j-4][i]);
				 
				 }
				 
				 printf("\n");
				 
				 
			}
		}
		
		
		
	    MPI_Finalize();
  	    return 0;
	
	
}
