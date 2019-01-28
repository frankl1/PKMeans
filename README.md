# PKMeans
This is an implementation of PKMeans, the MapReduce design of KMeans as described in the paper Parallel K -Means Clustering Based on MapReduce by *Weizhong Zhao, Huifang Ma and Qing He*.

## Dataset format
The dataset is composed of a set of csv files in which each line is a data point. Each data point is represented by its identifier followed by its coordinates.

## Usage
`hadoop jar pkmeans.jar PKMeans -d <DIM> [-e <EPS>] -i <IN> -k <NB_CLUSTERS> -o <OUT>`  
&ensp;&ensp; -d <DIM>             The number of dimensions of each data point.  
&ensp;&ensp; -e,--epsilon <EPS>   The number of significant decimals to consider when comparing floats. ex: 1e-3 means only the first 3 decimals are significant.  
&ensp;&ensp;  -i,--input <IN>      The folder that contains the dataset files.   
&ensp;&ensp;  -k <NB_CLUSTERS>     The number of clusters.   
&ensp;&ensp;  -o,--output <OUT>    The folder where the output will be written.   

Running the program will create in the output folder a file containing the coordinated of each centroid
