# PKMeans
Parallel KMeans algorithm for hadoop. This is an implementation of PKMeans, the MapReduce design of KMeans as described in the paper Parallel K -Means Clustering Based on MapReduce

## Dataset format
The dataset is composed of a set of csv files in which each line is a data point. Each data point is represented by its identifier followed by its coordinates.

## Usage
hadoop jar pkmeans.jar PKMeans input output
- *input* is the dataset folder
- *output* is the name of the output folder
 
 Running the program will create output_1, ouput_2, ..., output_i where *i* is the number of iterations of KMeans