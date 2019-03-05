# Description

Instead of sharing data between mappers using static variable as in the pseudo distributed version, the fully distributed code user job configuration. 

The Python3 script **pkmeans.py** is used to run the distributed pkmeans job until the convergence. The script updates the job configuration with the centroid of the previous iteration before each new iteration.

# Usage

`python3 pkmeans.py INT OUT k d`  

* INT: The name of the dataset folder in hdfs
* OUT: The name of the directory where the output will be written
* k: The number of clusters
* d: The number of dimension of datapoints