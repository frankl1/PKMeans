import sys
import subprocess
import random
import time

in_folder = sys.argv[1]
out_folder = sys.argv[2]
k = int(sys.argv[3])
d = int(sys.argv[4])
initial_centroids = f".pkmeans_set_initial_centroids{time.time()}"

nb_decimals = 3
stop = False

def set_initial_centroids(centroids=None):
	with open(initial_centroids, "w", encoding="utf8") as f:
		print(f"Set initial_centroids:  nb of centroids={0 if centroids is None else len(centroids)}")
		if centroids is None:
			centroids = []
			for i in range(k):
				centroid = []
				for j in range(d):
					centroid.append(round(random.uniform(-100, 100), nb_decimals))
				f.write(f"{i}\t{' '.join(list(map(lambda x: str(x), centroid)))}")
				if i < k:
					f.write("\n")
				centroids.append(centroid)
		else:
			for i, centroid in enumerate(centroids):
				f.write(f"{i}\t{' '.join(list(map(lambda x: str(x), centroid)))}")
				if i < k:
					f.write("\n")
	return centroids

def read_centroids(file):
	centroids = []
	for line in file:
		line = line.strip()
		centroid = line.split()[1:]
		centroid = list(map(lambda x: round(float(x), nb_decimals), centroid))
		centroids.append(centroid)
	return centroids


set_initial_centroids()
subprocess.run(["hdfs", "dfs", "-put", initial_centroids])
subprocess.run(["hdfs", "dfs", "-cat", initial_centroids])
current_centroids = set_initial_centroids()
print(current_centroids)

ret = subprocess.run(["hdfs", "dfs", "-ls", out_folder])

if ret.returncode == 0:
	print(f"Output directory {out_folder} Already exist in HDFS")
	exit(0)

nb_iter = 0
while not stop:
	subprocess.run(["hdfs", "dfs", "-rm", "-r", out_folder])
	subprocess.run(["hadoop", "jar", 
		"pkmeans.jar", "PKMeans", 
		"-i", in_folder, "-o", out_folder, "-c", initial_centroids])
	proc = subprocess.Popen(["hdfs", "dfs", "-cat", out_folder+"/*"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
	new_centroids = read_centroids(proc.stdout)
	print("###################")
	print(current_centroids)
	print("-------------------")
	print(new_centroids)
	print("###################")
	stop = (current_centroids == new_centroids)
	current_centroids = new_centroids.copy()
	set_initial_centroids(current_centroids)
	nb_iter += 1;

print(current_centroids)
subprocess.run(["hdfs", "dfs", "-rm", initial_centroids])
print(f"End after {nb_iter} iterations")