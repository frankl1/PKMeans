import java.io.File;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class PKMeans {
	private Configuration conf;
	private Job job;
	private ArrayList<double[]> centers;
	private double epsilon;
	private String src, out;
	private int nb_clusters, nb_dimensions;
	private Random random;
	private Logger logger = Logger.getLogger(PKMeansMapper.class);
	public static DecimalFormat dFormater = new DecimalFormat("0.000");

	public static void main(String[] args) {
		try {
			PKMeans pkMeans = new PKMeans(2, 8, 1e-3, "inputs", "outputs");
			pkMeans.run();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.err.println(e.getMessage());
		}
	}

	public PKMeans(int nb_clusters, int nb_dimensions, double epsilon, String src, String out) throws Exception{
		this.epsilon = epsilon;
		this.src = src;
		this.out = out;
		this.nb_clusters = nb_clusters;
		this.nb_dimensions = nb_dimensions;
		this.random = new Random(8513);
		this.centers = new ArrayList<double[]>();
		this.initCenters();
		PKMeansReducer.centers.addAll(centers);
		PKMeansCombiner.nb_dimensions = this.nb_dimensions;
		PKMeansReducer.nb_dimensions = this.nb_dimensions;
	}
	
	public void initJob()  throws Exception {
		conf = new Configuration();
		job = Job.getInstance(conf, "JobName");
		job.setJarByClass(PKMeans.class);

		job.setMapperClass(PKMeansMapper.class);
		job.setCombinerClass(PKMeansCombiner.class);
		job.setReducerClass(PKMeansReducer.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path(this.src));
		FileOutputFormat.setOutputPath(job, new Path(this.out));
	}

	public void initCenters() {
		for (int i = 0; i < this.nb_clusters; i++) {
			double[] center = new double[this.nb_dimensions];
			for (int j = 0; j < this.nb_clusters; j++) {
				center[j] = this.random.nextDouble();
			}
			this.centers.add(i, center);
		}
	}

	public void run() throws Exception {
		int nb_iter = 1;
		do {
			deleteOutput();
			initJob();
			centers.clear();
			centers.addAll(PKMeansReducer.centers);
			PKMeansMapper.centers.clear();
			PKMeansMapper.centers.addAll(centers);
			if (!job.waitForCompletion(true))
				logger.info("job failed");
			nb_iter++;
			logger.error("\n#iter=" + nb_iter + "\n");
			logger.error("\tOld_centers: " + centersToString(centers) + "\n");
			logger.error("\tNew_centers: " + centersToString(PKMeansReducer.centers)+"\n");
		} while (isDifferent(centers, PKMeansReducer.centers));
		logger.error("Old_centers: " + centersToString(centers) + "\n");
		logger.error("New_centers: " + centersToString(PKMeansReducer.centers));
	}

	public String centersToString(ArrayList<double[]> centers) {
		StringBuilder sb = new StringBuilder();
		for (double[] center : centers) {
			sb.append(centerToString(center) + "\t");
		}
		return sb.toString();
	}

	public String centerToString(double[] center) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < center.length; i++) {
			sb.append(dFormater.format(center[i]) + " ");
		}
		return sb.toString();
	}

	public boolean isDifferent(ArrayList<double[]> old_centers, ArrayList<double[]> new_centers) {
		if (old_centers.size() != new_centers.size()) {
			return true;
		}
		for (int i = 0; i < old_centers.size(); i++) {
			if (isDifferent(old_centers.get(i), new_centers.get(i))) {
				return true;
			}
		}
		return false;
	}

	public void deleteOutput() {
		try {
			File index = new File(this.out);
			String[] entries = index.list();

			if (entries == null)
				return;

			for (String s : entries) {
				File currentFile = new File(index.getPath(), s);
				currentFile.delete();
			}
			index.delete();
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	public boolean isDifferent(double[] center1, double[] center2) {
		for (int i = 0; i < center1.length; i++) {
			if (Math.abs(center1[i] - center2[i]) > epsilon) {
				return true;
			}
		}
		return false;
	}

}
