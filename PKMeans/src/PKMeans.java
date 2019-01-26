import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Random;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
	private int nb_iter;
	private FileSystem fs;
	private static Options options;

	public static void main(String[] args) {
		try {
			createOptions();
			CommandLineParser parser = new BasicParser();
			CommandLine cmd = parser.parse(options, args);

			double eps = Double.parseDouble(cmd.getOptionValue("epsilon", "1e-3"));
			int k = Integer.parseInt(cmd.getOptionValue('k'));
			int d = Integer.parseInt(cmd.getOptionValue('d'));
			String input = cmd.getOptionValue("input");
			String output = cmd.getOptionValue("output");

			PKMeans pkMeans = new PKMeans(k, d, eps, input, output);
			pkMeans.run();
		} catch (ParseException e) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("PKMeans", options, true);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.err.println(e.getMessage());
		}
	}

	public PKMeans(int nb_clusters, int nb_dimensions, double epsilon, String src, String out) throws Exception {
		this.epsilon = epsilon;
		this.src = src;
		this.out = out;
		this.nb_clusters = nb_clusters;
		this.nb_dimensions = nb_dimensions;
		this.random = new Random(8513);
		this.centers = new ArrayList<double[]>();
		this.nb_iter = 1;
		this.initCenters();
		PKMeansReducer.centers.addAll(centers);
		PKMeansCombiner.nb_dimensions = this.nb_dimensions;
		PKMeansReducer.nb_dimensions = this.nb_dimensions;
	}

	@SuppressWarnings("static-access")
	public static void createOptions() {
		options = new Options();
		options.addOption(OptionBuilder.withArgName("NB_CLUSTERS").hasArg().withDescription("The number of clusters").withType(1)
				.isRequired(true).create("k"));
		options.addOption(
				OptionBuilder.withArgName("DIM").hasArg().withDescription("The number of dimensions of each data point")
						.withType(1).isRequired(true).create("d"));
		options.addOption(OptionBuilder.withLongOpt("input").withArgName("IN").hasArg()
				.withDescription("The folder that contains the dataset files").isRequired(true).create("i"));
		options.addOption(OptionBuilder.withLongOpt("output").withArgName("OUT").hasArg()
				.withDescription("The folder where the output will be written").isRequired(true).create("o"));
		options.addOption(OptionBuilder.withLongOpt("epsilon").withArgName("EPS").hasArg()
				.withDescription("The number of decimals to consider while comparing decimal. ex: 1e-3").withType(1e-3)
				.isRequired(false).create("e"));
	}

	public void initJob() throws Exception {
		conf = new Configuration();
		fs = FileSystem.get(new Configuration());
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
		do {
			initJob();
			deleteOutputDirIfExists();
			centers.clear();
			centers.addAll(PKMeansReducer.centers);
			PKMeansMapper.centers.clear();
			PKMeansMapper.centers.addAll(centers);
			if (!job.waitForCompletion(true))
				logger.info("job failed");
			nb_iter++;
			logger.info("\n#iter=" + nb_iter + "\n");
			logger.info("\tOld_centers: " + centersToString(centers) + "\n");
			logger.info("\tNew_centers: " + centersToString(PKMeansReducer.centers) + "\n");
		} while (isDifferent(centers, PKMeansReducer.centers));
		logger.info("Finished in " + nb_iter + " iterations");
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

	public void deleteOutputDirIfExists() {
		try {
			fs.delete(new Path(this.out), true);
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
