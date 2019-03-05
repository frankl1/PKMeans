import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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
	private String src, out, centroids_file;
	private Logger logger = Logger.getLogger(PKMeansMapper.class);
	public static DecimalFormat dFormater = new DecimalFormat("0.000");
	private static Options options;

	public static void main(String[] args) {
		try {
			createOptions();
			CommandLineParser parser = new BasicParser();
			CommandLine cmd = parser.parse(options, args);

			String input = cmd.getOptionValue("input");
			String output = cmd.getOptionValue("output");
			String centroid_file = cmd.getOptionValue("centroid_file");

			PKMeans pkMeans = new PKMeans(input, output, centroid_file);
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

	public PKMeans(String src, String out, String centroid_file) throws Exception {
		this.src = src;
		this.out = out;
		this.centroids_file = centroid_file;
		this.centers = new ArrayList<double[]>();
		this.readCenters();
	}

	@SuppressWarnings("static-access")
	public static void createOptions() {
		options = new Options();
		options.addOption(OptionBuilder.withLongOpt("input").withArgName("IN").hasArg()
				.withDescription("The folder that contains the dataset files").isRequired(true).create("i"));
		options.addOption(OptionBuilder.withLongOpt("output").withArgName("OUT").hasArg()
				.withDescription("The folder where the output will be written").isRequired(true).create("o"));
		options.addOption(OptionBuilder.withLongOpt("centroid_file").withArgName("CENTROIDS").hasArg()
				.withDescription("The file that contains the initial centroid").isRequired(true).create("c"));
	}

	public void initJob() throws Exception {
		conf = new Configuration();
		conf.set("centroids", centersToString(centers));
		
		conf.setInt("nb_dimensions", this.centers.get(0).length);
		job = Job.getInstance(conf, "PKMeans");
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

	public void readCenters() {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(new File(this.centroids_file)));
			String center;
			while ((center = reader.readLine()) != null) {
				StringTokenizer tokenizer = new StringTokenizer(center);
				double[] centroid = new double[tokenizer.countTokens() - 1];
				int centerId = Integer.parseInt(tokenizer.nextToken());
				for (int i = 0; i < centroid.length; i++) {
					centroid[i] = Double.parseDouble(tokenizer.nextToken());
				}
				this.centers.add(centerId, centroid);
			}
			reader.close();
		} catch (IOException e) {
			logger.error("problem reading centroids file", e);
		}
	}

	public void run() throws Exception {
//		logger.info("\n\nCentroid: " + centersToString(this.centers) + "\n\n");
		initJob();
		if (!job.waitForCompletion(true))
			logger.info("job failed");
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
}
