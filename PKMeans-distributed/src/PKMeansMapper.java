import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class PKMeansMapper extends Mapper<Object, Text, LongWritable, Text> {
	private Logger logger = Logger.getLogger(PKMeansMapper.class);
	
	public void map(Object ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		ArrayList<double[]> centers = getCentroidsFromString(conf.get("centroids"));
		int first_comma_index = ivalue.toString().indexOf(",");
		double sample[] = getSample(ivalue.toString().substring(first_comma_index + 1));
		double min = Double.MAX_VALUE, dist;
		long index = -1;
		for(int i = 0; i < centers.size(); i++) {
			dist = distance(sample, centers.get(i));
			if (dist < min) {
				min = dist;
				index = i;
			}
		}
		if (index != -1)
			context.write(new LongWritable(index), new Text(ivalue.toString()));
		else
			logger.info("\n\nNo nearest cluster found? min = " + min + " sample = " + sample + "\n\n");
	}
	
	public ArrayList<double[]> getCentroidsFromString(String centroids_str) {
		ArrayList<double[]> centers = new ArrayList<>();
		String[] centers_tab = centroids_str.split("\t");
		for(String part: centers_tab) {
			String[] center_tab = part.split(" ");
			double[] center = new double[center_tab.length];
			int i = 0;
			for (String coord: center_tab) {
				center[i] = Double.parseDouble(coord);
				i++;
			}
			centers.add(center);
		}
		return centers;
	}
	
	public double[] getSample(String sampleText) {
		String[] sampleStr = sampleText.split(",");
		logger.info("Sample length " + sampleStr.length);
		double[] sample = new double[sampleStr.length]; 
		for(int i=0; i < sampleStr.length; i++) {
			sample[i] = Double.parseDouble(sampleStr[i]);
		}
		return sample;
	}
	
	public double distance(double[] sample, double[] center) {
		double d = .0f;
		for (int i=0; i < sample.length; i++) {
			d += ((sample[i]-center[i]) * (sample[i] - center[i]));
		}
		return Math.sqrt(d);
	}
}
