import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PKMeansCombiner extends Reducer<LongWritable, Text, LongWritable, Text> {
	
	public void reduce(LongWritable _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		int nb_dimensions = conf.getInt("nb_dimensions", 0);
		double sum[] = new double[nb_dimensions], sample[] = new double[nb_dimensions];
		int num = 0;
		for (int i = 0; i < nb_dimensions; i++) {
			sum[i] = 0.0;
		}
		for (Text val : values) {
			int first_comma_index = val.toString().indexOf(",");
			sample = getSample(val.toString().substring(first_comma_index + 1));
			for (int i=0; i < nb_dimensions; i++) {
				sum[i] += sample[i];
			}
			num++;
		}
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i<nb_dimensions; i++) {
			sb.append(sum[i]);
			sb.append(",");
		}
		sb.append(num);
		context.write(_key, new Text(sb.toString()));
	}

	public double[] getSample(String sampleText) {
		String[] sampleStr = sampleText.split(",");
		double[] sample = new double[sampleStr.length]; 
		for(int i=0; i < sampleStr.length; i++) {
			sample[i] = Double.parseDouble(sampleStr[i]);
		}
		return sample;
	}
}
