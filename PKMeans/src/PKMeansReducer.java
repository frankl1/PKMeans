import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PKMeansReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
	public static int nb_dimensions;
	public static ArrayList<double[]> centers = new ArrayList<>();
	public static DecimalFormat dFormater = new DecimalFormat("#.###");
	
	public void reduce(LongWritable _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double center[] = new double[nb_dimensions], sample[] = new double[nb_dimensions];
		int num = 0, last_comma_index = -1;
		for (int i = 0; i < nb_dimensions; i++) {
			center[i] = 0.0;
		}
		for (Text val : values) {
			last_comma_index = val.toString().lastIndexOf(",");
			sample = getSample(val.toString().substring(0, last_comma_index));
			for (int i=0; i < nb_dimensions; i++) {
				center[i] += sample[i];
			}
			num += Integer.parseInt(val.toString().substring(last_comma_index+1));
		}
		for (int i = 0; i<nb_dimensions; i++) {
			center[i] /= num;
		}
		StringBuilder center_sb = new StringBuilder();
		for (int i = 0; i<nb_dimensions; i++) {
			center_sb.append(dFormater.format(center[i]));
			if (i < nb_dimensions - 1)
				center_sb.append(" ");
		}
		centers.set((int)_key.get(), center);
		context.write(_key, new Text(center_sb.toString()));
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
