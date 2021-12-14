import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
class CleanMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		context.write(key, value);
	}
}

class CleanReducer extends Reducer<LongWritable, Text, Text, Text> {
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		//if there is comma in cell, line length = 13 (col[8] will always say non-hispanic), else line length = 12 
		String[] cols;
		String[] dates;
		String date;
		for(Text value : values){
			cols = value.toString().split(",");
			//get reformat dates into sql format
			if(cols.length == 12 || cols.length == 13){
				for(int i = 0; i < 4; i++){
					if(!cols[i].equals("") && cols[i].contains("/")){
						date = "";
						dates = cols[i].split("/");
						date = dates[0] + "-" + dates[1] + "-" + dates[2];
						cols[i] = date;
					}
				}
			}
			//get rid of extra ethnicity and combine dates in line
			if(cols.length == 12){
				String toReturn = "";
				for(int i = 0; i < 12; i++)
					toReturn += cols[i] + ",";
				toReturn = toReturn.substring(0,toReturn.length()-1); //get rid of last comma
				context.write(new Text(toReturn), new Text(""));
			}
			else if(cols.length == 13){
				String toReturn = "";
				for(int i = 0; i < 13; i++){
					if(i != 8)
						toReturn += cols[i] + ",";
				}
				toReturn = toReturn.substring(0,toReturn.length()-1); //get rid of last comma
				context.write(new Text(toReturn), new Text(""));
			}
			else
				context.write(new Text(""), new Text(""));
		}
	}
}

public class Clean {
	public static void main(String[] args) throws Exception {
		if(args.length != 2) {
			System.err.println("Usage: Clean <input path> <output path>");
			System.exit(-1);
		}
		Job job = new Job();
		job.setJarByClass(Clean.class);
		job.setJobName("Clean");
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(CleanMapper.class);
		job.setReducerClass(CleanReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}