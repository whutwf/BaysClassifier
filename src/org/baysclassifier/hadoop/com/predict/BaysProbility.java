package org.baysclassifier.hadoop.com.predict;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.baysclassifier.hadoop.com.help.MyUnits;

public class BaysProbility {
	
	public static int run(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf, "probility");
        job.setJarByClass(BaysProbility.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(ProbMap.class);
		Path intFilePath = new Path(MyUnits.OUTPUT_WORDS_IN_CLASS.concat("/part-r-00000"));
		Path outFilePath = new Path(MyUnits.PRIOR_PROBALITY);

		//输出路径已存在删除
		outFilePath.getFileSystem(conf).delete(outFilePath, true);
		FileInputFormat.addInputPath(job, intFilePath);
        FileOutputFormat.setOutputPath(job, outFilePath);      
        
        return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class ProbMap extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		
		private static Map<String, Integer> totalWordsInClass = new HashMap<>();
		private static int totalClassWords;
		private static DoubleWritable probility = new DoubleWritable();
		private static Text className = new Text();
//		private static Map<String, Double> condProbMap = new HashMap<>();
		private static Map<String, Map<String, Double>> condProbMap = new HashMap<String, Map<String, Double>>();
		private static Map<String, Double> priorMap = new HashMap<>();
		private static Map<String, Double> notFoundMap = new HashMap<>();			
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			double condProb = 0.0;
			String[] temp = value.toString().split("\t");
			if(temp.length == 3) {
				condProb = (Double.parseDouble(temp[2]) + 1.0) / (totalWordsInClass.get(temp[0]) + totalClassWords);
				if(condProbMap.containsKey(temp[0])) {
					condProbMap.get(temp[0]).put(temp[1], condProb);
				} else {
					Map<String, Double> tempCondMap = new HashMap<>();
					tempCondMap.put(temp[1], condProb);
					condProbMap.put(temp[0], tempCondMap);
				}		
			}
		
			className.set(temp[0] + "\t" + temp[1]);
			probility.set(condProb);			
			context.write(className, probility);
		}
		
		//执行初始化动作，只执行一次
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			priorProbility(conf);
			totalClassAboutWords(conf);
			notFoundProb();
//			for (Entry<String, Integer> entry : totalWordsInClass.entrySet()) { 
//				 System.out.println(entry.getKey() + "\t" + entry.getValue());
//			}	
		}
		
		//统计所有类的字典个数
		public static void totalClassAboutWords(Configuration conf) throws IOException {
			FileSystem fs = FileSystem.get(URI.create(MyUnits.OUTPUT_WORDS_IN_CLASS.concat("/part-r-00000")), conf);
			FSDataInputStream inputStream = fs.open(new Path(MyUnits.OUTPUT_WORDS_IN_CLASS.concat("/part-r-00000")));
			BufferedReader buffer = new BufferedReader(new InputStreamReader(inputStream));
			String strLine = null;
			while((strLine = buffer.readLine()) != null) {
				String[] temp = strLine.split("\t");
				if(!totalWordsInClass.containsKey(temp[0])) {
					totalWordsInClass.put(temp[0], Integer.parseInt(temp[2]));
				} else {
					int currentNum = totalWordsInClass.get(temp[0]) + Integer.parseInt(temp[2]);
					totalWordsInClass.put(temp[0], currentNum);
				}
				totalClassWords++;
			}
		}
		
		//计算先验概率
		public static void priorProbility(Configuration conf) throws IOException {
			
			Map<String, Integer> fileMap = new HashMap<>();
			int totalFiles = 0;
			FileSystem fs = FileSystem.get(URI.create(MyUnits.OUTPUT_FILES_IN_CLASS.concat("/part-r-00000")), conf);
			FSDataInputStream inputStream = fs.open(new Path(MyUnits.OUTPUT_FILES_IN_CLASS.concat("/part-r-00000")));
			BufferedReader buffer = new BufferedReader(new InputStreamReader(inputStream));
			String strLine = null;
			while((strLine = buffer.readLine()) != null) {
				String[] temp = strLine.split("\t");
				fileMap.put(temp[0], Integer.parseInt(temp[1]));
				totalFiles += Integer.parseInt(temp[1]);
			}
			for (Entry<String, Integer> entry : fileMap.entrySet()) { 
				 priorMap.put(entry.getKey(), (entry.getValue() / (double)totalFiles));
			}

		}
		
		public static void notFoundProb() {
			for (Entry<String, Integer> entry : totalWordsInClass.entrySet()) { 
				notFoundMap.put(entry.getKey(), (1.0 / (entry.getValue() + totalClassWords)));
			}	
		}
		public static Map<String, Map<String, Double>> getCondProbMap() {
			return condProbMap;
		}
		
		public static Map<String, Double> getPriorMap() {
			return priorMap;
		}
		
		public static Map<String, Double> getNotFoundMap() {
			return notFoundMap;
		}
		
	}
	
//	static class ProbReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
//		@Override
//		protected void reduce(Text arg0, Iterable<DoubleWritable> arg1,
//				Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context arg2)
//				throws IOException, InterruptedException {
//			// TODO Auto-generated method stub
//			super.reduce(arg0, arg1, arg2);
//		}
//	}
}
