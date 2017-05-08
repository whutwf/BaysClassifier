package org.baysclassifier.hadoop.com.predict;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.baysclassifier.hadoop.com.help.MyUnits;

public class BaysPredict {

	public static int run(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf, "predict");
        job.setJarByClass(BaysPredict.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(PredictMap.class);
        job.setReducerClass(PredictReduce.class);
		Path outFilePath = new Path(MyUnits.PREDICT_RESULT);
		Path intFilePath = new Path(MyUnits.INPUT_TEST_PATH);
		FileSystem fSystem = intFilePath.getFileSystem(conf);
		//输出路径已存在删除
		outFilePath.getFileSystem(conf).delete(outFilePath, true);
//
		iteratorAddFiles(fSystem, conf, job, intFilePath);
        FileOutputFormat.setOutputPath(job, outFilePath);      
        return job.waitForCompletion(true) ? 0 : 1;
//		return 1;
	}
	 
	 public static void iteratorAddFiles(FileSystem hdfs, Configuration conf, Job job, Path path){
	        try{
	        	
	            if(hdfs == null || path == null){
	                return;
	            }
	            //获取文件列表
	            FileStatus[] files = hdfs.listStatus(path);

	            //展示文件信息
	            for (int i = 0; i < files.length; i++) {
	                try{
	                    if(files[i].isDirectory()){
	                    	FileInputFormat.addInputPath(job, files[i].getPath());
	                    }else{
	                    	FileInputFormat.addInputPath(job, files[i].getPath());
	                    }
	                }catch(Exception e){
	                    e.printStackTrace();
	                }
	            }
	          
	        }catch(Exception e){
	            e.printStackTrace();
	        }
	        
	    }


//	public static void addInputPath(Configuration conf, Job job, String inputDir) throws IOException {
//		
//		try{
//			if(inputDir == null) {
//				return;
//			}
//			Path intFilePath = new Path(inputDir);
//			FileSystem fSystem = intFilePath.getFileSystem(conf);
//			FileStatus[] status = fSystem.listStatus(intFilePath);
//			for (FileStatus stat : status) {
//				if (stat.isDirectory()) {
//					addInputPath(conf, job, stat.getPath().toString());
//				} else if(stat.isFile()){
//					FileInputFormat.addInputPath(job, stat.getPath());
//				}
//			}
//		}catch(Exception e){
//			e.printStackTrace();
//		}
//		
//		
//	}
	
	protected static class PredictMap extends Mapper<LongWritable, Text, Text, Text> {
		private static Map<String, Map<String, Double>> condProbMap = new HashMap<String, Map<String, Double>>();
		private static Map<String, Double> priorMap = new HashMap<>();
		private static Map<String, Double> notFoundMap = new HashMap<>();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			Path filePath = fileSplit.getPath();
			String fileName = filePath.getName().toString();
			String className = filePath.getParent().getName().toString();
			
			for(String clsName : priorMap.keySet()) {
				double preProb = predictProb(value.toString(), clsName);
				
				context.write(new Text(className + "\t" + fileName), new Text(clsName + "\t" + preProb));
			}
		}
		
		//初始化相关概率
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			condProbMap = BaysProbility.ProbMap.getCondProbMap();
			priorMap = BaysProbility.ProbMap.getPriorMap();
			notFoundMap = BaysProbility.ProbMap.getNotFoundMap();
		}
		
		//文档基于某个类别预测概率，log处理
		public static double predictProb(String content, String className) {
			Map<String, Double> conProb = condProbMap.get(className);
			double pripor = priorMap.get(className);
			//预测概率初始化先验概率
			double predictProb = Math.log(pripor);
			double notFoundOfClass = notFoundMap.get(className);
			//每行处理
			StringTokenizer iTokenizer = new StringTokenizer(content);
			while(iTokenizer.hasMoreTokens()) {
				String word = iTokenizer.nextToken();
//				if(!MyUnits.hasDigit(word)){
					if(conProb.containsKey(word)) {
						predictProb += Math.log(conProb.get(word));
					} else {
						predictProb += Math.log(notFoundOfClass);
					}
//				}
				
			}
			return predictProb;
			
		}
	}
	
	protected static class PredictReduce extends Reducer<Text, Text, Text, Text> {
		Map<String, Double> resultMap = new HashMap<>();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			double maxP = -Double.MAX_VALUE;
			String predictClass = "";
			for(Text value : values) {
				String[] temp = value.toString().split("\t");
				double preP = Double.parseDouble(temp[1]);
				
				if(!resultMap.containsKey(temp[0])) {
					resultMap.put(temp[0], preP);
				} else {
					resultMap.put(temp[0], resultMap.get(temp[0]) + preP);
				}
			}
			System.out.println(resultMap);
			for(Map.Entry<String, Double> entry : resultMap.entrySet()){
				if(entry.getValue() > maxP) {
					maxP = entry.getValue();		
					predictClass = entry.getKey();
				}
			}
			System.out.println(key + "\t" + predictClass);
			context.write(key, new Text(predictClass));
		}
	} 
}
