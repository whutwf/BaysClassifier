package org.baysclassifier.hadoop.com.train.countterm;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.baysclassifier.hadoop.com.help.MyUnits;

public class TermCount {
	//使用计数器，前面两个没有用好
	static enum WordsNature {
		TOTALWORDS
	}
	
	static class TermMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		private final static IntWritable one = new IntWritable(1);
		//类名 + term名
	    private Text termWithClassName = new Text();
	    
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String dirName = context.getConfiguration().get("term.input.dir.name");
			if(!MyUnits.hasDigit(value.toString()) ){
				String tcn = dirName + "\t" + value.toString();
				termWithClassName = new Text(tcn);
				context.write(termWithClassName, one);
			}
			
		}
		
	}
	
	static class TermReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			int termNum = 0;	//文件个数
	        for(IntWritable i : values){
	            termNum += i.get();
	        }
	        if (termNum == 0) {
				context.getCounter(WordsNature.TOTALWORDS).increment(1);
			}
	        context.write(key, new IntWritable(termNum));
	    }
	}
	
	public static int run(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf, "words_count");
        job.setJarByClass(TermCount.class);

        job.setInputFormatClass(TermCombineFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(TermMapper.class);
        job.setCombinerClass(TermReducer.class);
        job.setReducerClass(TermReducer.class);

		Path intFilePath = new Path(MyUnits.INPUT_TRAIN_PATH);
		Path outFilePath = new Path(MyUnits.OUTPUT_WORDS_IN_CLASS);
		//输出路径已存在删除
		outFilePath.getFileSystem(conf).delete(outFilePath, true);
		
		//使用FileStatus存储相关路径，并加入输入路径
		FileSystem fSystem = intFilePath.getFileSystem(conf);
		FileStatus[] status = fSystem.listStatus(intFilePath);
		Path[] paths = FileUtil.stat2Paths(status);
		
        for(Path path : paths) {
                 FileInputFormat.addInputPath(job, path);
        }
        FileOutputFormat.setOutputPath(job, outFilePath);
        //这里为什么执行了两遍呢
//     // 调用计数器
//     		Counters counters = job.getCounters();
//     		Counter c1 = counters.findCounter(WordsNature.TOTALWORDS);
//     		System.out.println("------------>>>>: " + c1.getDisplayName() + ":" + c1.getName() + ": " + c1.getValue());
        return job.waitForCompletion(true) ? 0 : 1;
	}
}
