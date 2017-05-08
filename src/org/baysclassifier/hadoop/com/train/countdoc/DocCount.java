package org.baysclassifier.hadoop.com.train.countdoc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.baysclassifier.hadoop.com.help.MyUnits;

public class DocCount {
	
	static class DocMapper extends Mapper<Text, Text, Text, IntWritable>{
		
		private final static IntWritable one = new IntWritable(1);
		//目录名
	    private Text dirName = new Text();
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			dirName = key;
			context.write(dirName, one);
		}
	}
	
	static class DocReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			int fileNum = 0;	//文件个数
	        for(IntWritable i : values){
	            fileNum += i.get();
	        }
	        context.write(key,new IntWritable(fileNum));
	    }
	}
	
	public static int run(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf, "file_count");
        job.setJarByClass(DocCount.class);

        job.setInputFormatClass(DocCombineFileInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(DocMapper.class);
        job.setCombinerClass(DocReducer.class);
        job.setReducerClass(DocReducer.class);

		Path intFilePath = new Path(MyUnits.INPUT_TRAIN_PATH);
		Path outFilePath = new Path(MyUnits.OUTPUT_FILES_IN_CLASS);
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
        return job.waitForCompletion(true) ? 0 : 1;
	}
}
