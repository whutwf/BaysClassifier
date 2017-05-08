package org.baysclassifier.hadoop.com.train.countterm;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

/**
 * 自定义统计类别单词的CombineFileInputFormat 
 * 
 * @author whutwf
 * @version 2016.12
 */
public class TermCombineFileInputFormat extends CombineFileInputFormat<LongWritable, Text>{

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
		return new CombineFileRecordReader<LongWritable, Text>((CombineFileSplit)inputSplit, context, TermCombineFileRecordReader.class);
	}
}
