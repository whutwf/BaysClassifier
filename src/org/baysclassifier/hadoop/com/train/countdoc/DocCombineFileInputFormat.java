package org.baysclassifier.hadoop.com.train.countdoc;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

/**
 * 自定义统计类别下文件个数的CombineFileInputFormat
 * @author whutwf
 * @version 2016.12
 *
 */
public class DocCombineFileInputFormat extends CombineFileInputFormat<Text, Text>{

	//禁止文件分片
    @Override
	protected boolean isSplitable(JobContext context, Path file) {
		return false;
	}
	
	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
		return new CombineFileRecordReader<Text, Text>((CombineFileSplit)inputSplit, context, DocCombineFileRecordReader.class);
	}

}
