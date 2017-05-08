package org.baysclassifier.hadoop.com.train.countterm;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

/**
 * 自定义统计类别单词CombineFileRecordReader类
 * 
 * @author whutwf
 * @version 2016.12
 *
 */
public class TermCombineFileRecordReader extends RecordReader<LongWritable, Text>{

	private CombineFileSplit combineFileSplit;	//当前处理的分片
	private LineRecordReader lineRecordReader = new LineRecordReader();	//使用默认的逐行读取
	
	private LongWritable key = new LongWritable();	//读入key
	private Text value = new Text();	//读入value
	
	private int totalLength;	//分片中文件的数量
	private int currentIndex;	//读取文件的位置索引
	private float currentProcess = 0F;	//当前进度
	
	public TermCombineFileRecordReader(CombineFileSplit combineFileSplit, TaskAttemptContext context, Integer index) {
		super();
		this.combineFileSplit = combineFileSplit;
		this.currentIndex = index;	//当前处理小文件在Block在CombileFileSplit中索引
		this.totalLength = combineFileSplit.getPaths().length;
		//用于获取term文件名，以及所属类别名
		context.getConfiguration().set("term.input.file.name", combineFileSplit.getPath(currentIndex).getName());
		context.getConfiguration().set("term.input.dir.name", combineFileSplit.getPath(currentIndex).getParent().getName());
	}
	
	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
		combineFileSplit = (CombineFileSplit)inputSplit;
		//处理CombineSplit的小文件，构造FileSplit使用LineRecordReader
		FileSplit fileSplit = new FileSplit(combineFileSplit.getPath(currentIndex),
				combineFileSplit.getOffset(currentIndex), combineFileSplit.getLength(currentIndex),
				combineFileSplit.getLocations());
		lineRecordReader.initialize(fileSplit, context);	
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(currentIndex >= 0 && currentIndex < totalLength){
			return lineRecordReader.nextKeyValue();	//读取下一行
		}
		return false;
	}
	@Override
	public void close() throws IOException {
		lineRecordReader.close();	
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		key = lineRecordReader.getCurrentKey();
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		value = lineRecordReader.getCurrentValue();
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if(currentIndex >= 0 && currentIndex < totalLength){
			return currentProcess = (float)(currentIndex / totalLength);
		}
		return currentProcess;
	}

}
