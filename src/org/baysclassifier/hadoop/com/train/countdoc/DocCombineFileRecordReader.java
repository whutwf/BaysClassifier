package org.baysclassifier.hadoop.com.train.countdoc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;


/**
 * 自定义DocCombineFileRecordReader类， 读取文件切片
 * @author whutwf
 * @version 2016.12
 *
 */
public class DocCombineFileRecordReader extends RecordReader<Text, Text>{

	private CombineFileSplit combineFileSplit;	//当前处理的分片
	private Configuration conf;
	
	private Text key = new Text();	//读入key
	private Text value = new Text();	//读入value
	
	private int totalLength;	//分片中文件的数量
	private int currentIndex;	//读取文件的位置索引
	private float currentProcess = 0F;	//当前进度
	private boolean processed = false;	//标记文件是否处理
	
	public DocCombineFileRecordReader(CombineFileSplit combineFileSplit, TaskAttemptContext context, Integer index) {
		super();

		this.combineFileSplit = combineFileSplit;
		this.currentIndex = index;	//当前处理小文件在Block在
		this.conf = context.getConfiguration();
		this.totalLength = combineFileSplit.getPaths().length;
	}
	
	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(!processed){
	
			Path file = combineFileSplit.getPath(currentIndex);
			key.set(file.getParent().getName());
			value.set(file.getName());
			processed = true;
			return true;
		}
		return false;
	}
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
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
