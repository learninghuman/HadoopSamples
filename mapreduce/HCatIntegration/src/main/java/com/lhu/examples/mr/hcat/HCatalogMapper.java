package com.lhu.examples.mr.hcat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hive.hcatalog.data.HCatRecord;

public class HCatalogMapper extends Mapper<WritableComparable, HCatRecord, IntWritable, IntWritable> {

	int age;

	@Override
	protected void map(WritableComparable key, HCatRecord value,
			org.apache.hadoop.mapreduce.Mapper<WritableComparable, HCatRecord, IntWritable, IntWritable>.Context context)
					throws IOException, InterruptedException {
		age = (Integer) value.get(1);
		context.write(new IntWritable(age), new IntWritable(1));
	}
}
