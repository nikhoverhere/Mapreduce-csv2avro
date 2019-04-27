package com.nikh.csv2avro;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WriteCsvToAvro extends Configured implements Tool{

	static Schema SCHEMA;//This object represents a set of constraints that can be checked/ enforced against an XML document
	static String delim;
	static String format_date;
	
	public static class WriteCsvToAvroMap extends Mapper<LongWritable, Text, AvroKey<GenericRecord>, NullWritable> {
		
	protected void setup(Context context) throws IOException, InterruptedException {
			
			Configuration conf = context.getConfiguration();
			String schemaString = conf.get("SchemaString");
			format_date = conf.get("processedTimeStamp");
			SCHEMA = new Schema.Parser().parse(schemaString.toString());
			String delimParam = conf.get("delim");
			
			if (!delimParam.isEmpty() && delimParam != null) {
				delim = delimParam;
			} else {
				delim = "\u0001";
			}
			
		}
	
	private static GenericRecord convertToRecord(Object[] value){
			Record record = new Record(SCHEMA);
			List<Field> fields = SCHEMA.getFields();
			for (int i = 0; i < SCHEMA.getFields().size(); i++) {
				Object s = value[i];
				String fieldType = fields.get(i).schema().toString();
				if (fieldType.contains("int")) {
					try {
						record.put(i, Integer.parseInt(s.toString()));
					} catch (Exception e) {
						record.put(i, null);
					}
				} else if (fieldType.contains("long")) {
					try {
						record.put(i, Long.parseLong(s.toString()));
					} catch (Exception e) {
						record.put(i, null);
					}
				} else if (fieldType.contains("float")) {
					try {
						record.put(i, Float.parseFloat(s.toString()));
					} catch (Exception e) {
						record.put(i, null);
					}
				} else if (fieldType.contains("double")) {
					try {
						record.put(i, Double.parseDouble(s.toString()));
					} catch (Exception e) {
						record.put(i, null);
					}
				} else {
					try {
						record.put(i, s.toString());
					} catch (Exception e) {
						record.put(i, " ");
					}
				}
			}
			return record;
		}

	public void map(LongWritable key, javax.xml.soap.Text Values, Context context) throws IOException, 
						InterruptedException{
	
			String line = Values.toString();
			String modifiedrecord = line.replaceAll("\t", " ").replaceAll("\\\\", " ")
									.replaceAll("~", " ").replaceAll("'"," ");
			
			String[] arrVal = modifiedrecord.split(delim + "(?=(^\"]*\"[^\"]*\")*[^\"]*$)", -1);
			List<String> lst = new ArrayList<String>();

			
			for (int i = 0; i < arrVal.length; i++) {
				String s = arrVal[i].replaceAll("\"", "");
				String attr = s.trim();
				String shortDate = "1900-01-01";
				String longDate = "1900-01-01 00:00:00";
				SimpleDateFormat shortSdf = new SimpleDateFormat("YYYY-MM-dd");
				SimpleDateFormat longSdf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
				
				try {
					
					Date date1 = longSdf.parse(attr);
					Date date2 = longSdf.parse(longDate);
					
					if (date1.compareTo(date2) < 0) {
						lst.add(longDate);
					} 
					else {
						lst.add(attr);
					}
				} catch (ParseException e) {
	
					try {
						Date date1 = shortSdf.parse(attr);
						Date date2 = shortSdf.parse(shortDate);
						if (date1.compareTo(date2) < 0) {
							lst.add(shortDate + "00:00:00");
						} else {
							lst.add(attr + "00:00:00");
						}
					} catch (ParseException e1) {
						lst.add(attr);
					}
				}
			}
			
			Object[] attributes = new Object[SCHEMA.getFields().size() + 1];
			attributes[0] = format_date;
			
			for (int i = 0; i < lst.size(); i++) {
				attributes[i + 1] = lst.get(i);
			}
			
			context.write(new AvroKey<GenericRecord>(convertToRecord(attributes)), NullWritable.get());
		}
	}
	
	public static class IdentityReducer1 extends 
		Reducer<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, NullWritable>{
		
		public void reduce(AvroKey<GenericRecord> key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
			
			context.write(key, NullWritable.get());
		
		}
		
	}
	
	public int run(String[] args) throws Exception{
	
		if(args.length!=3){
			System.err.printf("Usage:%s[generic options] <input> <output \n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
	
		Date dt = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd HH:MM:SS");
		String  formatteddate = sdf.format(dt);
		Job job = new Job(getConf(), "Avro conversion");
		job.setJarByClass(getClass());
		Configuration conf = job.getConfiguration();
		FileInputFormat.addInputPath(job, new Path(args[1]));//FileInputFormat.setInputPaths(job, new Path(arg0[0]));
		org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, new Path(args[2]));
		SCHEMA = new Schema.Parser().parse(new File(args[0]));//Schema schema = new Parser().parse(new File("/home/skye/code/cloudera/avro/doc/examples/user.avsc"));
		
		conf.set("SchemaString", SCHEMA.toString());
		conf.set("processedTimeStamp", formatteddate);
		System.out.println("SCHEMA STRING:" +SCHEMA.toString());
		conf.set("mapred.output.compress", "true");
		conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
		conf.set("mapred.output.codec", "snappy");
		
		AvroJob.setMapOutputKeySchema(job, SCHEMA);
		AvroJob.setMapOutputValueSchema(job, Schema.create(Schema.Type.NULL));
		AvroJob.setOutputKeySchema(job, SCHEMA);
		AvroJob.setOutputValueSchema(job, Schema.create(Schema.Type.NULL));
		
		job.setMapOutputKeyClass(AvroKey.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(AvroKeyOutputFormat.class);
		job.setMapperClass(WriteCsvToAvroMap.class);
		job.setReducerClass(IdentityReducer1.class);
		
		long defaultBlockSize = 0;
		int NumOfReduce = 22;
		long inputFileLength = 0;
		
		try{
			
			FileSystem filesystem = FileSystem.get(this.getConf());
			inputFileLength = filesystem.getContentSummary(new Path(args[1])).getLength();
			defaultBlockSize = filesystem.getDefaultBlockSize();//Return the number of bytes that large input files should be optimally be split into to minimize i/o time.
			
			if(inputFileLength>0 && defaultBlockSize>0){
			
				NumOfReduce = (int)((inputFileLength/defaultBlockSize)+1)/14;
			
				if(NumOfReduce ==0){
				System.out.println("Numer of Reducres Calculated, Setting it to 1---");
				NumOfReduce = 1;
					}
				
			}
			
			job.setNumReduceTasks(NumOfReduce);
			System.out.println("Setting no of reducers to" +NumOfReduce);
			
		} 
	
		catch(Exception e)
		{
			System.out.println(e);
		}
	
	
		return job.waitForCompletion(true)?0:1;
	}

	
	public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new WriteCsvToAvro(), args);
		System.exit(res);
	}
	
	

}
