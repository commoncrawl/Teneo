package org.commoncrawl.stats;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.commoncrawl.warc.WARCFileInputFormat;

import com.spiegler.index.CCIndexReducer;
import com.spiegler.index.util.CCIndexKey;
import com.spiegler.index.util.CCIndexValue;

/**
 * Statistics for Common Crawl using the response metadata (WAT) from the Common Crawl dataset.
 *
 * @author Stephen Merity (Smerity)
 */
public class WATStatsCollector extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(WATStatsCollector.class);
	
	/**
	 * Main entry point that uses the {@link ToolRunner} class to run the Hadoop job. 
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WATStatsCollector(), args);
		System.exit(res);
	}

	/**
	 * Builds and runs the Hadoop job.
	 * @return	0 if the Hadoop job completes successfully and 1 otherwise.
	 */
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		//
		conf.setBoolean("mapred.output.compress", true);
		conf.setClass("mapred.output.compression.codec", GzipCodec.class, CompressionCodec.class);
		conf.setInt("mapred.max.map.failures.percent", 10);
		//
		Job job = new Job(conf);
		job.setJarByClass(WATStatsCollector.class);
		job.setNumReduceTasks(1);
		
		String inputPath = "data/*.warc.wat.gz";
		//inputPath = "s3n://aws-publicdatasets/common-crawl/crawl-data/CC-MAIN-2013-48/segments/1386163035819/wet/CC-MAIN-20131204131715-00000-ip-10-33-133-15.ec2.internal.warc.wet.gz";
		//inputPath = "s3n://aws-publicdatasets/common-crawl/crawl-data/CC-MAIN-2013-48/segments/1386163035819/wet/*.warc.wet.gz";
		LOG.info("Input path: " + inputPath);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		
		String outputPath = "/tmp/cc/";
		FileSystem fs = FileSystem.newInstance(conf);
		if (fs.exists(new Path(outputPath))) {
			fs.delete(new Path(outputPath), true);
		}
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.setInputFormatClass(WARCFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(CCIndexKey.class);
		job.setMapOutputValueClass(CCIndexValue.class);
		job.setOutputKeyClass(CCIndexKey.class);
	    job.setOutputValueClass(CCIndexValue.class);
	    
	    job.setMapperClass(StatsMap.StatsMapper.class);
	    job.setReducerClass(CCIndexReducer.class);
	    job.setCombinerClass(CCIndexReducer.class);
		
	    if (job.waitForCompletion(true)) {
	    	return 0;
	    } else {
	    	return 1;
	    }
	}
}