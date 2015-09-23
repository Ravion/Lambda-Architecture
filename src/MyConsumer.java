/*#
	        # Copyright 2015 Xoriant Corporation.
	        #
	        # Licensed under the Apache License, Version 2.0 (the "License");
	        # you may not use this file except in compliance with the License.
	        # You may obtain a copy of the License at
	        #
	        #     http://www.apache.org/licenses/LICENSE-2.0
	
	        #
	        # Unless required by applicable law or agreed to in writing, software
	        # distributed under the License is distributed on an "AS IS" BASIS,
	        # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	        # See the License for the specific language governing permissions and
	        # limitations under the License.
	        #
*/

package com.xoriant.kafkaProducer;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import kafka.serializer.StringDecoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.protobuf.generated.CellProtos.KeyValueOrBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple11;
import scala.Tuple2;

public class MyConsumer {
	private final static String stateTable = "state_total_stream";
	String dstPath = "hdfs://192.168.1.114/user/hadoop/StreamingDataMerged/";

	public static void main(String[] args) throws IOException {
		// System.setProperty("spark.executor.memory", "8g");
		System.setProperty("spark.serializer",
				"org.apache.spark.serializer.KryoSerializer");
		// Create the context with a 1 second batch size
		SparkConf sparkConf = new SparkConf();
		// final Configuration config = new Configuration();
		Configuration hadoopConfig = new Configuration();
		hadoopConfig.set("mapreduce.output.textoutputformat.separator", ",");
		sparkConf.setMaster("local[2]");
		sparkConf.setAppName("Insurance");
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

		JavaStreamingContext javaStreamingContext = new JavaStreamingContext(
				javaSparkContext, new Duration(500));

		int numThreads = Integer.parseInt(args[3]);
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		String[] topics = args[2].split(",");
		for (String topic : topics) {
			topicMap.put(topic, numThreads);
		}

		// 3. create connection with HBase
		Configuration config = null;

		try {
			config = HBaseConfiguration.create();
			config.set("hbase.zookeeper.quorum", "192.168.1.114");
			config.set("hbase.zookeeper.property.clientPort", "2181");

			// config.set("mapreduce.job.output.key.class",
			// Text.class.getName());
			// config.set("mapreduce.job.output.value.class",
			// IntWritable.class.getName());
			// config.set("mapreduce.outputformat.class" ,
			// TableOutputFormat.class.getName());
			// config.set("hbase.master", "127.0.0.1:60000");
			HBaseAdmin.checkHBaseAvailable(config);

			System.out.println("HBase is running!");
		} catch (MasterNotRunningException e) {
			System.out.println("HBase is not running!");
			System.exit(1);
		} catch (Exception ce) {
			System.out.println("here.....");
			ce.printStackTrace();
		}

		// config.set(TableInputFormat.INPUT_TABLE, rawTableName);

		// 4. new Hadoop API configuration
		final Job newAPIJobConfigurationState = Job.getInstance(config);
		newAPIJobConfigurationState.getConfiguration().set(
				TableOutputFormat.OUTPUT_TABLE, stateTable);
		newAPIJobConfigurationState
		.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);

		final Job newAPIJobConfigurationUser = Job.getInstance(config);
		newAPIJobConfigurationUser.getConfiguration().set(
				TableOutputFormat.OUTPUT_TABLE, "user_total_stream");
		newAPIJobConfigurationUser
		.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);

		final Job paymentHistoryConfig = Job.getInstance(config);
		paymentHistoryConfig.getConfiguration().set(
				TableOutputFormat.OUTPUT_TABLE, "payment_history_stream");
		paymentHistoryConfig
		.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
		/*
		 * Set<String> topics = new HashSet<String>(); topics.add("test");
		 * 
		 * 
		 * Map<String, String> kafkaParams = new HashMap<String, String>();
		 * kafkaParams.put("metadata.broker.list", "10.20.0.199:9092");
		 */
		/*
		 * JavaPairInputDStream<String, String> stream = KafkaUtils
		 * .createDirectStream(javaStreamingContext, String.class, String.class,
		 * StringDecoder.class, StringDecoder.class, kafkaParams, topics);
		 */

		JavaPairReceiverInputDStream<String, String> stream = KafkaUtils
				.createStream(javaStreamingContext, args[0], args[1], topicMap);

		System.out.println("Got my DStream! connecting to zookeeper " + args[0]
				+ " group " + args[1] + " topics" + topicMap);

		stream.count().print();


		JavaDStream<Tuple11<String, String, String, String, String, String, String, String, String, String, String>> records = stream
				.map(new Function<Tuple2<String, String>, Tuple11<String, String, String, String, String, String, String, String, String, String, String>>() {

					private static final long serialVersionUID = 1L;

					public Tuple11<String, String, String, String, String, String, String, String, String, String, String> call(
							Tuple2<String, String> defaultKeyAndRecords)
									throws Exception {

						String[] fields = defaultKeyAndRecords._2().split(",");

						return new Tuple11<String, String, String, String, String, String, String, String, String, String, String>(
								fields[0], fields[1], fields[2], fields[3],
								fields[4], fields[5], fields[6], fields[7],
								fields[8], fields[9], fields[10]);
					}
				});

		records.foreachRDD(new Function<JavaRDD<Tuple11<String,String,String,String,String,String,String,String,String,String,String>>, Void>() {
			private static final long serialVersionUID = -3333697808496161495L;

			public Void call(
					JavaRDD<Tuple11<String, String, String, String, String, String, String, String, String, String, String>> rdd)
							throws Exception {
				saveToHBasePaymentHistory(rdd,
						paymentHistoryConfig.getConfiguration());
				return null;
			}
		});

		JavaPairDStream<String, String> window = records
				.mapToPair(
						new PairFunction<Tuple11<String, String, String, String, String, String, String, String, String, String, String>, String, String>() {

							private static final long serialVersionUID = -8849699432349098738L;

							public Tuple2<String, String> call(
									Tuple11<String, String, String, String, String, String, String, String, String, String, String> arg0)
											throws Exception {

								String str = arg0._2() + "," + arg0._3() + ","
										+ arg0._4() + "," + arg0._5() + ","
										+ arg0._6() + "," + arg0._7() + ","
										+ arg0._8() + "," + arg0._9() + ","
										+ arg0._10() + "," + arg0._11();

								return new Tuple2<String, String>(arg0._1(),
										str);
							}
						}).window(new Duration(60000), new Duration(60000));

		
		window.saveAsNewAPIHadoopFiles(
				"hdfs://192.168.1.114/user/hadoop/StreamingData/Insurancedata",
				"", Text.class, Text.class, TextOutputFormat.class,
				hadoopConfig);

		JavaPairDStream<String, Integer> recordsMapState = records
				.mapToPair(new PairFunction<Tuple11<String, String, String, String, String, String, String, String, String, String, String>, String, Integer>() {
					private static final long serialVersionUID = 1L;

					public Tuple2<String, Integer> call(
							Tuple11<String, String, String, String, String, String, String, String, String, String, String> arg0)
									throws Exception {
						String key = arg0._10();
						Integer value = new Integer(arg0._7());

						return new Tuple2<String, Integer>(key, value);
					}

				});

		JavaPairDStream<String, Integer> recordsMapUser = records
				.mapToPair(new PairFunction<Tuple11<String, String, String, String, String, String, String, String, String, String, String>, String, Integer>() {
					private static final long serialVersionUID = 1L;

					public Tuple2<String, Integer> call(
							Tuple11<String, String, String, String, String, String, String, String, String, String, String> arg0)
									throws Exception {
						String key = arg0._1();
						Integer value = new Integer(arg0._7());

						return new Tuple2<String, Integer>(key, value);
					}

				});

		JavaPairDStream<String, Integer> reduceByKeyAndWindowState = recordsMapState
				.reduceByKeyAndWindow(
						new Function2<Integer, Integer, Integer>() {
							private static final long serialVersionUID = 197675516004789269L;

							public Integer call(Integer val1, Integer val2)
									throws Exception {
								return val1 + val2;

							}
						}, new Duration(86400000), new Duration(10000));

		JavaPairDStream<String, Integer> reduceByKeyAndWindowUser = recordsMapUser
				.reduceByKeyAndWindow(
						new Function2<Integer, Integer, Integer>() {
							private static final long serialVersionUID = 197675516004789269L;

							public Integer call(Integer val1, Integer val2)
									throws Exception {
								return val1 + val2;

							}
						}, new Duration(86400000), new Duration(60000));

		// reduce.count();
		reduceByKeyAndWindowState.print();

		reduceByKeyAndWindowState
		.foreachRDD(new Function<JavaPairRDD<String, Integer>, Void>() {
			private static final long serialVersionUID = 8534726505385048702L;

			public Void call(JavaPairRDD<String, Integer> rdd)
					throws Exception {
				saveToHBase(rdd,
						newAPIJobConfigurationState.getConfiguration());
				return null;
			}
		});

		reduceByKeyAndWindowUser
		.foreachRDD(new Function<JavaPairRDD<String, Integer>, Void>() {
			private static final long serialVersionUID = 8534726505385048702L;

			public Void call(JavaPairRDD<String, Integer> rdd)
					throws Exception {
				saveToHBase(rdd,
						newAPIJobConfigurationUser.getConfiguration());
				return null;
			}
		});

		javaStreamingContext.start();
		javaStreamingContext.awaitTermination();
	}

	// 6. saveToHBase method - insert data into HBase

	protected static void saveToHBasePaymentHistory(
			JavaRDD<Tuple11<String, String, String, String, String, String, String, String, String, String, String>> rdd,
			Configuration configuration) {
		JavaPairRDD<ImmutableBytesWritable, Put> hbasePut = rdd
				.mapToPair(new PairFunction<Tuple11<String,String,String,String,String,String,String,String,String,String,String>, ImmutableBytesWritable, Put>() {
					private static final long serialVersionUID = -6047304672317854321L;

					public Tuple2<ImmutableBytesWritable, Put> call(
							Tuple11<String,String,String,String,String,String,String,String,String,String,String> arg0) throws Exception {

						//String[] values = arg0._2().split(",");
						Put put = new Put(Bytes.toBytes(arg0._1().toString()
								+ "_" + arg0._8()));
						put.add(Bytes.toBytes("personalinfo"),
								Bytes.toBytes("userid"),
								Bytes.toBytes(arg0._1().toString()));
						put.add(Bytes.toBytes("personalinfo"),
								Bytes.toBytes("fname"),
								Bytes.toBytes(arg0._2()));
						put.add(Bytes.toBytes("personalinfo"),
								Bytes.toBytes("lname"),
								Bytes.toBytes(arg0._3()));
						put.add(Bytes.toBytes("personalinfo"),
								Bytes.toBytes("age"), Bytes.toBytes(arg0._4()));
						put.add(Bytes.toBytes("personalinfo"),
								Bytes.toBytes("gender"),
								Bytes.toBytes(arg0._5()));
						put.add(Bytes.toBytes("personalinfo"),
								Bytes.toBytes("policyno"),
								Bytes.toBytes(arg0._6()));
						put.add(Bytes.toBytes("transactioninfo"),
								Bytes.toBytes("premiumamount"),
								Bytes.toBytes(arg0._7()));
						put.add(Bytes.toBytes("transactioninfo"),
								Bytes.toBytes("transactiondate"),
								Bytes.toBytes(arg0._8()));
						put.add(Bytes.toBytes("transactioninfo"),
								Bytes.toBytes("transactiontype"),
								Bytes.toBytes(arg0._9()));
						put.add(Bytes.toBytes("personalinfo"),
								Bytes.toBytes("state"),
								Bytes.toBytes(arg0._10()));
						put.add(Bytes.toBytes("personalinfo"),
								Bytes.toBytes("country"),
								Bytes.toBytes(arg0._11()));
						return new Tuple2<ImmutableBytesWritable, Put>(
								new ImmutableBytesWritable(), put);
					}


				});
		hbasePut.saveAsNewAPIHadoopDataset(configuration);

	}

	protected static void saveToHBase(JavaPairRDD<String, Integer> rdd,
			Configuration configuration) {
		JavaPairRDD<ImmutableBytesWritable, Put> hbasePut = rdd
				.mapToPair(new PairFunction<Tuple2<String, Integer>, ImmutableBytesWritable, Put>() {
					private static final long serialVersionUID = -6775464596184182780L;

					public Tuple2<ImmutableBytesWritable, Put> call(
							Tuple2<String, Integer> arg0) throws Exception {

						Put put = new Put(Bytes.toBytes(arg0._1().toString()));
						// put.add(Bytes.toBytes("total"),
						// Bytes.toBytes("stateName"),
						// Bytes.toBytes(arg0._1().toString()));
						put.add(Bytes.toBytes("totalPremium"),
								Bytes.toBytes("totalPremium"),
								Bytes.toBytes(arg0._2().toString()));
						return new Tuple2<ImmutableBytesWritable, Put>(
								new ImmutableBytesWritable(), put);
					}
				});
		hbasePut.saveAsNewAPIHadoopDataset(configuration);

	}

	/*protected static void saveToHBasePaymentHistory(
			JavaPairRDD<String, String> rdd, Configuration configuration) {
		JavaPairRDD<ImmutableBytesWritable, Put> hbasePut = rdd
				.mapToPair(new PairFunction<Tuple2<String, String>, ImmutableBytesWritable, Put>() {
					private static final long serialVersionUID = -6047304672317854321L;

					public Tuple2<ImmutableBytesWritable, Put> call(
							Tuple2<String, String> arg0) throws Exception {
						String[] values = arg0._2().split(",");
						Put put = new Put(Bytes.toBytes(arg0._1().toString()
								+ "_" + values[6]));
						// Put put = new
						// Put(Bytes.toBytes(System.currentTimeMillis()));
						put.add(Bytes.toBytes("personalinfo"),
								Bytes.toBytes("userid"),
								Bytes.toBytes(arg0._1().toString()));
						put.add(Bytes.toBytes("personalinfo"),
								Bytes.toBytes("fname"),
								Bytes.toBytes(values[0]));
						put.add(Bytes.toBytes("personalinfo"),
								Bytes.toBytes("lname"),
								Bytes.toBytes(values[1]));
						put.add(Bytes.toBytes("personalinfo"),
								Bytes.toBytes("age"), Bytes.toBytes(values[2]));
						put.add(Bytes.toBytes("personalinfo"),
								Bytes.toBytes("gender"),
								Bytes.toBytes(values[3]));
						put.add(Bytes.toBytes("personalinfo"),
								Bytes.toBytes("policyno"),
								Bytes.toBytes(values[4]));
						put.add(Bytes.toBytes("transactioninfo"),
								Bytes.toBytes("premiumamount"),
								Bytes.toBytes(values[5]));
						put.add(Bytes.toBytes("transactioninfo"),
								Bytes.toBytes("transactiondate"),
								Bytes.toBytes(values[6]));
						put.add(Bytes.toBytes("transactioninfo"),
								Bytes.toBytes("transactiontype"),
								Bytes.toBytes(values[7]));
						put.add(Bytes.toBytes("personalinfo"),
								Bytes.toBytes("state"),
								Bytes.toBytes(values[8]));
						put.add(Bytes.toBytes("personalinfo"),
								Bytes.toBytes("country"),
								Bytes.toBytes(values[9]));
						return new Tuple2<ImmutableBytesWritable, Put>(
								new ImmutableBytesWritable(), put);
					}
				});
		hbasePut.saveAsNewAPIHadoopDataset(configuration);

	}*/

	/*	protected static void merge(String srcPath, String dstPath)
			throws IOException {
		Configuration config = new Configuration();
		config.set("mapreduce.output.textoutputformat.separator", ",");
		FileUtil.fullyDelete(new File(dstPath));
		FileUtil.fullyDelete(new File(srcPath));
		FileSystem hdfs = FileSystem.get(config);
		FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath),
				true, config, null);

	}*/
}
