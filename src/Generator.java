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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Generator {

	/** The props. */
	Properties props = new Properties();

	/** The producer. */
	KafkaProducer<String, String> producer;

	/** The sync. */
	boolean sync = false;

	/** The topic. */
	String topic;

	/** The key. */
	String key;

	/** The value. */
	String value;

	/** The producer record. */
	ProducerRecord<String, String> producerRecord;

	String firstName = null;
	String lastName = null;
	String policyNo = null;
	String userID = null;
	String[][] state = { { "Alabama", "AL" }, { "Alaska", "AK" },
			{ "Arizona", "AZ" }, { "Arkansas", "AR" }, { "California", "CA" },
			{ "Colorado", "CO" }, { "Connecticut", "CT" },
			{ "Delaware", "DE" }, { "Florida", "FL" }, { "Georgia", "GA" },
			{ "Hawaii", "HI" }, { "Idaho", "ID" }, { "Illinois", "IL" },
			{ "Indiana", "IN" }, { "Iowa", "IA" }, { "Kansas", "KS" },
			{ "Kentucky", "KY" }, { "Louisiana", "LA" }, { "Maine", "ME" },
			{ "Maryland", "MD" }, { "Massachusetts", "MA" },
			{ "Michigan", "MI" }, { "Minnesota", "MN" },
			{ "Mississippi", "MS" }, { "Missouri", "MO" }, { "Montana", "MT" },
			{ "Nebraska", "NE" }, { "Nevada", "NV" },
			{ "New Hampshire", "NH" }, { "New Jersey", "NJ" },
			{ "New Mexico", "NM" }, { "New York", "NY" },
			{ "North Carolina", "NC" }, { "North Dakota", "ND" },
			{ "Ohio", "OH" }, { "Oklahoma", "OK" }, { "Oregon", "OR" },
			{ "Pennsylvania", "PA" }, { "Rhode Island", "RI" },
			{ "South Carolina", "SC" }, { "South Dakota", "SD" },
			{ "Tennessee", "TN" }, { "Texas", "TX" }, { "Utah", "UT" },
			{ "Vermont", "VT" }, { "Virginia", "VA" }, { "Washington", "WA" },
			{ "West Virginia", "WV" }, { "Wisconsin", "WI" },
			{ "Wyoming", "WY" } };
	String[] transactionType = { "Debit", "Credit" };
	String policyType[] = { "MD", "TI" };
	String[] gender = { "M", "F" };
	Random rand = new Random();

	ArrayList<String> al = new ArrayList<String>();
	static String[][] namesList = new String[88799][];

	// takes publisher count and advertiser count, writes generated data to
	// filename.
	public void generateRecords(int pubCount, int advCount, String fileName) {

		System.out.println("Value is ::::::::::::::"
				+ props.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));

		createNamesList();
		/*
		 * File file = new File("D:/Aditya/InsuranceData/" + fileName + ".csv");
		 * FileWriter fw = null; try { fw = new
		 * FileWriter(file.getAbsoluteFile()); } catch (IOException e) {
		 * e.printStackTrace(); } BufferedWriter bw = new BufferedWriter(fw);
		 */
		int count = 0;
		int dateCount = 0;

		long paymentDate = 0;
		int MAX_DATE_COUNT = 30;
		for (int i = 1; i <= pubCount; i++) {
			for (int j = 1; j <= advCount; j++) {
				System.out.println("Count : " + count);
				/*
				 * if (count > 450000) { try { bw.close();
				 * System.out.println("Free memory : " +
				 * Runtime.getRuntime().freeMemory()); fw = new FileWriter( new
				 * File("D:/Aditya/InsuranceData/"+fileName + i + j + ".csv")
				 * .getAbsoluteFile()); bw = new BufferedWriter(fw);
				 * System.gc(); count = 0; } catch (Exception e) { } }
				 */

				getNamesList();
				int age = getAge();
				String gender = getGender();

				int paymentAmount = getPaymentAmount();
			/*	if (dateCount == 0) {
					paymentDate = getPaymentDate();

				}*/
				
				paymentDate = System.currentTimeMillis()/1000;
				
				dateCount++;
				if (dateCount == MAX_DATE_COUNT)
					dateCount = 0;

				// System.out.println("date : " + paymentDate);

				String transactionType = getTransactionType();
				String state = getState();
				String country = getCountry();

				try {

					System.out.println(userID + i + j + "," + firstName + ","
							+ lastName + "," + age + "," + gender + ","
							+ policyNo + i + "," + paymentAmount + ","
							+ paymentDate + "," + transactionType + "," + state
							+ "," + country);
					producerRecord = new ProducerRecord<String, String>(topic,
							key, userID + i + j + "," + firstName + ","
									+ lastName + "," + age + "," + gender + ","
									+ policyNo + i + "," + paymentAmount + ","
									+ paymentDate + "," + transactionType + ","
									+ state + "," + country);
					producer.send(producerRecord);
					/*
					 * bw.write(userID + i + j + "," + firstName + "," +
					 * lastName + "," + age + "," + gender + "," + policyNo + i
					 * + "," + paymentAmount + "," + paymentDate + "," +
					 * transactionType + "," + state + "," + country);
					 */

					// bw.newLine();

					count = count + 1;

				} catch (Exception e) {
					e.printStackTrace();
				}

			}
		}
		/*
		 * try { //bw.close(); } catch (IOException e) { // TODO Auto-generated
		 * catch block e.printStackTrace(); }
		 */
	}

	private void getNamesList() {
		// TODO Auto-generated method stub
		String[] name = namesList[rand.nextInt(88799)];
		firstName = name[0];
		lastName = name[1];

	}

	private void createNamesList() {
		// TODO Auto-generated method stub
		BufferedReader crunchifyBuffer = null;

		try {
			String crunchifyLine;
			//File file = ;
			crunchifyBuffer = new BufferedReader(new FileReader(ClassLoader.getSystemResource("names.csv").getFile()
					));
			
/*			crunchifyBuffer = new BufferedReader(new FileReader(
				     "/home/hadoop/names.csv"));*/

			int index = 0;
			// How to read file in java line by line?
			while ((crunchifyLine = crunchifyBuffer.readLine()) != null) {
				namesList[index++] = crunchifyLine.split(",");

			}
			/*
			 * for (int i = 0 ; i<=2;i++) { String[] name = namesList[i];
			 * System.out.println(name[0] + "==="+name[1]);
			 * 
			 * } System.out.println("Names List data : "+ namesList);
			 */

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (crunchifyBuffer != null)
					crunchifyBuffer.close();
			} catch (IOException crunchifyException) {
				crunchifyException.printStackTrace();
			}
		}
	}

	private String getState() {
		// TODO Auto-generated method stub

		String[] state1 = state[rand.nextInt(50)];
		// System.out.println(state1[0]+"====>" + state1[1]);
		getPolicyNo(state1[1]);
		return state1[0];
	}

	private String getPaymentDate() {
		// TODO Auto-generated method stub
		String date;

		int dd = rand.nextInt(7) + 1;
		int mm = random(1, 12);
		int yyyy = random(2010, 2013);
		// int hr = rand.nextInt(12) + 1;
		// int min = rand.nextInt(60) + 1;
		// int sec = rand.nextInt(60) + 1;

		switch (mm) {
		case 2:
			if (isLeapYear(yyyy)) {
				dd = random(1, 29);
			} else {
				dd = random(1, 28);
			}
			break;

		case 1:
		case 3:
		case 5:
		case 7:
		case 8:
		case 10:
		case 12:
			dd = random(1, 31);
			break;

		default:
			dd = random(1, 30);
			break;
		}

		String year = Integer.toString(yyyy);
		String month = Integer.toString(mm);
		String day = Integer.toString(dd);

		if (mm < 10) {
			month = "0" + mm;
		}

		if (dd < 10) {
			day = "0" + dd;
		}

		date = month + "-" + day + "-" + year;
		return date;

		// return "05-25-2012";
	}

	private String getCountry() {
		// TODO Auto-generated method stub
		return "USA";
	}

	private String getTransactionType() {
		// TODO Auto-generated method stub

		return transactionType[rand.nextInt(2)];
	}

	private int getPaymentAmount() {
		// TODO Auto-generated method stub
		return rand.nextInt(9000) + 1000;
		// return 1000;
	}

	private void getPolicyNo(String code) {
		// TODO Auto-generated method stub

		String pl = policyType[rand.nextInt(2)];
		if (pl.equals("MD")) {
			policyNo = pl + code + random(1000, 9999);
		} else
			policyNo = pl + code + random(100000, 999999);
		userID = "US" + pl;
	}

	private String getGender() {
		// TODO Auto-generated method stub

		return gender[rand.nextInt(2)];
	}

	private int getAge() {
		// TODO Auto-generated method stub
		return rand.nextInt(60) + 10;
	}

	public static int random(int lowerBound, int upperBound) {
		return (lowerBound + (int) Math.round(Math.random()
				* (upperBound - lowerBound)));
	}

	public static boolean isLeapYear(int year) {
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.YEAR, year);
		int noOfDays = calendar.getActualMaximum(Calendar.DAY_OF_YEAR);

		if (noOfDays > 365) {
			return true;
		}

		return false;
	}

	/**
	 * Configure kafka.
	 */
	public void configureKafka(String tpc) {
		System.out.println("Trying to initialize Kafka !");
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.118:9092");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());
		producer = new KafkaProducer<String, String>(props);

		topic = "test";
		key = "mykey";
		producerRecord = new ProducerRecord<String, String>(topic, key, value);
		System.out.println(" kafka Initialized!");
	}

}
