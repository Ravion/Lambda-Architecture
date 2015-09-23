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

package com.metlife.paymenthistory.dao;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.metlife.paymenthistory.dto.AggregateDto;
import com.metlife.paymenthistory.dto.PaymentHistoryDto;
import com.metlife.paymenthistory.dto.ResponseDto;
import com.metlife.paymenthistory.dto.UserDto;

@SuppressWarnings("deprecation")
public class PaymentDaoHbaseGet {

    public static ResponseDto getAllData() {
        try {
            Configuration config = HBaseConfiguration.create();
            config.addResource(new Path("/etc/hbase/conf/core-site.xml"));
            config.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));

            // config.set("hbase.zookeeper.quorum", "192.168.1.114");
            config.set("hbase.zookeeper.quorum", "10.20.0.199");
            config.set("hbase.zookeeper.property.clientPort", "2181");

            Map<String, BigDecimal> data = new HashMap<String, BigDecimal>();
            List<AggregateDto> aggregates = new ArrayList<AggregateDto>();

            HTable tableBatch = new HTable(config, "state_total_batch");
            Scan s = new Scan();

            long startTimeBatch = System.nanoTime();
            ResultScanner sstableBatch = tableBatch.getScanner(s);
            long endTimeBatch = System.nanoTime();
            for (Result r : sstableBatch) {
                for (KeyValue kv : r.raw()) {
                    if ((new String(kv.getFamily())).equals("totalPremium") && (new String(kv.getQualifier())).equals("amount")) {
                        BigDecimal d = new BigDecimal(new String(kv.getValue()));
                        data.put(new String(kv.getRow()), d);
                    }
                }
            }

            HTable tableStream = new HTable(config, "state_total_stream");
            long startTimeStream = System.nanoTime();
            ResultScanner ssStream = tableStream.getScanner(s);
            long endTimeStream = System.nanoTime();
            for (Result r : ssStream) {
                for (KeyValue kv : r.raw()) {
                    if ((new String(kv.getFamily())).equals("totalPremium") && (new String(kv.getQualifier())).equals("totalPremium")) {
                        BigDecimal d = new BigDecimal(new String(kv.getValue()));
                        BigDecimal sum = data.get(new String(kv.getRow())).add(d);
                        // System.out.println(Math.round(sum.divide(new
                        // BigDecimal(10000))));
                        data.put(new String(kv.getRow()), sum);
                    }
                }
            }

            for (String key : data.keySet()) {
                AggregateDto agg = new AggregateDto();
                agg.setAggregationKey(key);
                agg.setValue(data.get(key).longValue());
                aggregates.add(agg);
            }

            ResponseDto response = new ResponseDto();
            response.setAggregates(aggregates);
            response.setQueryExecutionTime((endTimeBatch + endTimeStream - startTimeBatch - startTimeStream) / 1000000);
            tableBatch.close();
            tableStream.close();
            return response;

        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseDto();
        }
    }

    public static ResponseDto getFilteredData(final String id/*
                                                              * , final String
                                                              * state
                                                              */) {
        try {

            Configuration config = HBaseConfiguration.create();
            config.addResource(new Path("/etc/hbase/conf/core-site.xml"));
            config.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));

            // config.set("hbase.zookeeper.quorum", "192.168.1.114");
            config.set("hbase.zookeeper.quorum", "10.20.0.199");
            config.set("hbase.zookeeper.property.clientPort", "2181");

            Filter filterId = new PrefixFilter(Bytes.toBytes(id + "_"));
            ResponseDto response = new ResponseDto();
            UserDto user = new UserDto();
            // user.setState(state);
            List<PaymentHistoryDto> payments = new ArrayList<PaymentHistoryDto>();

            /************** START BATCH ***************************/
            HTable tableBatch = new HTable(config, "payment_history_batch1");
            Get get = new Get(id.getBytes());
            // get.addFamily(Bytes.toBytes("transactioninfo"));
            // get.addFamily(Bytes.toBytes("personalinfo"));
            get.setMaxVersions(20);

            long startTimeBatch = System.nanoTime();
            Result rs = tableBatch.get(get);
            long endTimeBatch = System.nanoTime();


            if (rs.size() != 0) {
                String[][] aryStrings = new String[3][(rs.size() - 8) / 3];

                int i = 0;
                int j = 0;
                int k = 0;
                int setUser = 0;

                for (KeyValue kv : rs.raw()) {
                 //  if (setUser == 0) {
                        if ("Age".equals(new String(kv.getQualifier()))) {
                            user.setAge(new String(kv.getValue()));
                        } else if ("Country".equals(new String(kv.getQualifier()))) {
                            user.setCountry(new String(kv.getValue()));
                        } else if ("Fname".equals(new String(kv.getQualifier()))) {
                            user.setFirstName(new String(kv.getValue()).toUpperCase());
                        } else if ("Gender".equals(new String(kv.getQualifier()))) {
                            user.setGender(new String(kv.getValue()));
                        } else if ("Lname".equals(new String(kv.getQualifier()))) {
                            user.setLastName(new String(kv.getValue()).toUpperCase());
                        } else if ("PolicyNo".equals(new String(kv.getQualifier()))) {
                            user.setPolicyNo(new String(kv.getValue()));
                        } else if ("State".equals(new String(kv.getQualifier()))) {
                            user.setState(new String(kv.getValue()).toUpperCase());
                        } else if ("UserID".equals(new String(kv.getQualifier()))) {
                            user.setId(new String(kv.getValue()));
                          //  setUser = 1;
                        }
                   // }

                    if ("PaymentDate".equals(new String(kv.getQualifier()))) {
                        aryStrings[0][i] = new String(kv.getQualifier()) + ":" + new String(kv.getValue());
                        i++;
                    } else if ("PremiumAmount".equals(new String(kv.getQualifier()))) {
                        aryStrings[1][j] = new String(kv.getQualifier()) + ":" + new String(kv.getValue());
                        j++;
                    } else if ("TransactionType".equals(new String(kv.getQualifier()))) {
                        aryStrings[2][k] = new String(kv.getQualifier()) + ":" + new String(kv.getValue());
                        k++;
                    }
                }

                int row = 3;
                int col = (rs.size() - 8) / 3;
                int i1, j1;
                for (i1 = 0; i1 < col; i1++) {
                    PaymentHistoryDto py = new PaymentHistoryDto();
                    for (j1 = 0; j1 < row; j1++) {
                        String[] split = aryStrings[j1][i1].split(":");
                        if (split[0].equalsIgnoreCase("PaymentDate")) {
                            py.setPaymentDate(getFormattedDate(split[1]));
                        } else if (split[0].equalsIgnoreCase("PremiumAmount")) {
                            int int1 = new Integer(split[1]) / 10;
                            py.setAmount(String.valueOf(int1));
                        } else if (split[0].equalsIgnoreCase("TransactionType")) {
                            py.setTransactionType(split[1]);
                        }
                    }
                    payments.add(py);
                }
            }
            /**************************** END BATCH ***************************************/

            /**************************** START STREAM ***************************************/
            HTable tableStream = new HTable(config, "payment_history_stream");
            Scan sStream = new Scan();
            sStream.setFilter(filterId);

            long startTimeStream = System.nanoTime();
            ResultScanner sstableStream = tableStream.getScanner(sStream);
            long endTimeStream = System.nanoTime();
            for (Result r : sstableStream) {
                PaymentHistoryDto py = new PaymentHistoryDto();
                for (KeyValue kv : r.raw()) {
                    String q = new String(kv.getQualifier());
                    if ("personalinfo".equals(new String(kv.getFamily()))) {
                        if (q.equalsIgnoreCase("Fname")) {
                            user.setFirstName(new String(kv.getValue()).toUpperCase());
                        } else if (q.equalsIgnoreCase("Lname")) {
                            user.setLastName(new String(kv.getValue()).toUpperCase());
                        } else if (q.equalsIgnoreCase("Age")) {
                            user.setAge(new String(kv.getValue()));
                        } else if (q.equalsIgnoreCase("Gender")) {
                            user.setGender(new String(kv.getValue()));
                        } else if (q.equalsIgnoreCase("State")) {
                            user.setState(new String(kv.getValue()).toUpperCase());
                        } else if (q.equalsIgnoreCase("Country")) {
                            user.setCountry(new String(kv.getValue()));
                        } else if (q.equalsIgnoreCase("PolicyNo")) {
                            user.setPolicyNo(new String(kv.getValue()));
                        } else if (q.equalsIgnoreCase("UserID")) {
                            user.setId(new String(kv.getValue()));
                        }
                    } else if ("transactioninfo".equals(new String(kv.getFamily()))) {
                        if (q.equalsIgnoreCase("premiumamount")) {
                            // py.setAmount(new String(kv.getValue()));
                            int int1 = new Integer(new String(kv.getValue())) / 10;
                            // System.out.println(String.valueOf(int1));
                            py.setAmount(String.valueOf(int1));
                        } else if (q.equalsIgnoreCase("transactiondate")) {
                            py.setPaymentDate(getFormattedDate(new String(kv.getValue())));
                        } else if (q.equalsIgnoreCase("transactiontype")) {
                            py.setTransactionType(new String(kv.getValue()));
                        }
                    }
                }
                payments.add(py);
            }

            /**************************** END STREAM ***************************************/

            response.setUser(user);
            response.setPayments(payments);
            /*
             * long endTimeBatch = 0; long startTimeBatch = 0;
             */
            tableBatch.close();
            tableStream.close();
            response.setQueryExecutionTime((endTimeBatch + endTimeStream - startTimeBatch - startTimeStream) / 1000000);

            return response;

        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseDto();
        }
    }

    private static String getFormattedDate(String Date) {
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/YYYY");
        Date d = new java.util.Date(Long.parseLong(Date + "000"));
        return sdf.format(d);
    }

}
