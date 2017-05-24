package com.haozhuo.hive;

import org.apache.hive.hcatalog.streaming.DelimitedInputWriter;
import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.apache.hive.hcatalog.streaming.StreamingConnection;
import org.apache.hive.hcatalog.streaming.TransactionBatch;

import java.util.ArrayList;

/**
 * https://cwiki.apache.org/confluence/display/Hive/Streaming+Data+Ingest
 */
public class HiveHCatalog {
    public static void main(String[] args) {
        //-------   MAIN THREAD  ------- //
        String dbName = "default";
        String tblName = "alerts";
        ArrayList<String> partitionVals = new ArrayList<String>(2);
        partitionVals.add("Asia");
        partitionVals.add("India");
        String serdeClass = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";

        HiveEndPoint hiveEP = new HiveEndPoint("thrift://192.168.0.150:9083", dbName, tblName, partitionVals);

        //-------   Thread 1  -------//
        try {
            StreamingConnection connection = hiveEP.newConnection(true);
            String[] fieldNames = {"id","msg"};
            DelimitedInputWriter writer = new DelimitedInputWriter(fieldNames, ",", hiveEP);
            TransactionBatch txnBatch = connection.fetchTransactionBatch(10, writer);
            ///// Batch 1 - First TXN
            txnBatch.beginNextTransaction();
            txnBatch.write("1,Hello streaming".getBytes());
            txnBatch.write("2,Welcome to streaming".getBytes());
            txnBatch.commit();


            if(txnBatch.remainingTransactions() > 0) {
///// Batch 1 - Second TXN
                txnBatch.beginNextTransaction();
                txnBatch.write("3,Roshan Naik".getBytes());
                txnBatch.write("4,Alan Gates".getBytes());
                txnBatch.write("5,Owen O’Malley".getBytes());
                txnBatch.commit();


                txnBatch.close();
                connection.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }



    }
}
