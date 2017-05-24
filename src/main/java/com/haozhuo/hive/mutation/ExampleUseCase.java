package com.haozhuo.hive.mutation;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.hcatalog.streaming.mutate.client.AcidTable;
import org.apache.hive.hcatalog.streaming.mutate.client.MutatorClient;
import org.apache.hive.hcatalog.streaming.mutate.client.MutatorClientBuilder;
import org.apache.hive.hcatalog.streaming.mutate.client.Transaction;
import org.apache.hive.hcatalog.streaming.mutate.worker.BucketIdResolver;
import org.apache.hive.hcatalog.streaming.mutate.worker.MutatorCoordinator;
import org.apache.hive.hcatalog.streaming.mutate.worker.MutatorCoordinatorBuilder;
import org.apache.hive.hcatalog.streaming.mutate.worker.MutatorFactory;
import java.util.List;

/**
 *
 1. Create a MutatorClient to manage a transaction for the targeted ACID tables. This set of tables should include any transactional destinations or sources. Don't forget to register a LockFailureListener so that you can handle transaction failures.
 2. Open a new Transaction with the client.
 3. Get the AcidTables from the client.
 4. Begin the transaction.
 5. Create at least one MutatorCoordinator for each table. The AcidTableSerializer can help you transport the AcidTables when your workers are in a distributed environment.
 6. Compute your mutation set (this is your ETL merge process).
 7. Optionally: collect the set of affected partitions.
 8. Append bucket ids to insertion records. A BucketIdResolver can help here.
 9. Group and sort your data appropriately.
 10.Issue mutation events to your coordinators.
 11. Close your coordinators.
 12. Abort or commit the transaction.
 13. Close your mutation client.
 14. Optionally: create any affected partitions that do not exist in the metastore.
 */
public class ExampleUseCase {
    public static void main(String[] args)throws Exception {
        String metaStoreUri = "thrift://192.168.1.150:9083";
        String databaseName = "default";
        String tableName = "users";
        boolean createPartitions = false;

        StreamingTestUtils testUtils = new StreamingTestUtils();
        HiveConf conf = testUtils.newHiveConf(metaStoreUri);

        final int RECORD_ID_COLUMN = 2;
        final int[] BUCKET_COLUMN_INDEXES = new int[] { 0 };
        MutatorFactory mutatorFactory = new ReflectiveMutatorFactory(conf, Users.class, RECORD_ID_COLUMN,
                BUCKET_COLUMN_INDEXES);
        MutatorClient client = new MutatorClientBuilder()
                .addSinkTable(databaseName, tableName,false)
                .metaStoreUri(metaStoreUri)
                .build();
        client.connect();
        Transaction transaction = client.newTransaction();

        List<AcidTable> tables = client.getTables();
        System.out.println(tables.get(0));

        transaction.begin();

        BucketIdResolver bucketIdResolver = mutatorFactory.newBucketIdResolver(tables.get(0).getTotalBuckets());
        Users record1 = (Users) bucketIdResolver.attachBucketIdToRecord(new Users(333, "222dd"));
        Users record2 = (Users) bucketIdResolver.attachBucketIdToRecord(new Users(444, "222dd"));
        System.out.println(record1);
        MutatorCoordinator coordinator = new MutatorCoordinatorBuilder()
                .metaStoreUri(metaStoreUri)
                .table(tables.get(0))
                .mutatorFactory(mutatorFactory)
                .build();
        coordinator.insert(null, record1);
        coordinator.insert(null, record2);
        coordinator.close();
        transaction.commit();
        client.close();
    }

}
