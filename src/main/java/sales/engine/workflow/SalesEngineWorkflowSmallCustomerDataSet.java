package sales.engine.workflow;

import com.google.common.collect.ImmutableList;
import com.typesafe.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sales.engine.config.ConfigHelper;
import sales.engine.config.ConfigProperties;
import sales.engine.functions.*;
import sales.engine.model.CustomerRow;
import sales.engine.model.DimensionRecord;
import sales.engine.model.SalesRow;
import scala.Tuple2;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class SalesEngineWorkflowSmallCustomerDataSet {

    private static Logger LOGGER = LoggerFactory.getLogger(SalesEngineWorkflowSmallCustomerDataSet.class);

    private static final int TUNING_NUMBER_PARTITIONS = 7;

    public static void main(String[] args) throws IOException {

        if (args.length != 1 || !"dev".equals(args[0])) {
            LOGGER.error("Program argument should be 'dev'");
            System.exit(1);
        }

        ConfigHelper configHelper = new ConfigHelper(args);
        Config config = configHelper.getConfig();

        final String master = config.getString(ConfigProperties.SPARK_MASTER.getValue());
        final String sparkAppName = config.getString(ConfigProperties.SPARK_APP_NAME.getValue());

        final String salesData = config.getString(ConfigProperties.SALES_INPUT.getValue());
        final String customersData = config.getString(ConfigProperties.CUSTOMERS_INPUT.getValue());

        final String tempOutputData = config.getString(ConfigProperties.TEMP_OUTPUT.getValue());
        final String salesByStateOutputData = config.getString(ConfigProperties.SALES_BY_STATE_OUTPUT.getValue());

        final int tuningPartitionsCount = config.getInt(ConfigProperties.TUNING_NUMBER_OF_PARTITIONS.getValue());

        final Partitioner partitioner = new HashPartitioner(tuningPartitionsCount);

        SparkConf conf = new SparkConf()
                .setAppName(sparkAppName)
                .setMaster(master);

        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> customerRecordRDD = context.textFile(customersData).persist(StorageLevel.MEMORY_AND_DISK_SER());
        final ImmutableList<String> customerDataHeaders = ImmutableList.of("customer_id", "name", "street", "city", "state", "zip");
        final JavaRDD<CustomerRow> customerRowJavaRDD = customerRecordRDD.flatMap(new CustomerRowBuilderFunction(customerDataHeaders));
        final JavaPairRDD<String, CustomerRow> customerDf = customerRowJavaRDD.mapToPair(customerRow -> new Tuple2<>(customerRow.getCustomerID(), customerRow));

        final Broadcast<Map<String, CustomerRow>> customersBroadcasted = context.broadcast(customerDf.collectAsMap());

        final JavaRDD<String> salesRecordRDD = context.textFile(salesData).persist(StorageLevel.MEMORY_AND_DISK_SER());
        final ImmutableList<String> salesDataHeaders = ImmutableList.of("timestamp", "customer_id", "sales_price");
        final JavaRDD<SalesRow> salesRowJavaRDD = salesRecordRDD.flatMap(new SalesRowBuilderFunction(salesDataHeaders));
        final JavaPairRDD<String, List<SalesRow>> salesDf = SalesRowReducedByCustomerIDTransformation.transform(salesRowJavaRDD).persist(StorageLevel.MEMORY_AND_DISK_SER());

        // broadcast variable
        final JavaPairRDD<String, Tuple2<List<SalesRow>, CustomerRow>> customersSalesJoinRDD = salesDf.mapToPair((PairFunction<Tuple2<String, List<SalesRow>>, String, Tuple2<List<SalesRow>, CustomerRow>>) tuple2 -> {
            String custId = tuple2._1();
            final List<SalesRow> salesRows = tuple2._2();
            final CustomerRow customerRow = customersBroadcasted.value().get(custId);
            return new Tuple2<>(custId, new Tuple2<>(salesRows, customerRow));
        });


        final JavaPairRDD<DimensionRecord, Double> stateSalesPairRDD = customersSalesJoinRDD.flatMapToPair(CustomerSalesToStateSalesFunction.transform()).persist(StorageLevel.MEMORY_AND_DISK_SER());

        final JavaPairRDD<DimensionRecord, Double> stateSalesPairReducedRDD = stateSalesPairRDD.reduceByKey(partitioner, (s1, s2) -> s1 + s2).persist(StorageLevel.MEMORY_AND_DISK_SER());

        final JavaPairRDD<DimensionRecord, Double> sorted = stateSalesPairReducedRDD.sortByKey(new DimensionKeyComparator());

        final JavaRDD<String> result = sorted.map((Function<Tuple2<DimensionRecord, Double>, String>) v1 -> v1._1().toString() + "#" + v1._2());

        LOGGER.info("Lineage" + result.toDebugString());

        result.saveAsTextFile(tempOutputData);

        Configuration configuration = new Configuration();
        FileSystem hdfs = FileSystem.get(configuration);
        FileUtil.copyMerge(hdfs, new Path(tempOutputData), hdfs, new Path(salesByStateOutputData), false, configuration, null);

    }
}