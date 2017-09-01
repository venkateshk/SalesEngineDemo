package sales.engine.functions;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sales.engine.model.SalesRow;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class SalesRowReducedByCustomerIDTransformation {

    private static Logger logger = LoggerFactory.getLogger(SalesRowReducedByCustomerIDTransformation.class);

    public static JavaPairRDD<String, List<SalesRow>> transform(JavaRDD<SalesRow> salesRowJavaRDD) {

        logger.info("Entering transformation CustomerRowReducedByCustomerIDTransformation");

        final JavaPairRDD<String, List<SalesRow>> pairs = salesRowJavaRDD.mapToPair(salesRow -> {
            List<SalesRow> targetList = new ArrayList<>();
            targetList.add(salesRow);
            return new Tuple2<>(salesRow.getCustomerID(), targetList);
        });

        return pairs.reduceByKey((aRow, bRow) -> {
            aRow.addAll(bRow);
            return aRow;
        });
    }
}