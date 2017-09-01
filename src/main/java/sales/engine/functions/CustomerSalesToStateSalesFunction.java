package sales.engine.functions;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.joda.time.DateTime;
import sales.engine.model.CustomerRow;
import sales.engine.model.DimensionRecord;
import sales.engine.model.SalesRow;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class CustomerSalesToStateSalesFunction {

    public static PairFlatMapFunction<Tuple2<String, Tuple2<List<SalesRow>, CustomerRow>>, DimensionRecord, Double> transform() {
        return composite -> {
            List<Tuple2<DimensionRecord, Double>> results = new ArrayList<>();
            Tuple2<List<SalesRow>, CustomerRow> salesCustomerTuple = composite._2();
            List<SalesRow> salesList = salesCustomerTuple._1();
            final CustomerRow customerRow = salesCustomerTuple._2();
            salesList.forEach(salesRecord -> {
                results.addAll(emitDimMetricPair(customerRow, salesRecord));
            });
            return results.iterator();
        };
    }

    private static List<Tuple2<DimensionRecord, Double>> emitDimMetricPair(CustomerRow customerRow, SalesRow salesRecord) {
        List<Tuple2<DimensionRecord, Double>> results = new ArrayList<>();
        long ts = salesRecord.getTimeStamp();
        DateTime dt = new DateTime(ts * 1_000);
        final String yyyy = dt.toString("yyyy");
        final String mm = dt.toString("MM");
        final String dd = dt.toString("dd");
        final String hh = dt.toString("hh");

        DimensionRecord dim = new DimensionRecord(customerRow.getState());

        DimensionRecord dimY = new DimensionRecord(customerRow.getState());
        dimY.setYear(yyyy);

        DimensionRecord dimYM = new DimensionRecord(customerRow.getState());
        dimYM.setYear(yyyy);
        dimYM.setMonth(mm);

        DimensionRecord dimYMD = new DimensionRecord(customerRow.getState());
        dimYMD.setYear(yyyy);
        dimYMD.setMonth(mm);
        dimYMD.setDay(dd);

        DimensionRecord dimYMDH = new DimensionRecord(customerRow.getState());
        dimYMDH.setYear(yyyy);
        dimYMDH.setMonth(mm);
        dimYMDH.setDay(dd);
        dimYMDH.setHour(hh);

        results.add(new Tuple2<>(dim, salesRecord.getSalesPrice()));
        results.add(new Tuple2<>(dimY, salesRecord.getSalesPrice()));
        results.add(new Tuple2<>(dimYM, salesRecord.getSalesPrice()));
        results.add(new Tuple2<>(dimYMD, salesRecord.getSalesPrice()));
        results.add(new Tuple2<>(dimYMDH, salesRecord.getSalesPrice()));

        return results;
    }
}
