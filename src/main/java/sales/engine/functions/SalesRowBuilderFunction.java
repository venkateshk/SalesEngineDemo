package sales.engine.functions;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sales.engine.model.SalesRow;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SalesRowBuilderFunction implements FlatMapFunction<String, SalesRow> {

    private static Logger LOGGER = LoggerFactory.getLogger(SalesRowBuilderFunction.class);

    private List<String> headerColumns;


    public SalesRowBuilderFunction(List<String> headerColumns) {
        this.headerColumns = headerColumns;
    }

    private enum SalesDataColumns {

        TIMESTAMP("timestamp"),
        CUSTOMER_ID("customer_id"),
        SALES_PRICE("sales_price");

        String columnName;


        SalesDataColumns(String columnName) {
            this.columnName = columnName;
        }

        public String getColumnName() {
            return columnName;
        }
    }


    @Override
    public Iterator<SalesRow> call(String row) throws Exception {
        String SALES_ROW_DELIMITER = "#";
        String[] rowAsArray = row.split(SALES_ROW_DELIMITER);

        if (rowAsArray.length != headerColumns.size()) {
            LOGGER.error("Column count in the row doesn't match expected value. Row = {}", row);
            System.exit(1);
        }

        SalesRow salesRow = new SalesRow();
        List<SalesRow> salesRowList = new ArrayList<>();

        for (SalesDataColumns column: SalesDataColumns.class.getEnumConstants()) {

            int index = headerColumns.indexOf(column.getColumnName());
            if (index == -1) {
                continue;
            }
            String columnValue = rowAsArray[index];
            switch (column) {
                case TIMESTAMP:
                    salesRow.setTimeStamp(Long.parseLong(columnValue));
                    break;
                case CUSTOMER_ID:
                    salesRow.setCustomerID(columnValue);
                    break;
                case SALES_PRICE:
                    salesRow.setSalesPrice(Double.parseDouble(columnValue));
                    break;
            }
        }

        salesRowList.add(salesRow);
        return salesRowList.iterator();
    }
}