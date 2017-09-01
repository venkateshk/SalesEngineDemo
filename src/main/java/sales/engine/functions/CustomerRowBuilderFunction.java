package sales.engine.functions;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sales.engine.model.CustomerRow;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CustomerRowBuilderFunction implements FlatMapFunction<String, CustomerRow> {

    private static Logger LOGGER = LoggerFactory.getLogger(CustomerRowBuilderFunction.class);

    private List<String> headerColumns;


    public CustomerRowBuilderFunction(List<String> headerColumns) {
        this.headerColumns = headerColumns;
    }

    private enum CustomerDataColumns {

        CUSTOMER_ID("customer_id"),
        NAME("name"),
        STREET("street"),
        CITY("city"),
        STATE("state"),
        ZIP("zip");

        String columnName;


        CustomerDataColumns(String columnName) {
            this.columnName = columnName;
        }

        public String getColumnName() {
            return columnName;
        }
    }


    @Override
    public Iterator<CustomerRow> call(String row) throws Exception {
        String CUSTOMER_ROW_DELIMITER = "#";
        String[] rowAsArray = row.split(CUSTOMER_ROW_DELIMITER);

        if (rowAsArray.length != headerColumns.size()) {
            LOGGER.error("Column count in the row doesn't match expected value. Row = {}", row);
            System.exit(1);
        }

        CustomerRow customerRow = new CustomerRow();
        List<CustomerRow> customerRowList = new ArrayList<>();

        for (CustomerDataColumns column: CustomerDataColumns.class.getEnumConstants()) {

            int index = headerColumns.indexOf(column.getColumnName());
            if (index == -1) {
                continue;
            }
            String columnValue = rowAsArray[index];
            switch (column) {
                case CUSTOMER_ID:
                    customerRow.setCustomerID(columnValue);
                    break;
                case NAME:
                    customerRow.setName(columnValue);
                    break;
                case STREET:
                    customerRow.setStreet(columnValue);
                    break;
                case CITY:
                    customerRow.setCity(columnValue);
                    break;
                case STATE:
                    customerRow.setState(columnValue);
                    break;
                case ZIP:
                    customerRow.setZip(columnValue);
                    break;
            }
        }

        customerRowList.add(customerRow);
        return customerRowList.iterator();
    }
}