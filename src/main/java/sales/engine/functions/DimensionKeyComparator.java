package sales.engine.functions;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import sales.engine.model.DimensionRecord;

import java.io.Serializable;
import java.util.Comparator;

public class DimensionKeyComparator implements Comparator<DimensionRecord>, Serializable {


    @Override
    public int compare(DimensionRecord record1, DimensionRecord record2) {
        return ComparisonChain.start()
                .compare(record1.getState(), record2.getState(), Ordering.natural().nullsFirst())
                .compare(record2.getYear(), record1.getYear(), Ordering.natural().nullsFirst())
                .compare(record2.getMonth(), record1.getMonth(), Ordering.natural().nullsFirst())
                .compare(record2.getDay(), record1.getDay(), Ordering.natural().nullsFirst())
                .compare(record2.getHour(), record1.getHour(), Ordering.natural().nullsFirst())
                .result();
    }
}
