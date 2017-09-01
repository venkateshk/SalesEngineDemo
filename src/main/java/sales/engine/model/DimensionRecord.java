package sales.engine.model;

import java.io.Serializable;
import java.util.Optional;

public class DimensionRecord implements Serializable {

    private String state;
    private String year;
    private String month;
    private String day;
    private String hour;

    public DimensionRecord(String state) {
        this.state = state;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public void setHour(String hour) {
        this.hour = hour;
    }

    public String getState() {
        return state;
    }

    public String getYear() {
        return year;
    }

    public String getMonth() {
        return month;
    }

    public String getDay() {
        return day;
    }

    public String getHour() {
        return hour;
    }

    @Override
    public String toString() {
        return emptyIfNull(state) + "#" + emptyIfNull(year) + "#" + emptyIfNull(month) + "#" + emptyIfNull(day) + "#" + emptyIfNull(hour);
    }

    private String emptyIfNull(String s) {
        return Optional.ofNullable(s).orElse("");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DimensionRecord that = (DimensionRecord) o;

        if (!state.equals(that.state)) return false;
        if (year != null ? !year.equals(that.year) : that.year != null) return false;
        if (month != null ? !month.equals(that.month) : that.month != null) return false;
        if (day != null ? !day.equals(that.day) : that.day != null) return false;
        return hour != null ? hour.equals(that.hour) : that.hour == null;
    }

    @Override
    public int hashCode() {
        int result = state.hashCode();
        result = 31 * result + (year != null ? year.hashCode() : 0);
        result = 31 * result + (month != null ? month.hashCode() : 0);
        result = 31 * result + (day != null ? day.hashCode() : 0);
        result = 31 * result + (hour != null ? hour.hashCode() : 0);
        return result;
    }
}