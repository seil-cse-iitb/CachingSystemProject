package model;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

public class Query implements Serializable {

    private static final long serialVersionUID = 0L;
    private String[] cols;

    private String sensor_id;
    private Long TS1;
    private Long TS2;
    private String granularity;


    public String[] getCols() {
        return cols;
    }

    public void setCols(String[] cols) {
        this.cols = cols;
    }

    public String getSensor_id() {
        return sensor_id;
    }

    public void setSensor_id(String sensor_id) {
        this.sensor_id = sensor_id;
    }

    public Long getTS1() {
        return TS1;
    }

    public void setTS1(Long TS1) {
        this.TS1 = TS1;
    }

    public Long getTS2() {
        return TS2;
    }

    public void setTS2(Long TS2) {
        this.TS2 = TS2;
    }

    public String getGranularity() {
        return granularity;
    }

    public void setGranularity(String granularity ) {
        this.granularity = granularity;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Query)) return false;
        Query query = (Query) o;
        return Arrays.equals(cols, query.cols) &&
                Objects.equals(sensor_id, query.sensor_id) &&
                Objects.equals(TS1, query.TS1) &&
                Objects.equals(granularity, query.granularity) &&
                Objects.equals(TS2, query.TS2);
    }

    @Override
    public int hashCode() {

        int result = Objects.hash(sensor_id, TS1, TS2,granularity);
        result = 31 * result + Arrays.hashCode(cols);
        return result;
    }

    @Override
    public String toString() {
        return "Query{" +
                "cols=" + Arrays.toString(cols) +
                ", sensor_id='" + sensor_id + '\'' +
                ", granularity='" + granularity + '\'' +
                ", TS1=" + TS1 +
                ", TS2=" + TS2 +
                '}';
    }
}
