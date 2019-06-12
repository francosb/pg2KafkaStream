package com.github.pg2KafkaStream.models;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.io.Serializable;
import java.util.List;

public class SlotMessage implements Serializable {

    @SerializedName("xid")
    @Expose
    private int xid;
    @SerializedName("change")
    @Expose
    private List<Change> change = null;
    @SerializedName("nextlsn")
    @Expose
    private String nextlsn;
    @SerializedName("timestamp")
    @Expose
    private String timestamp;
    private final static long serialVersionUID = -629060839031661287L;


    public int getXid() {
        return xid;
    }

    public void setXid(int xid) {
        this.xid = xid;
    }

    public List<Change> getChange() {
        return change;
    }

    public void setChange(List<Change> change) {
        this.change = change;
    }

    public String getNextlsn() {
        return nextlsn;
    }

    public void setNextlsn(String nextlsn) {
        this.nextlsn = nextlsn;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("xid", xid)
                .append("change", change)
                .append("nextlsn", nextlsn)
                .append("timestamp", timestamp)
                .toString();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(timestamp)
                .append(change)
                .append(xid)
                .append(nextlsn).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (!(other instanceof SlotMessage)) {
            return false;
        }
        SlotMessage rhs = ((SlotMessage) other);
        return new EqualsBuilder()
                .append(timestamp, rhs.timestamp)
                .append(change, rhs.change)
                .append(xid, rhs.xid)
                .append(nextlsn, rhs.nextlsn).isEquals();
    }

    public class Change implements Serializable {

        @SerializedName("schema")
        @Expose
        private String schema;
        @SerializedName("kind")
        @Expose
        private String kind;
        @SerializedName("columnvalues")
        @Expose
        private List<String> columnvalues = null;
        private List<String> columnnames = null;
        @SerializedName("table")
        @Expose
        private String table;
        private final static long serialVersionUID = -629060839031661287L;


        public String getSchema() {
            return schema;
        }

        public void setSchema(String schema) {
            this.schema = schema;
        }

        public String getKind() {
            return kind;
        }

        public void setKind(String kind) {
            this.kind = kind;
        }

        public List<String> getColumnvalues() {
            return columnvalues;
        }

        public void setColumnvalues(List<String> columnvalues) {
            this.columnvalues = columnvalues;
        }

        public List<String> getColumnnames() {
            return columnnames;
        }

        public void setColumnnames(List<String> columnnames) {
            this.columnnames = columnnames;
        }

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("schema", schema)
                    .append("kind", kind)
                    .append("columnvalues", columnvalues)
                    .append("table", table).toString();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder()
                    .append(columnvalues)
                    .append(schema)
                    .append(table)
                    .append(columnnames)
                    .append(kind).toHashCode();
        }

        @Override
        public boolean equals(Object other) {
            if (other == this) {
                return true;
            }
            if (!(other instanceof Change)) {
                return false;
            }
            Change rhs = ((Change) other);
            return new EqualsBuilder()
                    .append(columnvalues, rhs.columnvalues)
                    .append(schema, rhs.schema)
                    .append(table, rhs.table)
                    .append(columnnames, rhs.columnnames)
                    .append(kind, rhs.kind).isEquals();
        }

    }


}

