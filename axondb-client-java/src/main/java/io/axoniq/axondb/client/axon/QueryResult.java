package io.axoniq.axondb.client.axon;

import io.axoniq.axondb.QueryValue;
import io.axoniq.axondb.grpc.RowResponse;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Author: marc
 */
public class QueryResult  {
    private final static QueryValue DEFAULT = QueryValue.newBuilder().build();
    private final RowResponse rowResponse;
    private final List<String> columns;

    public QueryResult(RowResponse nextItem, List<String> columns) {
        rowResponse = nextItem;
        this.columns = columns;
    }

    public Object get(String name) {
        return unwrap(rowResponse.getValuesOrDefault(name, DEFAULT));
    }

    public List<Object> getIdentifiers() {
        if(rowResponse.getIdValuesCount() == 0) return null;
        return rowResponse.getIdValuesList().stream().map(this::unwrap).collect(Collectors.toList());
    }

    public List<Object> getSortValues() {
        if(rowResponse.getSortValuesCount() == 0) return null;
        return rowResponse.getSortValuesList().stream().map(this::unwrap).collect(Collectors.toList());
    }

    public List<String> getColumns() {
        return columns;
    }

    private Object unwrap(QueryValue value) {
        switch (value.getDataCase()) {
            case TEXT_VALUE:
                return value.getTextValue();
            case NUMBER_VALUE:
                return value.getNumberValue();
            case BOOLEAN_VALUE:
                return value.getBooleanValue();
            case DOUBLE_VALUE:
                return value.getDoubleValue();
            default:
                return null;
        }
    }

    public String toString() {
        return columns.stream().map(col -> col + "=" + get(col)).collect(Collectors.joining(","));
    }
}
