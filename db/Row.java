package db;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by minjoo on 2/20/17.
 */
 public class Row {
     private Table table;
    private Map<String, Object> row;

    public Row(Table table) {
        this.table = table;
        row = new HashMap<>();
    }

    protected void addElemWithColName(String colName, Object value) {
        Object obj = "";
        if ("NOVALUE".equals(value)) {
            obj = value;
        } else if ("NaN".equals(value)){
            obj = value;
        } else if ("int".equals(table().cols().typeStrOfIndex(size()))) {
            obj = Integer.valueOf(value.toString());
        } else if ("float".equals(table().cols().typeStrOfIndex(size()))) {
            obj = Float.valueOf(value.toString());
        } else if ("string".equals(table().cols().typeStrOfIndex(size()))) {
            obj = value;
        } else {
            throw new RuntimeException("Malformed row entry: " + value); // TODO : HANDLE THEM IN BETTER WAY
        }
        row.put(colName, obj);
    }

    public boolean containsCol(String colName) {
        return row.containsKey(colName);
    }

    public Table table() {
        return table;
    }

    public int size() {
        return row.size();
    }

//    public String getNameOf(int i) {
//        return table().nameOf(i);
//    }

    public Object getValueByColName(String s) {
        return row.get(s);
    }

    public Object getValueByColIndex(int i) {
        return row.get(table().nameOfColIndex(i));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size(); i++) {
            if(i == 0) {
                if(getValueByColIndex(i).equals("NaN") || getValueByColIndex(i).equals("NOVALUE")) {
                    sb.append(getValueByColIndex(i));
                } else if (table().cols().typeStrOfIndex(i).equals("float")) {
                    String castedFloat = String.format("%.3f", (float) getValueByColIndex(i));
                    sb.append(castedFloat);
                } else {
                    sb.append(getValueByColIndex(i));
                }
            } else {
                if(getValueByColIndex(i).equals("NaN") || getValueByColIndex(i).equals("NOVALUE")) {
                    sb.append("," + getValueByColIndex(i));
                } else if(table().cols().typeStrOfIndex(i).equals("float")) {
                    String castedFloat = String.format("%.3f", (Float) getValueByColIndex(i));
                    sb.append("," + castedFloat);
                } else {
                    sb.append("," + getValueByColIndex(i));
                }
            }
        }
        return sb.toString();
    }
}
