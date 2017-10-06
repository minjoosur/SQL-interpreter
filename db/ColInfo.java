package db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by minjoo on 2/20/17.
 */
public class ColInfo {
    private ArrayList<String> nameList;
    private Map<String, Node> colInfo;

    private class Node {
        private String colName;
        private String colTypeStr;
        private Class colTypeObj;
        private int colIndex;

        Node(String colName, String colTypeStr, int colIndex) {
            this.colName = colName;
            this.colTypeStr = colTypeStr;
            this.colTypeObj = typeParser(colTypeStr);
            this.colIndex = colIndex;
        }

        public String colName() {
            return colName;
        }

        public int colIndex() {
            return colIndex;
        }

        public String colTypeStr() {
            return colTypeStr;
        }

        public Class colTypeObj() {
            return colTypeObj;
        }

        Class typeParser(String colTypeStr) {
            if (colTypeStr.equals("int")) {
                return Integer.class;
            } else if (colTypeStr.equals("float")) {
                return Float.class;
            } else if (colTypeStr.equals("string")) {
                return String.class;
            } else {
                throw new RuntimeException("Invalid type: " + colTypeStr);
            }
        }
    }

    /**
     * When a user creates a Table, this will be called inside of Table constructor.
     * Table()
     */
    ColInfo() {
        nameList = new ArrayList<>();
        colInfo = new HashMap<>();
    }

    /**
     * When Table is created by Joins, this will be called inside of Table constructor.
     * Table (T1, T2)
     * @param c1 First Table's colInfo
     * @param c2 Second Table's colInfo
     */
    ColInfo(ColInfo c1, ColInfo c2) {
        this();

        for (int i = 0; i < c1.size(); i++) {
            if ( c2.colInfo.containsKey(c1.nameOfIndex(i)) ) {
                add(c1.nameOfIndex(i), c1.typeStrOfIndex(i));
            }
        }
        for(int i = 0; i < c1.size(); i++) {
            if ( !colInfo.containsKey(c1.nameOfIndex(i))) {
                add(c1.nameOfIndex(i), c1.typeStrOfIndex(i));
            }
        }
        for(int i = 0; i < c2.size(); i++) {
            if ( !colInfo.containsKey(c2.nameOfIndex(i))) {
                add(c2.nameOfIndex(i), c2.typeStrOfIndex(i));
            }
        }
    }

    ArrayList<String> nameList() {
        return nameList;
    }

    Map<String, Node> colInfo() {
        return colInfo;
    }

    public static ArrayList<String> commonCols(ColInfo c1, ColInfo c2) {
        ArrayList<String> common = new ArrayList<>();
        for(int i = 0; i < c1.size(); i++) {
            if ( c2.colInfo.containsKey(c1.nameOfIndex(i)) ) {
                common.add(c1.nameOfIndex(i));
            }
        }
        return common;
    }

    int size() {
        return colInfo.size();
    }

    void add(String colName, String colTypeStr) {
        if( colInfo.containsKey(colName) ) {
            throw new RuntimeException("Duplicate column name: " + colName);
        }
        Node node = new Node(colName, colTypeStr, size());
        nameList.add(colName);
        colInfo.put(colName, node);
        return;
    }

    int indexOfName(String colName) {
        return colInfo.get(colName).colIndex;
    }

    String nameOfIndex(int n) {
        return nameList.get(n);
    }

    String typeStrOfIndex(int n){
        return colInfo.get(nameOfIndex(n)).colTypeStr;
    }

    String typeStrOfName(String s) {return colInfo.get(s).colTypeStr;}

    public Class typeObjOfIndex(int n) {
        return colInfo.get(nameList.get(n)).colTypeObj;
    }

    public Class typeObjOfName(String colName) {
        return colInfo.get(colName).colTypeObj;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < nameList().size(); i++) {
            if(i == 0) {
                sb.append(nameList().get(i) + " " + typeStrOfIndex(i));
            } else {
                sb.append("," + nameList().get(i) + " " + typeStrOfIndex(i));
            }
        }
        return sb.toString();
    }
}
