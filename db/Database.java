package db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.FileOutputStream;

public class Database {
    private Map<String, Table> tableList;

    public Database() {
        tableList = new HashMap<>();
    }

    public String transact(String query) {
        try {
            query = Parse.eval(this, query);
        } catch (RuntimeException e) {
            query = "ERROR: " + e.getMessage();
        }
        return query;
    }

    protected Table getTable(String tableName) {
        if (tableList.containsKey(tableName)) {
            return tableList.get(tableName);
        } else {
            throw new RuntimeException("Table " + tableName + " not found in your database.");
        }
    }

    public Map<String, Table> getTableList() {
        return tableList;
    }


    public String createNewTable(String tableName, String[] cols) {
        try {
            if (getTableList().containsKey(tableName)) {
                throw new RuntimeException("Table already exists!");
            }
            Table t = new Table(tableName);
            for (String c : cols) {
                String[] col = c.trim().split("\\s+");
                if (col.length == 2) {
                    t.addColumn(col[0], col[1]);
                } else {
                    throw new RuntimeException("invalid column : " + c);
                }
            }
            tableList.put(tableName, t);
        } catch (RuntimeException e) {
            return "ERROR: " + e.getMessage();
        }
        return "";
    }

    public String load(String tableName) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(tableName + ".tbl"));
            String line = br.readLine();
            if (line == null) {
                throw new RuntimeException("cannot read the file -> the file is empty!");
            }
            String[] cols = line.split("\\s*,\\s*");
            Pattern p = Pattern.compile("\\s*(\\w+)\\s*(\\w+)\\s*");
            for (int i = 0; i < cols.length; i++) {
                Matcher m = p.matcher(cols[i]);
                if (!m.matches()) {
                    throw new RuntimeException("invalid column definition");
                }
                if (!m.group(2).equals("string") && !m.group(2).equals("int")
                        && !m.group(2).equals("float")) {
                    throw new RuntimeException("column type is wrong");
                }
            }
            createNewTable(tableName, cols);

            line = br.readLine();
            while (line != null) {
                String[] rowElems = line.split("\\s*,\\s*");
                insertRow(tableName, rowElems);
                if (rowElems.length == cols.length) {
                    line = br.readLine();
                } else {
                    throw new RuntimeException("invalid row number definition");
                }
            }
            br.close();
        } catch (RuntimeException e) {
            if (tableList.containsKey(tableName)) {
                tableList.remove(tableName);
            }
            return "ERROR: " + e.getMessage();
        } catch (FileNotFoundException f) {
            return "ERROR: File not found.";
        } catch (IOException io) {
            return "ERROR: File input has error";
        }
        return "";
    }

    public String store(String tableName) {
        if (this.tableList.containsKey(tableName)) {
            String name = tableName + ".tbl";
            try (PrintStream out = new PrintStream(new FileOutputStream(name))) {
                out.print(tableList.get(tableName));
                out.flush();
                out.close();
            } catch (RuntimeException e) {
                return "ERROR: " + e.getMessage();
            } catch (FileNotFoundException f) {
                return "ERROR: File not found";
            }
        } else {
            return "ERROR: Such table does not exist.";
        }
        return "";
    }

    public String dropTable(String tableName) {
        try {
            if (tableList.containsKey(tableName)) {
                tableList.remove(tableName);
            } else {
                throw new RuntimeException("No such a table :" + tableName);
            }
        } catch (RuntimeException e) {
            return "ERROR: " + e.getMessage();
        }
        return "";
    }

    public String insertRow(String tableName, String[] cols) {
        Row r = tableList.get(tableName).createNewRow();
        for (int i = 0; i < cols.length; i++) {
            Matcher strMatcher = Pattern.compile("\\s*('[^']*'*)\\s*").matcher(cols[i]);
            Matcher intMatcher = Pattern.compile("\\s*(-?\\d+)\\s*").matcher(cols[i]);
            Matcher fltMatcher = Pattern.compile("\\s*(-?\\d*\\.\\d+)\\s*").matcher(cols[i]);

            if (!cols[i].equals("NOVALUE") && !cols[i].equals("NaN")) {
                if (strMatcher.matches()) {
                    if (!getTable(tableName).typeStrOfColIndex(i).equals("string")) {
                        throw new RuntimeException("Column type was string, but should be "
                                + getTable(tableName).typeStrOfColIndex(i));
                    }
                } else if (intMatcher.matches()) {
                    if (!getTable(tableName).typeStrOfColIndex(i).equals("int")) {
                        throw new RuntimeException("Column type was int, should be "
                                + getTable(tableName).typeStrOfColIndex(i));
                    }
                } else if (fltMatcher.matches()) {
                    if (!getTable(tableName).typeStrOfColIndex(i).equals("float")) {
                        throw new RuntimeException("Column type was float, but should be "
                                + getTable(tableName).typeStrOfColIndex(i));
                    }
                } else {
                    throw new RuntimeException("Wrong Column : Only string, int, float type!"


                    );
                }
            }

            String c = cols[i].trim();
            String input = c;
            Table t = tableList.get(tableName);
            t.addElemToRowWithColName(r, tableList.get(tableName).nameOfColIndex(i), input);
        }
        tableList.get(tableName).addWholeRow(r);
        return "";
    }

    public String printTable(String tableName) {
        return getTable(tableName).toString();
    }

    //public String

    public Table colExprSelect(String newTableName, ArrayList<String[]> allExprsHandledList,
                               String[] tableNameList) {
        //join all cols & get one joined table
        Table joined = Table.joinAllCols(this, "Joined", tableNameList);

        //expressions handling
        Table resultTable = Table.selectTableWithColExprs(joined,
                            newTableName, allExprsHandledList);

        return resultTable;
    }

    public Table joinAllColsWithCond(String newTableName, ArrayList<String[]> allCondsHandledList,
                                     String[] tableNameList) {
        //join all cols & get one joined table
        Table joined = Table.joinAllCols(this, "Joined", tableNameList);

        //conditions handling
        Table resultTable = Table.selectTableWithConds(joined, newTableName, allCondsHandledList);

        return resultTable;

    }

    public Table selectColExprCond(String newTableName,
                                   ArrayList<String[]> allExprsHandledList,
                                   ArrayList<String[]> allCondsHandledList,
                                   String[] tableNameList) {
        //join all cols & get one joined table
        Table joined = Table.joinAllCols(this, "Joined", tableNameList);

        //expressions handling
        Table middleTable = Table.selectTableWithColExprs(joined, "Middle", allExprsHandledList);

        //conditions handling
        Table resultTable = Table.selectTableWithConds(middleTable,
                            newTableName, allCondsHandledList);

        return resultTable;
    }

    public Table joinAllCols(String newTableName, String[] tableNameList) {
        try {
            return Table.joinAllCols(this, newTableName, tableNameList);
        } catch (RuntimeException e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}
