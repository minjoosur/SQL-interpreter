package db;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Created by minjoo on 2/20/17.
 */
public class Table {
    private String tableName;
    private ColInfo cols;
    private ArrayList<Row> rows;

    public Table(String name) {
        tableName = name;
        cols = new ColInfo();
        rows = new ArrayList<>();
    }

    public Table(String name, Table t1, Table t2) {
        tableName = name;
        cols = new ColInfo(t1.cols, t2.cols);
        rows = new ArrayList<>();

        for (Row r1 : t1.rows()) {
            for (Row r2 : t2.rows()) {
                Row newRow = this.createNewRow();
                for (String s : cols().nameList()) {
                    if (t1.contains(s) && t2.contains(s)) {
                        if (r1.getValueByColName(s).equals(r2.getValueByColName(s))) {
                            newRow.addElemWithColName(s, r1.getValueByColName(s));
                        } else {
                            break;
                        }
                    } else if (t1.contains(s)) {
                        newRow.addElemWithColName(s, r1.getValueByColName(s));
                    } else {
                        newRow.addElemWithColName(s, r2.getValueByColName(s));
                    }
                }
                addJoinedRow(newRow);
            }
        }
    }

    public String tableName() {
        return tableName;
    }

    public ColInfo cols() {
        return cols;
    }

    public ArrayList<Row> rows() {
        return rows;
    }

    public void addColumn(String colName, String type) {
        cols.add(colName, type);
        return;
    }

    public Row createNewRow() {
        return new Row(this);
    }

    public void addElemToRowWithColName(Row r, String colName, Object value) {
        if (this.contains(colName)) {
            if (r.size() < cols().size()) {
                r.addElemWithColName(colName, value);
            } else {
                throw new RuntimeException("The number of rows exceeded the number of columns. ");
            }
        } else {
            throw new RuntimeException("Column does not exist : " + colName);
        }
        return;
    }

    public void addWholeRow(Row r) {
        if (r.size() == cols.size()) {
            rows.add(r);
        } else {
            throw new RuntimeException("Row size does not match the table column size.");
        }
    }

    public void addJoinedRow(Row r) {
        if (r.size() == cols.size()) {
            rows.add(r);
        }
    }

    public boolean contains(String s) {
        return cols().colInfo().containsKey(s);
    }

    public int indexOfColName(String s) {
        return cols().indexOfName(s);
    }

    public String nameOfColIndex(int i) {
        return cols().nameOfIndex(i);
    }

    public String typeStrOfColName(String s) {
        return this.cols().typeStrOfName(s);
    }

    public String typeStrOfColIndex(int i) {
        return this.cols().typeStrOfIndex(i);
    }

    public static Table joinAllCols(Database db, String newTableName, String[] tableNameList) {
        Table temp = db.getTable(tableNameList[0]);
        if (tableNameList.length == 1) {
            return temp;
        } else {
            for (int i = 1; i < tableNameList.length; i++) {
                temp = new Table(newTableName, temp, db.getTable(tableNameList[i]));
            }
            return temp;
        }
    }

    //this method is for SELECT with COLUMN EXPRESSIONS
    public static Table selectTableWithColExprs(Table oldTable, String tableName, ArrayList<String[]> allExprsHandledList) {
        Table newTable = new Table(tableName);

        for (String[] col : allExprsHandledList) {
            if (col.length == 1) {
                if (oldTable.contains(col[0])) {
                    newTable.addColumn(col[0], oldTable.typeStrOfColName(col[0]));
                    newTable.copyAColumnFromTable(oldTable, col, oldTable.typeStrOfColName(col[0]), oldTable.typeStrOfColName(col[0]));
                } else {
                    throw new RuntimeException("This column does not exist in joined table");
                }
            } else if (col.length == 4) {
                if (oldTable.contains(col[0]) && oldTable.contains(col[2])) {   //if old table contains both columns
                    if (oldTable.typeStrOfColName(col[0]).equals(oldTable.typeStrOfColName(col[2])) && oldTable.typeStrOfColName(col[0]).equals("string") && col[1].equals("+")) {
                        //if operation is string + string
                        newTable.addColumn(col[3], "string");
                        newTable.copyAColumnFromTable(oldTable, col, oldTable.typeStrOfColName(col[0]), oldTable.typeStrOfColName(col[2]));
                    } else if (!oldTable.typeStrOfColName(col[0]).equals("string") && !oldTable.typeStrOfColName(col[2]).equals("string")) {
                        //if not string & string, (which means, int or floats)
                        if (oldTable.typeStrOfColName(col[0]).equals(oldTable.typeStrOfColName(col[2])) && oldTable.typeStrOfColName(col[0]).equals("int")) {
                            //if operation is int +-*/ int
                            newTable.addColumn(col[3], "int");
                            newTable.copyAColumnFromTable(oldTable, col, oldTable.typeStrOfColName(col[0]), oldTable.typeStrOfColName(col[2]));
                        } else { //if operation is float & float OR float & int
                            newTable.addColumn(col[3], "float");
                            newTable.copyAColumnFromTable(oldTable, col, oldTable.typeStrOfColName(col[0]), oldTable.typeStrOfColName(col[2]));
                        }
                    } else {    //if trying to do illegal operation - which means one of them is string but not + operation
                        throw new RuntimeException("Wrong Operation for column types: " + oldTable.typeStrOfColName(col[0]) + " and " + oldTable.typeStrOfColName(col[2]));
                    }
                } else if (!oldTable.contains(col[0])) {
                    throw new RuntimeException("The column " + col[0] + " does not exist in joined table");
                } else {    //if second one (col[2]) is not column name
                    Matcher strMatcher = Pattern.compile("\\s*'([^']*)'*\\s").matcher(col[2]);
                    Matcher intMatcher = Pattern.compile("\\s*(\\d+)\\s*").matcher(col[2]);
                    Matcher fltMatcher = Pattern.compile("\\s*(\\d+\\.\\d+)\\s*").matcher(col[2]);
                    if (strMatcher.matches() && "string".equals(oldTable.typeStrOfColName(col[0])) && col[1].equals("+")) { // if col[2] is String && String + String
                        newTable.addColumn(col[3], "string");
                        newTable.copyAColumnFromTable(oldTable, col, oldTable.typeStrOfColName(col[0]), "string");
                    } else if (intMatcher.matches() && "int".equals(oldTable.typeStrOfColName(col[0]))) {  //if col[2] is int && col[0] is int
                        newTable.addColumn(col[3], "int");
                        newTable.copyAColumnFromTable(oldTable, col, oldTable.typeStrOfColName(col[0]), "int");
                    } else if (intMatcher.matches() && "float".equals(oldTable.typeStrOfColName(col[0]))) {  //if col[2] is int && col[0] is float
                        newTable.addColumn(col[3], "float");
                        newTable.copyAColumnFromTable(oldTable, col, oldTable.typeStrOfColName(col[0]), "int");
                    } else if (fltMatcher.matches() && ("int".equals(oldTable.typeStrOfColName(col[0])) || "float".equals(oldTable.typeStrOfColName(col[0])))) {  //if col[2] is float && col[0]is int or float
                        newTable.addColumn(col[3], "float");
                        newTable.copyAColumnFromTable(oldTable, col, oldTable.typeStrOfColName(col[0]), "float");
                    } else {
                        throw new RuntimeException("Such column expression not allowed : " + col[0] + col[1] + col[2] + " as " + col[3]);
                    }
                }
            } else {
                throw new RuntimeException("Malformed column expression!");
            }
        }
        return newTable;
    }

    public void copyAColumnFromTable(Table oldTable, String[] expr, String col1TypeString, String col2TypeString) {
        if (expr.length == 1) {    //if the expression is just one column

            if (cols().size() == 1) {        //if table 'rows' is empty
                for (int i = 0; i < oldTable.rows().size(); i++) {
                    Row oldRow = oldTable.rows().get(i);
                    Row newRow = createNewRow();

                    newRow.addElemWithColName(expr[0], oldRow.getValueByColName(expr[0]));
                    addWholeRow(newRow);
                }
            } else {                        //if table 'rows' has row already
                for (int i = 0; i < oldTable.rows().size(); i++) {
                    Row oldRow = oldTable.rows().get(i);

                    rows().get(i).addElemWithColName(expr[0], oldRow.getValueByColName(expr[0]));
                }
            }

        } else {                   //if the expression is operation (ex) x * y as a
            for (int i = 0; i < oldTable.rows().size(); i++) {
                Row oldRow = oldTable.rows().get(i);
                Object col2Value;
                if(oldTable.contains(expr[2])) {    // if column operation between columns
                    col2Value = oldRow.getValueByColName(expr[2]);
                } else {                            // if column and literal operation
                    if ("string".equals(col2TypeString)) {  //if col[0] + str
                        col2Value = expr[2];
                    } else if ("int".equals(col2TypeString)) {   // if col[0] + int
                        col2Value = Integer.valueOf(expr[2]);
                    } else {                                     //if col[0] + float
                        col2Value = Float.valueOf(expr[2]);
                    }
                }
                Object result;

                if (oldRow.getValueByColName(expr[0]).equals("NaN") || col2Value.equals("NaN")) {
                    result = "NaN";
                } else if (expr[1].equals("+")) {   //string + string, int + int, float + float, int + float, float + int
                    if (oldRow.getValueByColName(expr[0]).equals("NOVALUE") || col2Value.equals("NOVALUE")) { // if there's novalue
                        if (oldRow.getValueByColName(expr[0]).equals("NOVALUE") && col2Value.equals("NOVALUE")) {   //if noval + novalue
                            result = "NOVALUE";
                        } else if (oldRow.getValueByColName(expr[0]).equals("NOVALUE")) { //if first one is novalue
                            result = col2Value;
                        } else { //if second one is novalue
                            result = oldRow.getValueByColName(expr[0]);
                        }
                    } else if (col1TypeString.equals(col2TypeString)) { //if their type is the same, (str+str, int+int, float+float)
                        if (col1TypeString.equals("string")) {   //str + str
                            String result1 = (String) oldRow.getValueByColName(expr[0]);
                            String result2 = (String) col2Value;
                            result = result1.substring(0, result1.length() - 1) + result2.substring(1);
                        } else if (col1TypeString.equals("int")) {//int + int
                            result = (Integer) oldRow.getValueByColName(expr[0]) + (Integer) col2Value;
                        } else {                                //float + float
                            result = (Float) oldRow.getValueByColName(expr[0]) + (Float) col2Value;
                        }
                    } else if (col1TypeString.equals("int")) {    //int + float
                        result = (Integer) oldRow.getValueByColName(expr[0]) + (Float) col2Value;
                    } else { //float + int
                        result = (Float) oldRow.getValueByColName(expr[0]) + (Integer) col2Value;
                    }

                } else if (expr[1].equals("-")) {    //int - int, float - int, int - float, float - float
                    if (oldRow.getValueByColName(expr[0]).equals("NOVALUE") || col2Value.equals("NOVALUE")) { // if there's novalue
                        if (oldRow.getValueByColName(expr[0]).equals("NOVALUE") && col2Value.equals("NOVALUE")) {   //if noval - novalue
                            result = "NOVALUE";
                        } else if (oldRow.getValueByColName(expr[0]).equals("NOVALUE")) { //if first one is novalue (noval - int, noval - float)
                            if (col2TypeString.equals("int")) { //noval - int
                                result = 0 - (Integer) col2Value;
                            } else { // noval - float
                                result = 0 - (Float) col2Value;
                            }
                        } else { //if second one is novalue (int - noval , float - noval)
                            result = oldRow.getValueByColName(expr[0]);
                        }
                    } else if (col1TypeString.equals(col2TypeString)) { //if their types are the same, (int-int, float-float)
                        if (col1TypeString.equals("int")) {//int - int
                            result = (Integer) oldRow.getValueByColName(expr[0]) - (Integer) col2Value;
                        } else {                         //float - float
                            result = (Float) oldRow.getValueByColName(expr[0]) - (Float) col2Value;
                        }
                    } else if (col1TypeString.equals("int")) {    //int - float
                        result = (Integer) oldRow.getValueByColName(expr[0]) - (Float) col2Value;
                    } else {                                    //float - int
                        result = (Float) oldRow.getValueByColName(expr[0]) - (Integer) col2Value;
                    }

                } else if (expr[1].equals("*")) {    //int * int, float * int, int * float, float * float
                    if (oldRow.getValueByColName(expr[0]).equals("NOVALUE") || col2Value.equals("NOVALUE")) { // if there's novalue
                        if (oldRow.getValueByColName(expr[0]).equals("NOVALUE") && col2Value.equals("NOVALUE")) {   //if noval * novalue
                            result = "NOVALUE";
                        } else if (oldRow.getValueByColName(expr[0]).equals("NOVALUE")) { //if first is novalue (noval * int, noval * float)
                            if (col2TypeString.equals("int")) { //noval * int
                                result = 0;
                            } else { // noval * float
                                result = 0 * (Float) col2Value;
                            }
                        } else { //if second one is novalue (int * noval , float * noval)
                            if (col1TypeString.equals("int")) { // int * noval
                                result = 0;
                            } else { // float * noval
                                result = (Float) oldRow.getValueByColName(expr[0]) * 0;
                            }
                        }
                    } else if (col1TypeString.equals(col2TypeString)) { //if their type is the same, (int*int, float*float)
                        if (col1TypeString.equals("int")) {//int * int
                            result = (Integer) oldRow.getValueByColName(expr[0]) * (Integer) col2Value;
                        } else {                         //float * float
                            result = (Float) oldRow.getValueByColName(expr[0]) * (Float) col2Value;
                        }
                    } else if (col1TypeString.equals("int")) {    //int * float
                        result = (Integer) oldRow.getValueByColName(expr[0]) * (Float) col2Value;
                    } else {                                    //float * int
                        result = (Float) oldRow.getValueByColName(expr[0]) * (Integer) col2Value;
                    }

                } else {                            // int/int, float/int, int/float, float/float
                    if (col2Value.toString().equals("0") || col2Value.toString().equals("0.0")) {// division by zero
                        result = "NaN";
                    } else if (oldRow.getValueByColName(expr[0]).equals("NOVALUE") || col2Value.equals("NOVALUE")) { // if there's novalue
                        if (oldRow.getValueByColName(expr[0]).equals("NOVALUE") && col2Value.equals("NOVALUE")) {   //if noval / novalue
                            result = "NOVALUE";
                        } else if (oldRow.getValueByColName(expr[0]).equals("NOVALUE")) { //if first is novalue (noval / int, noval / float)
                            if (col2TypeString.equals("int")) { //noval * int
                                result = 0;
                            } else { // noval * float
                                result = 0 / (Float) col2Value;
                            }
                        } else { //if second one is novalue (int / noval , float / noval)
                            result = "NaN";
                        }
                    } else {    //not division by zero
                        if (col1TypeString.equals(col2TypeString)) { //if their type is the same, (int/int, float/float)
                            if (col1TypeString.equals("int")) {//int / int
                                result = (Integer) oldRow.getValueByColName(expr[0]) / (Integer) col2Value;
                            } else {                         //float / float
                                result = (Float) oldRow.getValueByColName(expr[0]) / (Float) col2Value;
                            }
                        } else if (col1TypeString.equals("int")) {    //int / float
                            result = (Integer) oldRow.getValueByColName(expr[0]) / (Float) col2Value;
                        } else {                                    //float / int
                            result = (Float) oldRow.getValueByColName(expr[0]) / (Integer) col2Value;
                        }
                    }
                }

                if (cols().size() == 1) {  //if it's the first column
                    Row newRow = createNewRow();

                    newRow.addElemWithColName(expr[3], result);
                    addWholeRow(newRow);
                } else {                //if it's NOT the first column
                    rows().get(i).addElemWithColName(expr[3], result);
                }
            }
        }
    }

    public static Table selectTableWithConds (Table oldTable, String tableName, ArrayList<String[]> allCondsHandledList) {
        Table newTable = new Table(tableName);
        //Copy all the cols from oldTable to this table
        for(int i = 0; i < oldTable.cols().size(); i++) {
            newTable.addColumn(oldTable.nameOfColIndex(i), oldTable.typeStrOfColIndex(i));
        }

        //iterating through oldTable's rows
        for(int i = 0; i < oldTable.rows().size(); i++) {
            Row oldRow = oldTable.rows().get(i);

            //iterating through all the conditions
            int count = 0;
            for (String[] cond : allCondsHandledList) { //cond : [col1 , > , col2]

                boolean condIsTrue = false;
                if (oldTable.contains(cond[0]) && oldTable.contains(cond[2])) {   //if old table contains both columns
                    if (oldTable.typeStrOfColName(cond[0]).equals(oldTable.typeStrOfColName(cond[2])) && oldTable.typeStrOfColName(cond[0]).equals("string")) {
                        //if operation is string >, >=, <, <=, !=, == string
                        condIsTrue = newTable.condChecker(oldRow, cond, "string", "string");
                    } else if (!oldTable.typeStrOfColName(cond[0]).equals("string") && !oldTable.typeStrOfColName(cond[2]).equals("string")) {
                        //if not string & string, (which means, int or floats)
                        if (oldTable.typeStrOfColName(cond[0]).equals(oldTable.typeStrOfColName(cond[2])) && oldTable.typeStrOfColName(cond[0]).equals("int")) {
                            //if operation is int +-*/ int
                            condIsTrue = newTable.condChecker(oldRow, cond, "int", "int");
                        } else { //if operation is float & float OR float & int
                            condIsTrue = newTable.condChecker(oldRow, cond, oldTable.typeStrOfColName(cond[0]), oldTable.typeStrOfColName(cond[2]));
                        }
                    } else {    //if trying to do illegal operation - which means one of them is string but not + operation
                        throw new RuntimeException("Wrong Operation for column types: " + oldTable.typeStrOfColName(cond[0]) + " and " + oldTable.typeStrOfColName(cond[2]));
                    }
                } else if (!oldTable.contains(cond[0])) {    //if old table does not contain col[0]
                    throw new RuntimeException("The column " + cond[0] + " does not exist in joined table");
                } else {    //if second one (cond[2]) is not column name
                    Matcher strMatcher = Pattern.compile("\\s*\\'([\\s\\w\\d]*)\\'\\s*").matcher(cond[2]);
                    Matcher intMatcher = Pattern.compile("\\s*(\\d+)\\s*").matcher(cond[2]);
                    Matcher fltMatcher = Pattern.compile("\\s*(\\d+\\.\\d+)\\s*").matcher(cond[2]);
                    if (strMatcher.matches() /*&& "string".equals(oldTable.typeStrOfColName(cond[0]))*/) { // if cond[2] is String && String < String
                        condIsTrue = newTable.condChecker(oldRow, cond, "string", "string");
                    } else if (intMatcher.matches() && "int".equals(oldTable.typeStrOfColName(cond[0]))) {  //if cond[2] is int && col[0] is int : int < int
                        condIsTrue = newTable.condChecker(oldRow, cond, "int", "int");
                    } else if (intMatcher.matches() && "float".equals(oldTable.typeStrOfColName(cond[0]))) {  //if cond[2] is int && col[0] is float
                        condIsTrue = newTable.condChecker(oldRow, cond, "float", "int");
                    } else if (fltMatcher.matches() && ("int".equals(oldTable.typeStrOfColName(cond[0])) || "float".equals(oldTable.typeStrOfColName(cond[0])))) {  //if col[2] is float && col[0]is int or float
                        condIsTrue = newTable.condChecker(oldRow, cond, "int", "float");
                    } else {
                        throw new RuntimeException("Such condition expression not allowed : " + cond[0] + cond[1] + cond[2]);
                    }
                }
                if(!condIsTrue) {
                    break;
                }else {
                    count++;
                }
            }
            //add only rows which satisfies every possibility
            if (count == allCondsHandledList.size()) {
                newTable.addWholeRow(oldRow);
            }
        }
        return newTable;
    }

    public boolean condChecker(Row oldRow, String[] cond, String col1TypeString, String oper2TypeString) {
        Object oper2Value;
        if(oldRow.containsCol(cond[2])) {    // if col & col
            oper2Value = oldRow.getValueByColName(cond[2]);
        } else {                             // if col & literal
            if ("string".equals(oper2TypeString)) {  //if col[0] & str
                oper2Value = (String) cond[2];
            } else if ("int".equals(oper2TypeString)) {   // if col[0] & int
                oper2Value = Integer.valueOf(cond[2]);
            } else {                                     //if col[0] & float
                oper2Value = Float.valueOf(cond[2]);
            }
        }

        Object oper1Value = oldRow.getValueByColName(cond[0]);
        if (oper1Value.equals("NOVALUE") || oper2Value.equals("NOVALUE")) {  //if one of them are novalue
            return false;

        } else if (cond[1].equals(">")) {   //string > string, int > int, float > float, int > float, float > int
            if (oper1Value.equals("NaN") || oper2Value.equals("NaN")) { // if there's NaN
                if(oper1Value.equals(oper2Value)) { //if both of them are NaN
                    return false;   //NaN > NaN  -> false
                } else {
                    return oper1Value.equals("NaN"); // if sth > NaN -> True , or False
                }
            } else if (col1TypeString.equals(oper2TypeString)) { //if their type is the same, (str>str, int>int, float>float)
                if (col1TypeString.equals("string")) {   //str > str
                    int result =  ((String) oper1Value).substring(0,((String) oper1Value).length()-1).compareTo(((String) oper2Value).substring(0,((String) oper2Value).length()-1));
                    return result > 0;
                } else if (col1TypeString.equals("int")) {//int > int
                    boolean result = (Integer) oper1Value > (Integer) oper2Value;
                    return result;
                } else {                                //float > float
                    return (Float) oper1Value > (Float) oper2Value;
                }
            } else if (col1TypeString.equals("int")) {    //int + float
                return (Integer) oper1Value > (Float) oper2Value;
            } else { //float + int
                return (Float) oper1Value > (Integer) oper2Value;
            }

        } else if (cond[1].equals(">=")) {   //string >= string, int >= int, float >= float, int >= float, float >= int
            if (oper1Value.equals("NaN") || oper2Value.equals("NaN")) { // if there's NaN
                return oper1Value.equals("NaN"); // if sth > NaN -> True , or False
            } else if (col1TypeString.equals(oper2TypeString)) { //if their type is the same, (str>str, int>int, float>float)
                if (col1TypeString.equals("string")) {   //str > str
                    return ((String) oper1Value).substring(0,((String) oper1Value).length()-1).compareTo(((String) oper2Value).substring(0,((String) oper2Value).length()-1)) >= 0;
                } else if (col1TypeString.equals("int")) {//int > int
                    return (Integer) oper1Value >= (Integer) oper2Value;
                } else {                                //float > float
                    return (Float) oper1Value >= (Float) oper2Value;
                }
            } else if (col1TypeString.equals("int")) {    //int > float
                return (Integer) oper1Value >= (Float) oper2Value;
            } else { //float > int
                return (Float) oper1Value >= (Integer) oper2Value;
            }

        } else if (cond[1].equals("<")) {   //string > string, int < int, float < float, int < float, float < int
            if (oper1Value.equals("NaN") || oper2Value.equals("NaN")) { // if there's NaN
                if(oper1Value.equals(oper2Value)) { //if both of them are NaN
                    return false;   //NaN > NaN  -> false
                } else {
                    return oper2Value.equals("NaN"); // if NaN < Sth -> True , or False
                }
            } else if (col1TypeString.equals(oper2TypeString)) { //if their type is the same, (str<str, int<int, float<float)
                if (col1TypeString.equals("string")) {   //str < str
                    return ((String) oper1Value).substring(0,((String) oper1Value).length()-1).compareTo(((String) oper2Value).substring(0,((String) oper2Value).length()-1)) < 0;
                } else if (col1TypeString.equals("int")) {//int < int
                    return (Integer) oper1Value < (Integer) oper2Value;
                } else {                                //float < float
                    return (Float) oper1Value < (Float) oper2Value;
                }
            } else if (col1TypeString.equals("int")) {    //int < float
                return (Integer) oper1Value < (Float) oper2Value;
            } else { //float < int
                return (Float) oper1Value < (Integer) oper2Value;
            }

        } else if (cond[1].equals("<=")) {   //string >= string, int <= int, float <= float, int <= float, float <= int
            if (oper1Value.equals("NaN") || oper2Value.equals("NaN")) { // if there's NaN
                return oper2Value.equals("NaN"); // if NaN <= Sth , NaN <= NaN -> True , or False
            } else if (col1TypeString.equals(oper2TypeString)) { //if their type is the same, (str<=str, int<=int, float<=float)
                if (col1TypeString.equals("string")) {   //str <= str
                    return ((String) oper1Value).substring(0,((String) oper1Value).length()-1).compareTo(((String) oper2Value).substring(0,((String) oper2Value).length()-1)) <= 0;
                } else if (col1TypeString.equals("int")) {//int <= int
                    return (Integer) oper1Value <= (Integer) oper2Value;
                } else {                                //float <= float
                    return (Float) oper1Value <= (Float) oper2Value;
                }
            } else if (col1TypeString.equals("int")) {    //int <= float
                return (Integer) oper1Value <= (Float) oper2Value;
            } else { //float > int
                return (Float) oper1Value <= (Integer) oper2Value;
            }

        } else if (cond[1].equals("!=")) {   //string != string, int != int, float != float, int != float, float != int
            if (oper1Value.equals("NaN") || oper2Value.equals("NaN")) { // if there's NaN
                return !oper1Value.equals(oper2Value); // if NaN != Sth , Sth != NaN -> True , or False
            } else if (col1TypeString.equals(oper2TypeString)) { //if their type is the same, (str<=str, int<=int, float<=float)
                if (col1TypeString.equals("string")) {   //str != str
                    return !((String) oper1Value).equals((String) oper2Value);
                } else if (col1TypeString.equals("int")) {//int != int
                    return !((Integer) oper1Value).equals((Integer) oper2Value);
                } else {                                //float <= float
                    return !((Float) oper1Value).equals((Float) oper2Value);
                }
            } else if (col1TypeString.equals("int")) {    //int <= float
                return !((Float) oper2Value).equals((((Integer) oper1Value + (Float) oper2Value)) / 2);
            } else { //float != int
                return !((Float) oper1Value).equals((((Float) oper1Value + (Integer) oper2Value)) / 2);
            }

        } else {   //string == string, int == int, float == float, int == float, float == int
            if (oper1Value.equals("NaN") || oper2Value.equals("NaN")) { // if there's NaN
                return oper1Value.equals(oper2Value); // if NaN ==  NaN -> True , or False
            } else if (col1TypeString.equals(oper2TypeString)) { //if their type is the same, (str==str, int==int, float==float)
                if (col1TypeString.equals("string")) {   //str == str
                    return ((String) oper1Value).equals((String) oper2Value);
                } else if (col1TypeString.equals("int")) {//int == int
                    return ((Integer) oper1Value).equals((Integer) oper2Value);
                } else {                                //float == float
                    return ((Float) oper1Value).equals((Float) oper2Value);
                }
            } else if (col1TypeString.equals("int")) {    //int == float
                return ((Float) oper2Value).equals((((Integer) oper1Value + (Float) oper2Value)) / 2);
            } else { //float != int
                return ((Float) oper1Value).equals((((Float) oper1Value + (Integer) oper2Value)) / 2);
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(cols().toString());
        for (int i = 0; i < rows().size(); i++) {
            sb.append("\n" + rows().get(i).toString());
        }
        return sb.toString();
    }
}

