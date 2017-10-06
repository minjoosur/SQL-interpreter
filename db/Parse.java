package db;

import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class Parse {
    // Various common constructs, simplifies parsing.
    private static final String REST  = "\\s*(.*)\\s*",
                                COMMA = "\\s*,\\s*",
                                AND   = "\\s+and\\s+";

    // Stage 1 syntax, contains the command name.
    private static final Pattern CREATE_CMD = Pattern.compile("create table " + REST),
                                 LOAD_CMD   = Pattern.compile("load " + REST),
                                 STORE_CMD  = Pattern.compile("store " + REST),
                                 DROP_CMD   = Pattern.compile("drop table " + REST),
                                 INSERT_CMD = Pattern.compile("insert into " + REST),
                                 PRINT_CMD  = Pattern.compile("print " + REST),
                                 SELECT_CMD = Pattern.compile("select " + REST);

    // Stage 2 syntax, contains the clauses of commands.
    private static final Pattern CREATE_NEW  = Pattern.compile("(\\S+)\\s+\\((\\S+\\s+\\S+\\s*" +
                                               "(?:,\\s*\\S+\\s+\\S+\\s*)*)\\)"),
                                 SELECT_CLS  = Pattern.compile("([^,]+?(?:,[^,]+?)*)\\s+from\\s+" +
                                               "(\\S+\\s*(?:,\\s*\\S+\\s*)*)(?:\\s+where\\s+" +
                                               "([\\w\\s+\\-*/'<>=!]+?(?:\\s+and\\s+" +
                                               "[\\w\\s+\\-*/'<>=!]+?)*))?"),
                                 CREATE_SEL  = Pattern.compile("(\\S+)\\s+as select\\s+" +
                                                   SELECT_CLS.pattern()),
                                 INSERT_CLS  = Pattern.compile("(\\S+)\\s+values\\s+(.+?" +
                                               "\\s*(?:,\\s*.+?\\s*)*)");

    private static final Pattern ONE_COL_EXPR = Pattern.compile("\\s*(\\w+)\\s*([*/+-])\\s*(\\w+|\\d+|\'[\\w\\d\\s]*\')\\s+as\\s+(\\w+)"),
                                 ONE_COL_ONLY = Pattern.compile("(\\w+)"),
                                 COND_EXPR    = Pattern.compile("\\s*(\\w+)\\s*(>|<|<=|>=|==|!=)\\s*(\\d+\\.\\d+|\\d+|\\w+|\\'[^\\']*\\')\\s*");


    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Expected a single query argument");
            return;
        }
    }

    protected static String eval(Database db, String query) {
        Matcher m;
        if ((m = CREATE_CMD.matcher(query)).matches()) {
             query = createTable(db, m.group(1));
        } else if ((m = LOAD_CMD.matcher(query)).matches()) {
             query = loadTable(db, m.group(1));
        } else if ((m = STORE_CMD.matcher(query)).matches()) {
             query = storeTable(db, m.group(1));
        } else if ((m = DROP_CMD.matcher(query)).matches()) {
             query = dropTable(db, m.group(1));
        } else if ((m = INSERT_CMD.matcher(query)).matches()) {
             query = insertRow(db, m.group(1));
        } else if ((m = PRINT_CMD.matcher(query)).matches()) {
             query =  printTable(db, m.group(1));
        } else if ((m = SELECT_CMD.matcher(query)).matches()) {
             query = select("temp", db, m.group(1));
        } else {
            return "ERROR: Malformed query: " + query + "\n";
        }
        return query;
    }

    private static String createTable(Database db, String expr) {
        Matcher m;
        if ((m = CREATE_NEW.matcher(expr)).matches()) {
            return createNewTable(db, m.group(1), m.group(2).split(COMMA));
        } else if ((m = CREATE_SEL.matcher(expr)).matches()) {
            return createSelectedTable(db, m.group(1), m.group(2), m.group(3), m.group(4));
        } else {
            return "ERROR: Malformed create: "+ expr + "\n";
        }
    }

    private static String createNewTable(Database db, String tableName, String[] cols) {
       return db.createNewTable(tableName, cols);
    }

    private static String createSelectedTable(Database db, String name, String exprs, String tables, String conds) {
        System.out.printf("You are trying to create a table named %s by selecting these expressions:" +
                " '%s' from the join of these tables: '%s', filtered by these conditions: '%s'\n", name, exprs, tables, conds);

        if(db.getTableList().containsKey(name)) {
            return "ERROR: Table Already Exists!";
        }
        db.getTableList().put(name, select(name,db, exprs, tables, conds) );
        return "";
    }

    private static String loadTable(Database db, String tableName) {
        if(db.getTableList().containsKey(tableName)) {
            db.getTableList().remove(tableName);
        }
        return db.load(tableName);
    }

    private static String storeTable(Database db, String tableName) {
        return db.store(tableName);
    }

    private static String dropTable(Database db, String tableName) {
        return db.dropTable(tableName);
    }

    private static String insertRow(Database db, String expr) {
        Matcher m = INSERT_CLS.matcher(expr);
        if (!m.matches()) {
            return "ERROR: Malformed insert: " + expr + "\n";
        }else{
            try {
                String rowStr = m.group(2);
                String tableName = m.group(1);
                String[] cols = rowStr.split(COMMA);
                return db.insertRow(tableName, cols);
            } catch (Exception e) {
                return "ERROR: " + e.getMessage();
            }
        }
    }

    private static String printTable(Database db, String tableName) {
        return db.printTable(tableName);
    }

    private static String select(String tableName, Database db, String expr) {
        Matcher m = SELECT_CLS.matcher(expr);
        if (!m.matches()) {
            return "ERROR: Malformed select: " + expr + "\n";
        }
        return select(tableName, db, m.group(1), m.group(2), m.group(3)).toString();
    }

    private static Table select(String tableName, Database db, String multiColExprs, String tables, String conds) {
//        System.out.printf("You are trying to select these expressions:" +
//                " '%s' from the join of these tables: '%s', filtered by these conditions: '%s'\n", multiColExprs, tables, conds);

        //handling tableNames
        String[] tableNames = tables.trim().replace(" ", "").split(COMMA);


        //handling column expressions

        //SELECT * FROM T1, T2 (WHERE C1 > C2)
        if(multiColExprs.equals("*")) {
            if(conds == null) { //select * from table
                return db.joinAllCols(tableName, tableNames);
            } else {    //select * from table WHERE CONDS
                String[] multiCondsList = conds.split("and");
                ArrayList<String[]> allCondsHandledList = new ArrayList<>();

                for (String singleCond : multiCondsList) {
                    Matcher condMatcher = COND_EXPR.matcher(singleCond);

                    if(condMatcher.matches()) {
                        allCondsHandledList.add(new String[] {condMatcher.group(1), condMatcher.group(2), condMatcher.group(3)});
                    } else {
                        throw new RuntimeException("ERROR: Malformed condition expression : " + conds);
                    }
                }
                return db.joinAllColsWithCond(tableName, allCondsHandledList, tableNames);
            }
        }

        //SELECT COL1 FROM T1, T2 (WHERE C1 > C2)
        //SELECT COL1, COL2 + COL3 AS COL4 FROM T1, T2 (WHERE C1>C2)
        String[] multiColExprsList = multiColExprs.split(COMMA);
        ArrayList<String[]> allExprHandledList = new ArrayList<>();

        for (String singleColExpr : multiColExprsList) {
            Matcher oneColOnlyMatcher = ONE_COL_ONLY.matcher(singleColExpr);
            Matcher oneColExprMatcher = ONE_COL_EXPR.matcher(singleColExpr);
            if (oneColOnlyMatcher.matches()) {  //if that ONE EXPRESSION is just one column : COL1
                allExprHandledList.add(new String[] {oneColOnlyMatcher.group(1)});
            } else if (oneColExprMatcher.matches()){ //if that ONE EXPRESSION is complicated : COL1 + COL2 AS COL4
                allExprHandledList.add(new String[] {oneColExprMatcher.group(1), oneColExprMatcher.group(2), oneColExprMatcher.group(3), oneColExprMatcher.group(4)});
            } else {
                throw new RuntimeException( "ERROR: Your column expression is not allowed : " + multiColExprs);
            }
        }

        //String[] tableNames => [t1, t2, t3]
        //ArrayList AllExprHandledList => [ [a], [b, +, y, z], [c] ]

        if (conds == null){ //select x, x + y as z from t1, t2
            return db.colExprSelect(tableName, allExprHandledList, tableNames);
        } else {    //select x, x+y as z from table WHERE CONDS
        String[] multiCondsList = conds.split("and");
        ArrayList<String[]> allCondsHandledList = new ArrayList<>();

        for (String singleCond : multiCondsList) {
            Matcher condMatcher = COND_EXPR.matcher(singleCond);

            if(condMatcher.matches()) {
                allCondsHandledList.add(new String[] {condMatcher.group(1), condMatcher.group(2), condMatcher.group(3)});
            } else {
                throw new RuntimeException("ERROR: Malformed condition expression : " + conds);
            }
        }
        return db.selectColExprCond(tableName, allExprHandledList, allCondsHandledList, tableNames);
        }
    }

    //private static String[] colExprsList
}
