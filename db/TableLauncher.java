package db;

import db.ColInfo;
import db.Row;
import db.Table;
import org.junit.Test;

/**
 * Created by minjoo on 2/20/17.
 */

public class TableLauncher {
    @Test
    public void ColInfoTest () {
        ColInfo c1 = new ColInfo();
        c1.add("x", "int");
        c1.add("z", "int");
        c1.add("w", "int");
        c1.add("y", "int");

        ColInfo c2 = new ColInfo();
        c2.add("y", "int");
        c2.add("z", "int");
        c2.add("a", "int");
        c2.add("w", "int");

        System.out.print(c1);
        System.out.print(c2);

        ColInfo c3 = new ColInfo(c1, c2);

        System.out.print(c3);
    }

    @Test
    public void RowTest() {
        Table t1 = new Table("t1");
        t1.addColumn("x", "int");
        t1.addColumn("z", "int");
        t1.addColumn("w", "int");
        t1.addColumn("y", "int");

        Row r1 = new Row(t1);


        ColInfo c1 = t1.cols();
        System.out.print(c1);
        System.out.print(r1);

    }

    @Test
    public void TableTest(){
        Table t1 = new Table("t1");
        t1.addColumn("x", "int");
        t1.addColumn("z", "int");
        t1.addColumn("w", "int");
        t1.addColumn("y", "int");

        Row r1 = new Row(t1);


        t1.addWholeRow(r1);

        ColInfo c1 = t1.cols();
        //System.out.print(c1);
        //System.out.print(r1);
        System.out.print(t1);


        Table t2 = new Table("t2");
        t2.addColumn("y", "int");
        t2.addColumn("z", "int");
        t2.addColumn("a", "int");
        t2.addColumn("w", "int");

        Row r2 = new Row(t2);


        t2.addWholeRow(r2);

        ColInfo c2 = t2.cols();
        //System.out.print(c2);
        //System.out.print(r2);
        System.out.print(t2);

        Table t3 = new Table("t3", t1, t2);
        System.out.print(t3);
//        ColInfo c3 = t3.cols();
//        Row r3 = t3.rows().getValueByColName(0);


        return;
    }


    public void main (String[] args) {
    }


}
