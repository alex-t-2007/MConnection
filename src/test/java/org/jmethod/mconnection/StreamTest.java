package org.jmethod.mconnection;

import static org.jmethod.mconnection.MConnection.LIMIT;
import static org.jmethod.mconnection.MConnection.createDataSource;
import static org.jmethod.mconnection.TestUtils.deleteAll;
import static org.jmethod.mconnection.TestUtils.testCreateRows;
import static org.jmethod.mconnection.TestUtils.testCreateTables;
import static org.jmethod.mconnection.TestUtils.testDropTables;
import static org.jmethod.mconnection.TestUtils.testReadRows;
import static org.jmethod.mconnection.TestUtils.testUpdateRows;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class StreamTest {

    private static List<Integer> list;
    private static long[] dts = {0L, 0L};

    @Test
    public void mainTest() {
        test();
        test();
        test();
        test();
        test();
        test();
        test();
        test();
        test();
        test();

        test();
        test();
        test();
        test();
        test();
        test();
        test();
        test();
        test();
        test();

        Utils.outln("dtS1F1 ++ dt=" + dts[0]);
        Utils.outln("dtS1F1 -- dt=" + dts[1]);
        Utils.outln("dtS1F1    dt=" + (dts[0] + dts[1]));
    }

    private static void test() {

        int SIZE = 1000000;

        long t0 = System.currentTimeMillis();
        if (list == null) {
            list = listInit(SIZE);
        }
        long t1 = System.currentTimeMillis();
        List<Integer> listFor = for1(list);
        long t2 = System.currentTimeMillis();
        List<Integer> listStream = stream1(list);
        long t3 = System.currentTimeMillis();
        boolean eqLists = equalsLists(listFor, listStream);

        long dtFor1 = t2 - t1;
        long dtStream1 = t3 - t2;
        long dtS1F1 = dtStream1 - dtFor1;
        if (dtS1F1 >= 0) {
            dts[0] = dts[0] + dtS1F1;
        } else {
            dts[1] = dts[1] + dtS1F1;
        }

        Utils.outln("1. listInit t=" + (t1 - t0));
        Utils.outln("2. for1     t=" + dtFor1);
        Utils.outln("3. stream1  t=" + dtStream1);
        Utils.outln("4. eqLists   =" + eqLists);
        Utils.outln("5. SIZE      =" + SIZE);
        Utils.outln("6. stream1 - for1 dt=" + dtS1F1);
        Utils.outln("------------------------------------------------------------------------");
    }

    private static List<Integer> listInit(int size){
        List<Integer> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            list.add(i);
        }
        return list;
    }

    private static List<Integer> for1(List<Integer> list){
        List<Integer> list2 = new ArrayList<>(list.size());
        for (Integer i : list) {
            list2.add(i + 100);
        }
        return list2;
    }

    private static List<Integer> stream1(List<Integer> list){
        List<Integer> list2 = list.stream()
                .map(i -> i + 100)
                .collect(Collectors.toList());
        return list2;
    }

    private static boolean equalsLists(List<Integer> list1, List<Integer> list2){
        return list1.equals(list2);
    }
}
