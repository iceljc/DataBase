package edu.student;

import storageManager.FieldType;
import storageManager.Tuple;
import storageManager.Block;
import storageManager.Relation;
import storageManager.MainMemory;

import java.lang.reflect.Array;
import java.util.Comparator;
import java.util.List;
import java.util.ArrayList;


public class Sort {
        static class SubList {
        int start;
        int len;
        int memoryIndex;
        int current;
        Block block;
        Relation relation;
        private MainMemory memory;

        SubList(int start, int len, int index, Relation relation, MainMemory memory) {
            this.start = start;
            this.len = len;
            this.memoryIndex = index;
            this.memory = memory;
            this.current = 0;
            this.relation = relation;
            this.block = null;

//            load();
        }

        void load() {
            if(current >= len) {
                block = null;
                return;
            }

            relation.getBlock(start + current, memoryIndex);
            block = memory.getBlock(memoryIndex);
        }

        void update() {
            if(block.isEmpty()) {
                current++;
                load();
            }
        }

        ArrayList<Tuple> getTuples() {
            return block.getTuples();
        }

        boolean isLoaded() {
            return block != null;
        }
    }

    public static void sort(MainMemory memory, Relation relation, Statement order) {
        if(order == null)
            return;
        order = order.getFirstChild();
        int outNumBlocks = relation.getNumOfBlocks();
        TableAttribute att = null;
        if (order.getBranchSize() == 1)
            att = new TableAttribute(order.getFirstChildAttribute());
        else if (order.getBranchSize() == 2)
            att = new TableAttribute(order.getFirstChildAttribute(), order.getLastChildAttribute());
        if (outNumBlocks <= memory.getMemorySize()) {
            relation.getBlocks(0, 0, outNumBlocks);
            Sort.inMemorySort(memory.getTuples(0, outNumBlocks), att);
        } else
            Sort.twoPassSort(relation, att, memory);
    }

    public static void inMemorySort(List<storageManager.Tuple> tuples, TableAttribute att) {
        System.out.println("In Memory Sort");
        tuples.sort(new Comparator<Tuple>() {
            @Override
            public int compare(Tuple o1, Tuple o2) {
                if(att == null)
                    return compareTuple(o1, o2);
                else
                    return compareTuple(o1, o2, att);
            }
        });
    }

    public static int compareTuple(Tuple o1, Tuple o2) {
        int res = 0;
        for(int i = 0; i < o1.getNumOfFields(); ++i) {
            storageManager.Field f1 = o1.getField(i);
            storageManager.Field f2 = o2.getField(i);
            if(f1.type == FieldType.INT) {
                res = Integer.compare(f1.integer, f2.integer);
                if(res != 0)
                    return res;
            }
            else {
                res = f1.str.compareTo(f2.str);
                if(res != 0)
                    return res;
            }
        }
        return res;
    }

    public static int compareTuple(Tuple o1,Tuple o2,TableAttribute att) {
//        System.out.println("CompareTuple: " + att.attribute);
        storageManager.Schema s1 = o1.getSchema();
        storageManager.Field f1 = null;
        for(String s : s1.getFieldNames()) {
            if(s.contains(att.to_string())) {
                f1 = o1.getField(s);
            }
        }

        storageManager.Schema s2 = o2.getSchema();
        storageManager.Field f2 = null;
        for(String s : s2.getFieldNames()) {
            if(s.contains(att.to_string())) {
                f2 = o2.getField(s);
            }
        }

        if(f1.type == FieldType.INT) {
            int res = Integer.compare(f1.integer, f2.integer);
            if(res != 0)
                return res;
        }
        else {
            int res = f1.str.compareTo(f2.str);
            if(res != 0)
                return res;
        }
        return 0;
    }


    // do not know how to do this yet
    public static void twoPassSort(storageManager.Relation relation, TableAttribute att, storageManager.MainMemory mem) {
        System.out.println("Two Pass Sort");
        storageManager.Schema schema = relation.getSchema();
        int numBlocks = relation.getNumOfBlocks();
//        System.out.printf("Number of blocks: %d\n", numBlocks);
        int diskBlock = 0;
        List<SubList> sublists = new ArrayList<>();
        int memLoc = 0;
        while(numBlocks > 0) {
            int readBlocks = Math.min(mem.getMemorySize(), numBlocks);
            relation.getBlocks(diskBlock, 0, readBlocks);

            ArrayList<Tuple> tuples = mem.getTuples(0, readBlocks);

            // Sort all of the tuples in memory
            inMemorySort(tuples, att);

            mem.setTuples(0, tuples);
            relation.setBlocks(diskBlock, 0, readBlocks);

            sublists.add(new SubList(memLoc * mem.getMemorySize(), readBlocks, memLoc, relation, mem));

            memLoc++;
            numBlocks -= readBlocks;
            diskBlock += readBlocks;
        }

        System.out.println(relation);

        for(int i = 0; i < mem.getMemorySize(); ++i)
            mem.getBlock(i).clear();

        for(SubList l : sublists)
            l.load();

        assert sublists.size() < mem.getMemorySize();

        int oMemoryIndex = sublists.size();

        Block output = mem.getBlock(oMemoryIndex);
        output.clear();


        int relLoc = 0;
        while(!sublists.isEmpty()) {
           Tuple m = getMinFromSubLists(sublists, att);
           if(m == null)
               break;
           if(output.isFull()) {
               relation.setBlock(relLoc++, oMemoryIndex);
               output.clear();
           }
           output.appendTuple(m);
        }

        if(!output.isEmpty())
            relation.setBlock(relLoc, oMemoryIndex);
        output.clear();
        // I think there is another step but I do not know how to implement it.
        System.out.println(relation);
    }

    public static Tuple getMinFromSubLists(List<SubList> sublists, TableAttribute att) {
        List<Tuple> list = new ArrayList<>();
        List<ArrayList<Tuple>> cache = new ArrayList<>();

        for(int i = 0; i < sublists.size();) {
            SubList l = sublists.get(i);
            l.update();

            if(l.isLoaded()) {
                ArrayList<Tuple> t = l.getTuples();
                cache.add(t);
                list.add(t.get(0));
            }
            else {
                // the list is empty remove it
                sublists.remove(l);
                continue;
            }
            ++i;
        }

        if(list.isEmpty())
            return null;

        Sort.inMemorySort(list, att);
        Tuple t = list.get(0);
        removeMin(sublists, t);
        return t;
    }

    public static void removeMin(List<SubList> subs, Tuple min) {
        for(SubList s : subs) {
            ArrayList<Tuple> l = s.getTuples();
            for(int i = 0; i < l.size();)
                if(Sort.compareTuple(l.get(i), min) == 0)
                    l.remove(i);
                else
                    ++i;

            if(l.isEmpty())
                s.block.clear();
            else
                s.block.setTuples(l);
        }
    }

}
