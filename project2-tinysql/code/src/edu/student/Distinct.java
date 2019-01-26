package edu.student;

import java.util.ArrayList;
import java.util.List;

public class Distinct {

    public static void execute(String table, storageManager.SchemaManager manager, storageManager.MainMemory memory) {
        storageManager.Relation relation = manager.getRelation(table);

        if(relation.getNumOfBlocks() <= memory.getMemorySize()) {
            relation.getBlocks(0, 0, relation.getNumOfBlocks());
            ArrayList<storageManager.Tuple> tuples = memory.getTuples(0, relation.getNumOfBlocks());
            Distinct.inMemoryDistinct(tuples);
            if(tuples.isEmpty()) {
                relation.deleteBlocks(0);
            }
            else {
                memory.setTuples(0, tuples);
                int blocks = (int) Math.ceil((double) tuples.size() / tuples.get(0).getTuplesPerBlock());
                relation.setBlocks(0, 0, blocks);
                relation.deleteBlocks(blocks);
            }
        }
        else
            Distinct.twoPassDistinct(table, memory, manager);
    }

    // this is by no mean the most efficent algorithm, it is the simplest.
    public static void inMemoryDistinct(List<storageManager.Tuple> tuples) {
        // this starts at the end of the list so that when the indices are added, they are in
        // an order so that they can be removed without changing the elements indices
        // at the front of the list
        List<Integer> remove = new ArrayList<>();
        for(int i = 0; i <  tuples.size() - 1; ++i)
            for(int j =  i + 1; j < tuples.size();)
                if(Sort.compareTuple(tuples.get(i), tuples.get(j)) == 0)
                    tuples.remove(j);
                else
                    ++j;

//        System.out.println(tuples.size());
//        remove.forEach(System.out::println);
//        for(int i : remove)
//            tuples.remove(i);
    }

    public static void twoPassDistinct(String relation, storageManager.MainMemory memory, storageManager.SchemaManager manager) {
        List<String> names = List.of(relation);
//        Helpers.phaseOnePass(memory, manager, names, null);



        int distLoc = 0;
        int outLoc = 0;
        storageManager.Relation r  = manager.getRelation(relation);
        Sort.twoPassSort(r, null, memory);

        System.err.flush();
        System.out.println(r);
        System.out.flush();

        int size = r.getNumOfBlocks();

        while(size > 0) {
            System.out.println(size);
            int readSize = Math.min(size, memory.getMemorySize() - 1);
            r.getBlocks(distLoc, 0, readSize);
            ArrayList<storageManager.Tuple> tuples = memory.getTuples(0, readSize);
            int oldSize = tuples.size();
            inMemoryDistinct(tuples);
            memory.setTuples(0, tuples);
            if(oldSize != tuples.size()) {
                int blockSize = (int) Math.ceil((double) tuples.size() / tuples.get(0).getTuplesPerBlock());
                r.setBlocks(outLoc, 0, blockSize);
                outLoc += blockSize;
            }
            else {
                r.setBlocks(outLoc, 0, readSize);
                outLoc += readSize;
            }

            distLoc += readSize - 1;
            size -= readSize;
        }
        System.err.flush();
        System.out.println(r);
        System.out.flush();
        r.deleteBlocks(outLoc);
    }
}
