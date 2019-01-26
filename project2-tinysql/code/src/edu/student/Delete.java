package edu.student;

import java.util.ArrayList;

public class Delete {
    private storageManager.MainMemory memory;
    private storageManager.SchemaManager manager;

    public static boolean execute(storageManager.MainMemory mem, storageManager.SchemaManager manager, Statement delete) {
        Delete del = new Delete(mem, manager);
        return del.execute(delete);
    }


    Delete(storageManager.MainMemory mem, storageManager.SchemaManager man) {
        memory = mem;
        manager = man;
    }


    boolean execute(Statement delete) {
        String table = delete.searchChildren("relation").getFirstChildAttribute();
        Statement where = delete.searchChildren("search_expression");

        storageManager.Relation r = manager.getRelation(table);


        int relLoc = 0;
        int writeLoc = 0;
        int size = r.getNumOfBlocks();
        int memSize = memory.getMemorySize() - 1;
        storageManager.Block output = memory.getBlock(0);
        output.clear();

        System.out.println("Size of relation: " + size + " size of memory: " + memory.getMemorySize());
        if(where != null) {
            while (size > 0) {
                int readBlock = Math.min(size, memSize);
                r.getBlocks(relLoc, 1, readBlock);
                ArrayList<storageManager.Tuple> tuples = memory.getTuples(1, readBlock);
                for (storageManager.Tuple t : tuples) {
                    if (!Expression.evaluateBoolean(where, t)) {
                        if (output.isFull()) {
                            r.setBlock(writeLoc++, 0);
                            output.clear();
                        }
                        output.appendTuple(t);
                    } else {
                        System.out.println("Removing Tuple: " + t);
                    }
                }
                size -= readBlock;
                relLoc += readBlock;
            }
            if (!output.isEmpty())
                r.setBlock(writeLoc++, 0);
            output.clear();
            r.deleteBlocks(writeLoc);
        }
        else {
            r.deleteBlocks(0);
        }
        return true;
    }
}
