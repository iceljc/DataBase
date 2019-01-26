package edu.student;

import storageManager.MainMemory;
import storageManager.Relation;
import storageManager.Schema;
import storageManager.SchemaManager;

import java.util.List;
import java.util.ArrayList;

public class Projection {
    public static ArrayList<storageManager.Tuple> execute(SchemaManager manager, MainMemory memory, Relation relation, Relation out, Statement where, boolean saveSmallTables) {
        if(relation.getNumOfBlocks() == 0)
            return new ArrayList<>();
        if(relation.getNumOfBlocks() <= memory.getMemorySize()) {
            ArrayList<storageManager.Tuple> res = inMemoryProjection(relation, out, where, manager, memory);

            // by default the inMemoryProjection doesnt save the tuples to the output relation
            if(saveSmallTables) {
                if(res.isEmpty())
                    out.deleteBlocks(0);
                else {
                    int blocks = (int) Math.ceil((double) res.size() / res.get(0).getTuplesPerBlock());
                    memory.setTuples(0, res);
                    out.setBlocks(0, 0, blocks);
                }
                return null;
            }
            else
                return res;
        }
        else {
            onePassProjection(relation, out, where, manager, memory);
            return null;
        }
    }

    public static ArrayList<storageManager.Tuple> inMemoryProjection(storageManager.Relation r, storageManager.Relation out, Statement where, storageManager.SchemaManager manager, storageManager.MainMemory memory) {
        ArrayList<storageManager.Tuple> res = new ArrayList<>();
        String tempTableName = out.getRelationName();
        r.getBlocks(0, 0, r.getNumOfBlocks());
        ArrayList<storageManager.Tuple> tuples = memory.getTuples(0, r.getNumOfBlocks());
        for (storageManager.Tuple t : tuples)
            if (where == null || Expression.evaluateBoolean(where, t))
                res.add(Helpers.transform_tuple(t, tempTableName, manager));
        return res;
    }

    public static void onePassProjection(storageManager.Relation r, storageManager.Relation out, Statement where, storageManager.SchemaManager manager, storageManager.MainMemory memory) {
        String tempTableName = out.getRelationName();
        int diskBlock = 0;
        // the first block is to be used as output.
        int opSize = memory.getMemorySize() - 1;
        storageManager.Block output = memory.getBlock(0);
        output.clear();
        int numBlocks = r.getNumOfBlocks();
//                int block_index = 0;
        int relLoc = 0;
        while(numBlocks > 0) {
            int readBlocks = Math.min(opSize, numBlocks);
            r.getBlocks(diskBlock, 1, readBlocks);
            ArrayList<storageManager.Tuple> tuples = memory.getTuples(1, readBlocks);

//                    ArrayList<storageManager.Tuple> res = new ArrayList<>();
            for(storageManager.Tuple t : tuples) {
                // this logic works, i think
                // if there is no where, then the tuple will be tranfromed to the new format and added
                if(where == null || Expression.evaluateBoolean(where, t)) {
                    if(output.isFull()) {
                        // i think this is wrong.
                        out.setBlock(relLoc++, 0);
                        output.clear();
                    }
                    storageManager.Tuple res_t = Helpers.transform_tuple(t, tempTableName, manager);
                    output.appendTuple(res_t);
                }
            }

            numBlocks -= readBlocks;
            diskBlock += readBlocks;
        }
        if(!output.isEmpty())
            out.setBlock(relLoc, 0);
    }



    public static storageManager.Schema get_schema(List<TableAttribute> selections, Schema old, boolean singleTable) {
        ArrayList<String> names = new ArrayList<>();
        ArrayList<storageManager.FieldType> types = new ArrayList<>();
//            System.out.println("Original Schema: ");
//            System.out.println(originalScheme);
        if (selections.get(0).is_star) {
            // populate the selections with all of the column names
            selections.clear();

            for (String name : old.getFieldNames()) {
//                    System.out.println(name);
                selections.add(new TableAttribute(name));
            }
        }

//            System.out.println("Building temporary table");
        for (TableAttribute att : selections) {
            String n = att.to_string();
//                System.out.println("Looking at field: " + n);
            if(singleTable)
                n = att.attribute;

            names.add(n);
            types.add(old.getFieldType(n));
        }

        return new Schema(names, types);
    }
}
