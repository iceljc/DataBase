package edu.student;

import java.io.FilterOutputStream;
import java.util.*;

import storageManager.MainMemory;
import storageManager.SchemaManager;
import storageManager.Field;
import storageManager.FieldType;
import storageManager.Relation;
import storageManager.Block;
import storageManager.Tuple;

import java.util.List;
import java.util.ArrayList;


public class Helpers {
    public static storageManager.Tuple toTuple(String name, List<storageManager.Field> tupleElement, storageManager.SchemaManager manager) {
        storageManager.Relation r = manager.getRelation(name);
        storageManager.Tuple t = r.createTuple();

        if (t.getNumOfFields() != tupleElement.size()) {
            // error
            return null;
        }

        for (int i = 0; i < t.getNumOfFields(); i++) {
            storageManager.Field f = t.getField(i);
            storageManager.Field lf = tupleElement.get(i);
            if (f.type != lf.type) {
                // error
                return null;
            }

            switch (lf.type) {
                case INT:
                    t.setField(i, lf.integer);
                    break;
                case STR20:
                    t.setField(i, lf.str);
                    break;
            }
        }
        return t;
    }


    // this is a naive merging of schema field names. Meant for use in cross join
    static public storageManager.Schema naive_schema_merge(String t1, storageManager.Schema s1, String t2, storageManager.Schema s2) {
        ArrayList<String> field_names = new ArrayList<>();
        ArrayList<storageManager.FieldType> field_types = new ArrayList<>();

        for(int i = 0; i < s1.getNumOfFields(); ++i) {
            field_names.add((new StringBuilder()).append(t1).append(".").append(s1.getFieldName(i)).toString());
            field_types.add(s1.getFieldType(i));
        }

        for(int i = 0; i < s2.getNumOfFields(); ++i) {
            field_names.add((new StringBuilder()).append(t2).append(".").append(s2.getFieldName(i)).toString());
            field_types.add(s2.getFieldType(i));
        }

        return new storageManager.Schema(field_names, field_types);
    }

    static public Tuple merge_tuples(Tuple t1, Tuple t2, storageManager.Schema schema) {
        return null;
    }


    static public storageManager.Tuple transform_tuple(storageManager.Tuple tuple, String new_relation, SchemaManager manager) {
        storageManager.Relation r = manager.getRelation(new_relation);
        storageManager.Schema s = r.getSchema();
        String name = r.getRelationName();

        List<storageManager.Field> new_fields = new ArrayList<>();
        for(String fieldName : s.getFieldNames()) {
            storageManager.Field f = tuple.getField(fieldName);
            new_fields.add(f);
        }
        return Helpers.toTuple(name, new_fields, manager);
    }

    static List<NaturalJoinInfo> possible_join_pairs(List<String> tables, SchemaManager manager) {
        int numtables = tables.size();
        if(numtables == 1)
            return null;
        List<NaturalJoinInfo> info = new ArrayList<>();
        for(int i = 0; i < tables.size()-1; ++i) {
            for(int j = i + 1; j < tables.size(); ++j) {
                String t1 = tables.get(i);
                String t2 = tables.get(j);
                if(t1.equalsIgnoreCase(t2))
                    continue;
                storageManager.Schema s1 = manager.getSchema(t1);
                storageManager.Schema s2 = manager.getSchema(t2);
                List<String> common = Helpers.common_fields(s1, s2);
                List<String> columns = new ArrayList<>();
                columns.addAll(common);

                // build a list of the resulting field names
                for(String c : s1.getFieldNames())
                    if(!common.contains(c))
                        columns.add(c);
                for(String c : s2.getFieldNames())
                    if(!common.contains(c))
                        columns.add(c);

                if(!common.isEmpty())
                    info.add(new NaturalJoinInfo(t1, t2, common, columns));
            }
        }
        return info;
    }

    public static NaturalJoinInfo find_info(List<NaturalJoinInfo> info, String a, String b) {
        for(NaturalJoinInfo i : info) {
            String s = i.tableS;
            String l = i.tableL;
            if(s.equalsIgnoreCase(a) && l.equalsIgnoreCase(b))
                return i;
            else if(s.equalsIgnoreCase(b) && l.equalsIgnoreCase(a))
                return i;
        }
        return null;
    }

    static List<String> common_fields(storageManager.Schema s1, storageManager.Schema s2) {
        List<String> columns = new ArrayList<>();
        for(String c1 : s1.getFieldNames()) {
            for(String c2 : s2.getFieldNames()) {
               if(c1.equalsIgnoreCase(c2) && s1.getFieldType(c1) == s2.getFieldType(c2))
                   columns.add(c1.toLowerCase());
            }
        }
        return columns;
    }

    static void phaseOnePass(MainMemory memory, SchemaManager manager, List<String> tables, TableAttribute att) {
        for (String table : tables) {
            storageManager.Relation r = manager.getRelation(table);
            storageManager.Schema schema = r.getSchema();
            int numBlocks = r.getNumOfBlocks();
            int diskBlock = 0;
            while (numBlocks > 0) {
                int readBlocks = Math.min(memory.getMemorySize(), numBlocks);
                r.getBlocks(diskBlock, 0, readBlocks);
                ArrayList<Tuple> tuples = memory.getTuples(0, readBlocks);
                Sort.inMemorySort(tuples, att);
                memory.setTuples(0, tuples);
                numBlocks -= readBlocks;
                diskBlock += readBlocks;
            }
        }
    }
     // puts the smaller tables at the front so we do not build a large table at the being
    // this will keep the table the smallest for as long as possible
    public static ArrayList<String> optimize_cross_join_order(ArrayList<String> tables, SchemaManager manager) {
        tables.sort(new Comparator<String>() {
            @Override

            public int compare(String o1, String o2) {
                storageManager.Relation r1 = manager.getRelation(o1);
                storageManager.Relation r2 = manager.getRelation(o2);
                return Integer.compare(r1.getNumOfTuples(), r2.getNumOfTuples());

            }
        });

        return tables;
    }

    public static void printTable(String table, SchemaManager manager, MainMemory memory, FilterOutputStream output) {
        storageManager.Relation relation = manager.getRelation(table);
        int _numBlocks = relation.getNumOfBlocks();
        int _diskBlock = 0;
        storageManager.Schema schema = relation.getSchema();
        for(String _s : schema.getFieldNames()) {
            System.out.print(_s);
            System.out.print('\t');
        }
        System.out.println();
        System.err.flush();
        while(_numBlocks > 0) {
            int readBlocks = Math.min(memory.getMemorySize(), _numBlocks);
            relation.getBlocks(_diskBlock, 0, readBlocks);
            for(storageManager.Tuple t : memory.getTuples(0, readBlocks)) {
                System.out.println(t);
            }
            _numBlocks -= readBlocks;
            _diskBlock += readBlocks;
            System.out.flush();
        }
    }

//    public static ArrayList<String> get_join_order(List<String> relationName, storageManager.SchemaManager manager) {
//        ArrayDeque<String> rels = new ArrayDeque<>();
//        ArrayList<String> output = new ArrayList<>();
//
//        for(int i = 0; i < relationName.size() - 1; ++i) {
//            String t1 = relationName.get(i);
//            String t2 = relationName.get(i + 1);
//
//            storageManager.Schema s1 = manager.getSchema(t1);
//            storageManager.Schema s2 = manager.getSchema(t2);
//
//            List<String> columns = new ArrayList<>();
//            for(String c1 : s1.getFieldNames()) {
//                for(String c2 : s2.getFieldNames()) {
//                    if(c1.equalsIgnoreCase(c2) && s1.getFieldType(c1) == s2.getFieldType(c2))
//                        columns.add(c1.toLowerCase());
//                }
//            }
//            if(columns.isEmpty()) {
//                rels.push(t1);
//            }
//            else {
//                output.add(t1);
//            }
//        }
//    }
}
