package edu.student;

import storageManager.*;
import storageManager.FieldType;
import storageManager.MainMemory;
import storageManager.SchemaManager;

import javax.swing.plaf.synth.SynthTextAreaUI;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;


public class Insert {
    MainMemory memory;
    SchemaManager manager;
    public static boolean execute(MainMemory memory, SchemaManager manager, Statement statement) {
        Insert insert = new Insert(memory, manager);
        return insert.execute(statement);
    }


    Insert(MainMemory mem, SchemaManager manager) {
        this.memory = mem;
        this.manager = manager;
    }


    boolean execute(Statement statement) {
        assert statement.getBranchSize() == 3;
        String table = statement.getFirstChild().getFirstChildAttribute();
//        System.out.println("Inserting into Table: " + table);
        Statement columns = statement.getChild(1);
        List<String> column_names = get_column_names(columns);

        Statement values = statement.getLastChild();
//        System.out.println("Attribute Name: " +  values.getAttribute());
        if(values.getAttribute().equalsIgnoreCase("values")) {
            List<storageManager.Field> fields = build_field_list(values, manager.getSchema(table), column_names);
            if(fields == null) {
                return false;
            }
            else if(fields.isEmpty()) {
                System.err.println("Insert Error: values list is empty");
                return false;
            }
            if(validate_table(table, column_names, fields)) {
                // we now build a tuple and insert into the table
                storageManager.Tuple tuple = Helpers.toTuple(table, reorder_fields(table, column_names, fields), manager);
                insertTuple(table, 0, tuple);
            }
            else {
                System.err.println("Insert Error: field types didn't match");
            }
        }
        else if(values.getAttribute().equalsIgnoreCase("select")){
            /// TODO
            String resultTable = Select.subquery(memory, manager, values);
            System.err.println("Select Result table: " + resultTable);

            storageManager.Relation input = manager.getRelation(resultTable);

            storageManager.Schema schema = input.getSchema();
            List<String> schema_names = schema.getFieldNames();


            if(column_names.size() != schema_names.size()) {
                System.err.println("Insert Error: the input table schema doesnt match output");
                return false;
            }

            storageManager.Relation outputRelation = manager.getRelation(table);
            storageManager.Schema outputSchema = outputRelation.getSchema();

            int i = 0;
            for(String n : column_names) {
                FieldType type = outputSchema.getFieldType(n);

                FieldType it = schema.getFieldType(i++);

                if(type != it)
                    System.err.println("Insert Error: Field types do not match");
            }

            int diskBlock = 0;
            // the first block is to be used as output.
            int opSize = memory.getMemorySize() - 1;
            storageManager.Block output = memory.getBlock(0);
            output.clear();
            int numBlocks = input.getNumOfBlocks();
            int relLoc = outputRelation.getNumOfBlocks();
            while(numBlocks > 0) {
                int readBlocks = Math.min(opSize, numBlocks);
                input.getBlocks(diskBlock, 1, readBlocks);
                ArrayList<storageManager.Tuple> tuples = memory.getTuples(1, readBlocks);

                for(storageManager.Tuple t : tuples) {
                    if(output.isFull()) {
                        // i think this is wrong.
                        outputRelation.setBlock(relLoc++, 0);
                        output.clear();
                    }
                    storageManager.Tuple res_t = Helpers.transform_tuple(t, table, manager);
                    output.appendTuple(res_t);
                }

                numBlocks -= readBlocks;
                diskBlock += readBlocks;
            }
            if(!output.isEmpty())
                outputRelation.setBlock(relLoc, 0);
        }
        return true;
    }

    // both of the errors in this function should be handled else where so they do not need
    // to be checked here.
    // from the storage manager example.
    public void insertTuple(String name, int memory_block_index, storageManager.Tuple tuple) {
        storageManager.Block block_reference;
        storageManager.Relation relation_reference = manager.getRelation(name);
        if(relation_reference == null) {
            System.out.println("InsertTuple: Relation '" + name + "' doesn't exist");
            return;
        }
        if (relation_reference.getNumOfBlocks()==0) {
//            System.out.print("The relation is empty" + "\n");
//            System.out.print("Get the handle to the memory block " + memory_block_index + " and clear it" + "\n");
            block_reference= memory.getBlock(memory_block_index);
            block_reference.clear(); //clear the block
            block_reference.appendTuple(tuple); // append the tuple
//            System.out.print("Write to the first block of the relation" + "\n");
            relation_reference.setBlock(relation_reference.getNumOfBlocks(),memory_block_index);
        } else {
//            System.out.print("Read the last block of the relation into memory block 5:" + "\n");
            relation_reference.getBlock(relation_reference.getNumOfBlocks()-1,memory_block_index);
            block_reference=memory.getBlock(memory_block_index);

            if (block_reference.isFull()) {
//                System.out.print("(The block is full: Clear the memory block and append the tuple)" + "\n");
                block_reference.clear(); //clear the block
                block_reference.appendTuple(tuple); // append the tuple
//                System.out.print("Write to a new block at the end of the relation" + "\n");
                relation_reference.setBlock(relation_reference.getNumOfBlocks(),memory_block_index); //write back to the relation
            } else {
//                System.out.print("(The block is not full: Append it directly)" + "\n");
                block_reference.appendTuple(tuple); // append the tuple
//                System.out.print("Write to the last block of the relation" + "\n");
                relation_reference.setBlock(relation_reference.getNumOfBlocks()-1,memory_block_index); //write back to the relation
            }
        }
    }

    List<storageManager.Field>  build_field_list(Statement tuples, storageManager.Schema schema, List<String> columns) {
        List<storageManager.Field> fields = new ArrayList<>();
        for(int i = 0; i < tuples.getBranchSize(); ++i) {
            Statement value = tuples.getBranch().get(i);
            storageManager.FieldType expected_type = schema.getFieldType(columns.get(i));

            assert value.getAttribute().equalsIgnoreCase("values");

            String val = value.getFirstChildAttribute();
            storageManager.Field f = new storageManager.Field();
            if(val.contains("\"")) {
                // We have a string, build a string field
                val = val.substring(1, val.length() - 1);
                f.type = storageManager.FieldType.STR20;
                f.str = val;
            }
            else {
                // We have an integer, build an integer field
                if(val.equalsIgnoreCase("null"))  {
                    if(expected_type == FieldType.INT) {
                        f.type = storageManager.FieldType.INT;
                        f.integer = Integer.MIN_VALUE;
                    }
                    else {
                        f.type = storageManager.FieldType.STR20;
                        f.str = "";
                    }
                }
                else {
                    int valI = Integer.parseInt(val);
                    f.type = storageManager.FieldType.INT;
                    f.integer = valI;
                }
            }
            if(f.type != expected_type) {
                System.err.println("Insert Error: Incompatible value type");
                return null;
            }
            fields.add(f);
        }
        return fields;
    }

    List<storageManager.Field> reorder_fields(String table, List<String> columns, List<storageManager.Field> fields) {
        Map<String, storageManager.Field> fieldMap = new HashMap<>();
        for(int i = 0; i < columns.size(); ++i) {
            fieldMap.put(columns.get(i), fields.get(i));
        }

        storageManager.Schema schema = manager.getSchema(table);
        List<String> schema_fields = schema.getFieldNames();

        fields.clear();

        for(String field : schema_fields) {
            storageManager.Field f = fieldMap.get(field);
            fields.add(f);
        }
        return fields;
    }

    boolean validate_table(String table, List<String> columns, List<storageManager.Field> fields) {
        if(columns.size() != fields.size())
            return false;

        storageManager.Relation relation = manager.getRelation(table);
        storageManager.Schema schema = relation.getSchema();
        List<String> schema_fields = schema.getFieldNames();

        if(schema.getNumOfFields() != columns.size())
            return false;

        // Checks whether the column names specificed and the input types are the same
        for(int i = 0; i < columns.size(); ++i) {
            String field = columns.get(i);

            if(schema_fields.indexOf(field) == -1) {
                System.err.println("Insert Error: Field name not found in schema: " + field);
                System.err.flush();
                return false;
            }

            storageManager.FieldType type = schema.getFieldType(field);

            if(type != fields.get(i).type)
                return false;

        }
        return true;
    }

    List<String> get_column_names(Statement columns) {
        System.out.println(columns.getAttribute());
        List<String> names = new ArrayList<>();
        for(Statement stmt : columns.getBranch()) {
            if(stmt.getBranchSize() == 2)
                names.add(stmt.getFirstChildAttribute() + "." + stmt.getLastChildAttribute());
            else
                names.add(stmt.getFirstChildAttribute());
        }
        return names;
    }
}
