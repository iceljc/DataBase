package edu.student;

import java.util.ArrayList;

public class TemporaryTables {
    static storageManager.SchemaManager manager;
    static ArrayList<String> tables;

    public static void init(storageManager.SchemaManager manager) {
        TemporaryTables.manager = manager;
        TemporaryTables.tables = new ArrayList<>();
    }

    public static String new_temporary_table(storageManager.Schema schema) {
        int num_tables = tables.size();
        String tableName = "__temp" + String.valueOf(num_tables) + "__";
        manager.createRelation(tableName, schema);
        tables.add(tableName);
        return tableName;
    }

    public static void clear() {
        for(String t : tables) {
            manager.deleteRelation(t);
        }
        tables.clear();
    }

    public static String last() {
        if(tables.isEmpty())
            return null;
        return tables.get(tables.size() - 1);
    }
}

