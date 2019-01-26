package edu.student;

import java.util.ArrayList;
import java.util.List;
import storageManager.MainMemory;
import storageManager.SchemaManager;

public class Create {

	public static boolean execute(MainMemory memory, SchemaManager schemaManager, Statement statement) {
		String tableName = statement.getFirstChild().getFirstChildAttribute();
//        System.out.println("Create Table: " + tableName);


        // This Statement is the table information
        Statement fieldInfo = statement.getLastChild();
        assert fieldInfo.getAttribute() == "create_col";
        ArrayList<String> fieldNames = get_field_names_from_query(fieldInfo);
        ArrayList<storageManager.FieldType> fieldTypes = get_field_types_from_query(fieldInfo);
//        System.out.print("Number of Field Names: ");
//        System.out.println(fieldNames.size());

//        System.out.print("Number of Field Types: ");
//        System.out.println(fieldTypes.size());

        storageManager.Schema scheme = new storageManager.Schema(fieldNames, fieldTypes);
        storageManager.Relation r = createTable(tableName, schemaManager, scheme);

        return true;
	}

	private static ArrayList<String> get_field_names_from_query(Statement fieldInfo) {
        ArrayList<String> names = new ArrayList<>();
        for(int i = 0; i < fieldInfo.getBranchSize(); ++i) {
            Statement field = fieldInfo.getChild(i);
            assert field.getAttribute() == "create_col_feature";
            String name = field.getFirstChild().getFirstChildAttribute();
//            System.out.println("Field Name: " + name);
            names.add(name);
        }
        return names;
    }

    private static ArrayList<storageManager.FieldType> get_field_types_from_query(Statement fieldInfo) {
        ArrayList<storageManager.FieldType> fieldTypes = new ArrayList<>();
//        System.out.println(fieldInfo.getBranchSize());
        
        for(int i = 0; i < fieldInfo.getBranchSize(); ++i) {
            Statement field = fieldInfo.getChild(i);
            assert field.getAttribute() == "create_col_feature";
            String name = field.getLastChild().getFirstChildAttribute();
//            System.out.println("Field Type: " + name);

            switch(name.toLowerCase()) {
                case "int":
                    fieldTypes.add(storageManager.FieldType.INT);
                    break;
                case "str20":
                    fieldTypes.add(storageManager.FieldType.STR20);
                    break;
                default:
                    System.out.println("Error: Unknown column type: " + name + ", only [INT, STR20]");
            }
        }
        return fieldTypes;
    }

    private static storageManager.Relation createTable(String name, SchemaManager schemaManager, storageManager.Schema schema) {
        storageManager.Relation r = schemaManager.createRelation(name, schema);
        return r;
    }


}

