package edu.student;

import edu.student.*;

import storageManager.Disk;
import storageManager.MainMemory;
import storageManager.SchemaManager;
import storageManager.Relation;


public class Drop {

	public static boolean execute(MainMemory memory, SchemaManager schemaManager, Statement statement) {
		String tableName = statement.getFirstChild().getFirstChildAttribute();
        System.out.println("Drop Table: " + tableName);
        
        return dropTable(schemaManager, tableName);
	}

	private static boolean dropTable(SchemaManager schemaManager, String name) {
		storageManager.Relation find = schemaManager.getRelation(name);
        if(find == null) {
            System.out.println("DropTable: table '" + name + "' does not exist");
            return false;
        } else {
        	schemaManager.deleteRelation(name);
        	return true;
        }
        
	}


}










