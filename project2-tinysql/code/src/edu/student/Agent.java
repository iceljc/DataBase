package edu.student;

import java.io.File;
import java.io.FilterOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import edu.student.*;

import storageManager.Disk;
import storageManager.MainMemory;
import storageManager.SchemaManager;
import java.io.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class Agent {
	private Disk disk;
    private MainMemory memory;
    private SchemaManager schemaManager;
    private Parser parser;
    public static FilterOutputStream outStream = null;

    // private double dbmsTime;
    // private long dbmsIO;
    private long computerStartTime;

    Agent() {
    	this.disk = new Disk();
        this.memory = new MainMemory();
        this.schemaManager = new SchemaManager(memory, disk);
        this.parser = new Parser();
        TemporaryTables.init(this.schemaManager);
    }

    private boolean execute(String command) {
    	Statement statementTree = parser.parser(command);
    	String primeCommandType = statementTree.getAttribute(); // create, insert ...

        TemporaryTables.clear();

    	switch (primeCommandType.toLowerCase()) {
    		case "create":
    			return Create.execute(memory, schemaManager, statementTree);

    		case "insert":
    			return Insert.execute(memory, schemaManager, statementTree);

    		case "select":
    			return Select.execute(memory, schemaManager, statementTree, true);

    		case "delete":
    			return Delete.execute(memory, schemaManager, statementTree);

    		case "drop":
    			return Drop.execute(memory, schemaManager, statementTree);

    		default:
    			System.out.println("Invalid statement !");
    			return false;
    	}

    }


    boolean executeOneStatement(String command) {
        executionBegin();
        boolean done = execute(command);
        if (done) {
            executionEnd();
        }

        return done;
    }

    boolean executeMultiStatements(String fileName, String outfile) {
        try {
            if(outfile == null)
                Agent.outStream = System.out;
            else
                Agent.outStream = new BufferedOutputStream(new FileOutputStream(outfile));
            List<String> slines = null;
            try(Stream<String> lines = Files.lines(Paths.get(fileName))) {
                slines = lines.collect(Collectors.toList());
            }
            catch(Exception e) {
                System.err.println("Input file doesn't exist");
                System.exit(1);
            }
            for(String line : slines) {
                executeOneStatement(line);
            }
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    	return true;
    }


    private void executionBegin() {
    	computerStartTime = System.currentTimeMillis();
        disk.resetDiskTimer();
        disk.resetDiskIOs();   
    }

    private void executionEnd() {
    	System.out.println("Computer elapse time = " + (System.currentTimeMillis() - computerStartTime) + " ms");
        System.out.println("Calculated elapse time = " + String.format("%.2f", disk.getDiskTimer()) + " ms");
        System.out.println("Calculated Disk I/Os = " + disk.getDiskIOs());
        
    }


}










