package edu.student;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.Scanner;

import storageManager.Field;
import storageManager.FieldType;
import storageManager.Schema;

public class Main {

    public static void main(String[] args) {
        // write output file
        Agent agent = new Agent();
        String input = "";
        String output = null;
        if(args.length >= 2) {
            // argument parsing
            if(args[0].equals("-f")) {
                input = args[1];
            }
            else {
                System.err.println("Invalid commandline argument");
                return;
            }
            if(args.length >= 4) {
                if(args[2].equals("-o")) {
                    output = args[3];
                }
                else {
                    System.err.println("Invalid commandline argument");
                    return;
                }
            }
        }
        mainInterface(agent, input, output);
    }

    private static void mainInterface(Agent agent, String inputFile, String outfile) {
        if(!inputFile.isEmpty()) {
            agent.executeMultiStatements(inputFile, outfile);
            return;
        }

        Scanner input = new Scanner(System.in);
        while (true) {
            System.out.println("Please input a command:\n"
                    + "S: Process a TinySQL statement\n"
                    + "F: Process a file with statements\n"
                    + "Q: Quit\n"
                    + "> ");

            String inpContent = input.nextLine().trim();
            switch (inpContent.toUpperCase()) {
                case "S":
                    while (true) {
                        System.out.println("Please input a TinySQL statement (type \"R\" to return to upper menu): ");
                        String command = input.nextLine().trim();
                        if (command.equals("R")) {
                            break;
                        } else {
                            if (agent.executeOneStatement(command)) {
                                System.out.println("TinySQL statement executed successfully !");
                            } else {
                                System.out.println("Invalid statment !");
                            }

                        }
                    }
                    break;

                case "F":
                    System.out.println("Please input a file name (type \"R\" to return to upper menu): ");
                    String fileName = input.nextLine().trim();
                    if (fileName.equals("R")) {
                        break;
                    } else {
                        if (agent.executeMultiStatements(fileName, null)) {
                            System.out.println("File executed successfully !");
                        } else {
                            System.out.println("Invalid file !");
                        }
                    }
                    break;

                case "Q":
                    input.close();
                    System.out.println("Exit TinySQL !!");
                    return;
            }
        }
    }
}
