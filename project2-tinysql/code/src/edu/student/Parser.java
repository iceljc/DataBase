package edu.student;


import java.util.*;

public class Parser{

    private HashMap<String, Integer> priority;

    public Parser() {
        priority = new HashMap<String, Integer>();
        priority.put("OR", 0);
        priority.put("AND", 1);
        priority.put("or", 0);
        priority.put("and", 1);
        priority.put("=", 2);
        priority.put(">", 2);
        priority.put("<", 2);
        priority.put("+", 3);
        priority.put("-", 3);
        priority.put("*", 4);
    }

    private static boolean isInteger(String str) {
        try {
            Integer.parseInt(str);
            return true;
        } catch (NumberFormatException nfe) {
            return false;
        }
    }

    public Statement parser(String command) {
        System.out.println("Start processing query: " + command + "\n");
        command = command.replaceAll(";", "").replaceAll("=", " = ").replaceAll(" {2}", " ").replaceAll(" \\(", "\\(").replaceAll(", ", ",");

        String cmd[] = command.split("( |,|(?=\\())");

        // for (String a : cmd){
        // 	a = a.trim();
        // 	a = a.replaceAll("\\(", "").replaceAll("\\)", "");
        // 	System.out.println(a);
        // }

        return parse(cmd, "initial");
    }

    private Statement parse(String[] command, String key){
        Statement statement = null;

        switch (key.toLowerCase()) {
            case "initial":
                switch (command[0].toLowerCase()){
                    case "select":
                    case "create":
                    case "insert":
                    case "delete":
                    case "drop":
                        statement = parse(command, command[0]);
                        break;
                }
                break;


            case "delete":
                String relation = command[2];
                statement = new Statement(key);
                statement.addNode(addLeaf(relation, "relation"));
                if (command.length>3 && command[3].equalsIgnoreCase("where")){
                    statement.addNode(parse(Arrays.copyOfRange(command, 4, command.length), "where"));
                }
                break;


            case "insert":
                int insertValues = -1;
                int insertSelect = -1;

                // get index for VALUES or SELECT
                for (int i=0; i<command.length; ++i) {
                    if (command[i].equalsIgnoreCase("values"))
                    {
                        insertValues = i;
                    }
                    else if (command[i].equalsIgnoreCase("select"))
                    {
                        insertSelect = i;
                    }
                }

                if (insertValues == -1 && insertSelect == -1){
                    System.out.println("INSERT ERROR: Cannot find VALUES or SELECT !\n");
                }else if (insertValues != -1 && insertSelect != -1){
                    System.out.println("INSERT ERROR: Find both VALUES and SELECT !\n");
                }else{
                    statement = new Statement(key);
                    statement.addNode(addLeaf(command[2], "relation"));
                    int insertAttribute = insertValues != -1? insertValues : insertSelect;
                    statement.addNode(parse(Arrays.copyOfRange(command, 3, insertAttribute), "column"));

                    if (insertValues != -1) { // values exists
                        statement.addNode(parse(Arrays.copyOfRange(command, insertValues+1, command.length), "values"));
                    }else{ // select exists
                        statement.addNode(parse(Arrays.copyOfRange(command, insertSelect, command.length), "values"));
                    }
                }
                break;

            case "values":
                if (command[0].equalsIgnoreCase("select")){
                    statement = parse(command, "select");
                }else{
                    statement = new Statement(key);
                    for (String cmd : command){
                        cmd = cmd.trim();
                        cmd = cmd.replaceAll("\\(", "").replaceAll("\\)", "");
                        statement.addNode(addLeaf(cmd, "value"));
                    }
                }

                break;

            case "drop":
                statement = new Statement(key);
                statement.addNode(addLeaf(command[2], "relation"));
                break;

            case "select":
                statement = new Statement(key);
                int from_id = -1, where_id = -1, order_id = -1, distinct_id = -1;

                for (int i = 0; i != command.length; ++i){
                    if (command[i].equalsIgnoreCase("from")){
                        from_id = i;
                    }else if (command[i].equalsIgnoreCase("where")){
                        where_id = i;
                    }else if (command[i].equalsIgnoreCase("order")){
                        order_id = i;
                    }
                    else if (command[i].equalsIgnoreCase("distinct"))
                        distinct_id = i;
                }

                if(distinct_id != -1) {
                    statement.addNode(new Statement("distinct"));
                }
                else {
                    // this is a hack so things still works
                    distinct_id = 0;
                }

                if (from_id != -1){ // select (...) from ...

                    statement.addNode(parse(Arrays.copyOfRange(command, distinct_id + 1, from_id), "column"));
                }

                if (where_id != -1){ // select ... from (...) where (...) order by ...
                    statement.addNode(parse(Arrays.copyOfRange(command, from_id+1, where_id), "from"));
                    int order_check = order_id != -1 ? order_id : command.length;
                    statement.addNode(parse(Arrays.copyOfRange(command, where_id+1, order_check), "where"));
                }else{ // select ... from (...) order by ...
                    int order_check = order_id != -1 ? order_id : command.length;
                    statement.addNode(parse(Arrays.copyOfRange(command, from_id+1, order_check), "from"));
                }

                if (order_id != -1){ // select ... order by (...)
                    statement.addNode(parse(Arrays.copyOfRange(command, order_id+2, command.length), "order"));
                }
                break;


            case "order":
                statement = new Statement(key);
                statement.addNode(addLeaf(command[0], "column_id"));
                break;


            case "from":
                statement = new Statement(key);
                for (String cmd : command) {
                    statement.addNode(addLeaf(cmd, "relation"));
                }
                break;


            case "where":
                statement = new Statement("search_expression");
                statement.addNode(searchCondition(command));
                break;


            case "column":
                statement = new Statement(key);
                // this should never happen, this is not where we wanted distinct to be
                if (command[0].equalsIgnoreCase("distinct")){
                    statement.addNode(new Statement("distinct"));
                    Statement col = statement.getLeaf();
                    for (int i = 1; i<command.length; ++i){
                        String cmd = command[i];
                        if (cmd.length()>0){
                            col.addNode(addLeaf(cmd.charAt(cmd.length()-1) == ','? cmd.substring(0, cmd.length()-1): cmd, "column_id"));
                        }
                    }
                }else{
                    for (String cmd : command){
                        cmd = cmd.replaceAll("[()]", "");
                        statement.addNode(addLeaf(cmd, "column_id"));
                    }
                }
                break;

            case "create":
                statement = new Statement(key);
                statement.addNode(addLeaf(command[2], "relation"));
                statement.addNode(parse(Arrays.copyOfRange(command, 3, command.length), "create_col"));
                break;

            case "create_col":
                statement = new Statement(key);
                for (int i = 0; i<command.length/2; ++i) {
                    statement.addNode(parse(Arrays.copyOfRange(command, 2*i, 2*i+2), "create_col_feature"));
                }
                break;

            case "create_col_feature":
                statement = new Statement(key);
                String fieldName = command[0].replaceAll("\\(", "");
                String fieldType = command[1].replaceAll("\\)", "");
                statement.addNode(addLeaf(fieldName, "column_id"));
                statement.addNode(addLeaf(fieldType, "column_type"));
                break;


        }

        return statement;

    }



    private Statement searchCondition(String[] tokens){
        Stack<Statement> stack = new Stack<Statement>();

        for (int i = 0; i < tokens.length; ++i) {
            String token = tokens[i];

            if (priority.containsKey(token)) {
                if (stack.size() >= 3) {
                    Statement last = stack.pop();
                    if (priority.get(token) >= priority.get(stack.peek().getAttribute())) {
                        stack.push(last);
                        stack.push(new Statement(token));
                    } else {
                        while (stack.size()>0 && priority.get(token) < priority.get(stack.peek().getAttribute())) {
                            Statement operator = stack.pop();
                            Statement non_operator = stack.pop();
                            operator.addNode(non_operator);
                            operator.addNode(last);
                            last = operator;
                        }

                        stack.push(last);
                        stack.push(new Statement(token));
                    }

                } else {
                    stack.push(new Statement(token));
                }


            } else if (isInteger(token)) {

                stack.push(addLeaf(token, "INT"));

            } else if (token.charAt(0) == '"') {

                stack.push(addLeaf(token.substring(1, token.length()-1), "STR20"));

            } else if (token.charAt(0) == '(') {
                int start = i;

                for (int bracket_count = 0; i<tokens.length; ++i) {
                    String t = tokens[i];

                    if (t.charAt(0) == '(') {
                        bracket_count++;
                    }
                    if (t.charAt(t.length()-1) == ')') {
                        bracket_count--;
                        if (bracket_count == 0) {
                            break;
                        }
                    }
                }

                String[] tokensInBracket = Arrays.copyOfRange(tokens, start, i+1);

                // remove the first '(' and last ')'
                tokensInBracket[0] = tokensInBracket[0].substring(1);
                int len = tokensInBracket.length;
                tokensInBracket[len-1] = tokensInBracket[len-1].substring(0, tokensInBracket[len-1].length()-1);

                stack.push(searchCondition(tokensInBracket));

            } else {
                stack.push(addLeaf(token, "column_id"));
            }
        }

        if (stack.size() >= 3) {
            Statement operand;
            for (operand = stack.pop(); stack.size() >= 2;) {
                Statement operator = stack.pop();
                operator.addNode(stack.pop());
                operator.addNode(operand);
                operand = operator;
            }
            return operand;

        } else {
            return stack.peek();
        }



    }



    private Statement addLeaf(String cmd, String key) {
        Statement statement = new Statement(key);
        String[] words = cmd.split("\\.");
        for (String w : words) {
            statement.addNode(new Statement(w));
        }
        return statement;
    }


}

