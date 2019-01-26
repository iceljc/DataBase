package edu.student;

import storageManager.FieldType;
import storageManager.Schema;
import storageManager.Tuple;

public class Expression {

    public static boolean evaluateBoolean(Statement statement, Tuple tuple) {
        String key = statement.getAttribute().toLowerCase();
        if (key.equalsIgnoreCase("search_expression")) {
            return evaluateBoolean(statement.getFirstChild(), tuple);
        } else {
            Statement left = statement.getFirstChild();
            Statement right = statement.getLastChild();
            switch (key) {
                case "and":
                    return evaluateBoolean(left, tuple) && evaluateBoolean(right, tuple);
                case "or":
                    return evaluateBoolean(left, tuple) || evaluateBoolean(right, tuple);
                case "=":
                    return evaluateTuple(left, tuple).equals(evaluateTuple(right, tuple));
                case ">":
                    return evaluateInt(left, tuple) > evaluateInt(right, tuple);
                case "<":
                    return evaluateInt(left, tuple) < evaluateInt(right, tuple);
                default:
                    System.out.flush();
                    System.err.println("ERROR ! This is an incorrect expression...: " + key);
                    System.err.flush();
                    return false;
            }
        }

    }


    private static int evaluateInt(Statement statement, Tuple tuple) {
        String key = statement.getAttribute().toLowerCase();
        switch (key) {
            case "int":
                return Integer.parseInt(statement.getLeafAttribute());

            case "column_id":
                StringBuilder relation_field_name = new StringBuilder();
                for (Statement i : statement.getBranch()) {
                    relation_field_name.append(i.getAttribute()).append(".");
                }
                relation_field_name.setLength(relation_field_name.length()-1);
                String name = relation_field_name.toString();
                return tuple.getField(name).integer;

            default:
                Statement left = statement.getFirstChild();
                Statement right = statement.getLastChild();
                switch (key) {
                    case "+":
                        return evaluateInt(left, tuple) + evaluateInt(right, tuple);
                    case "-":
                        return evaluateInt(left, tuple) - evaluateInt(right, tuple);
                    case "*":
                        return evaluateInt(left, tuple) * evaluateInt(right, tuple);
                    default:
                        return 0;
                }

        }

    }

    private static TupleValue evaluateTuple(Statement statement, Tuple tuple) {
        TupleValue value = new TupleValue();
        String key = statement.getAttribute();

        switch (key) {
            case "INT":
                value.fieldType = FieldType.INT;
                value.number = Integer.parseInt(statement.getLeafAttribute());
                break;
            case "STR20":
                value.fieldType = FieldType.STR20;
                value.str = statement.getLeafAttribute();
                break;
            case "column_id":
                Schema schema = tuple.getSchema();
                String fieldName;
                if (statement.getBranchSize() == 1) {
                    // e.g. exam = 100
                    fieldName = statement.getFirstChildAttribute();

                } else if (statement.getBranchSize() == 2) {
                    // e.g. course.exam = 100
                    String relationName = statement.getFirstChildAttribute();
                    fieldName = statement.getLastChildAttribute();
                    // this is a hack for now, maybe forever (I will forget about it).
                    fieldName = relationName + '.' + fieldName;
//                    if (schema.getFieldOffset(fieldName) == -1) {
//                        System.err.print("Cannot find Field " + fieldName + " in Relation " + relationName);
//                        break;
//                    }
                } else {
                    System.err.print("ERROR ! Unknown Expression.");
                    break;
                }

                FieldType field_type = schema.getFieldType(fieldName);
                value.fieldType = field_type;
                if (field_type == FieldType.INT) {
                    value.number = tuple.getField(fieldName).integer;
                } else {
                    value.str = tuple.getField(fieldName).str;
                }
                break;
            default:
                value.fieldType = FieldType.INT;
                value.number = evaluateInt(statement, tuple);

        }

        return value;

    }



}
