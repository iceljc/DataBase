package edu.student;

import storageManager.MainMemory;
import storageManager.Schema;
import storageManager.SchemaManager;

import java.util.ArrayList;
import java.util.List;

public class Select {
    MainMemory memory;
    SchemaManager manager;
    boolean print;

    public static boolean execute(MainMemory memory, SchemaManager manager, Statement statement, boolean outputResults) {
        Select select = new Select(memory, manager, outputResults);
        boolean res = select.select(statement);
        return res;
    }

    public static String subquery(MainMemory memory, SchemaManager manager, Statement statement) {
        Select select = new Select(memory, manager, false);
        return select.execute_select(statement);
    }

    Select(MainMemory mem, SchemaManager man, boolean print) {
        this.memory = mem;
        this.manager = man;
        this.print = print;
    }

    boolean select(Statement statement) {
        execute_select(statement);
        return true;
    }

    private List<TableAttribute> get_selection_columns(Statement selections) {
        List<TableAttribute> selects = new ArrayList<>();
        assert selections.getAttribute().equals("column");
        for(Statement column : selections.getBranch()) {
            if(column.getBranchSize() == 1) {
                String col = column.getFirstChildAttribute();
                if(col.equals("*"))
                    selects.add(new TableAttribute());
                else
                    selects.add(new TableAttribute(col));
            }
            else if(column.getBranchSize() == 2){
                selects.add(new TableAttribute(column.getFirstChildAttribute(), column.getLastChildAttribute()));
            }
            else {
                System.err.println("Error in selection");
            }
        }
        return selects;
    }

    ArrayList<String> get_from_tables(Statement t) {
        ArrayList<String> tables = new ArrayList<>();
        assert t.getAttribute().equals("from");
        for(Statement ts : t.getBranch()) {
            tables.add(ts.getFirstChildAttribute());
        }
//        tables.forEach(System.out::println);
        return tables;
    }

    String execute_select(Statement statement) {

        Statement maybeDistinct = statement.getFirstChild();

        boolean distinct = false;

        if(maybeDistinct.getAttribute().equalsIgnoreCase("distinct"))
            distinct = true;

        int size = statement.getBranchSize();

        List<TableAttribute> selections = get_selection_columns(statement.getByName("column"));
        ArrayList<String> tables = get_from_tables(statement.getByName("from"));

        Statement where = statement.getByName("search_expression");
        Statement order = statement.getByName("order");
        String temptable = "";

        // this is for the special case of only selecting from one table
        if(tables.size() == 1)  {
            // this will be the table name
            String table = tables.get(0);
            storageManager.Relation r = manager.getRelation(table);
            storageManager.Schema s = r.getSchema();
            Schema newSchema = Projection.get_schema(selections, s, true);

            String tempTableName =  TemporaryTables.new_temporary_table(newSchema);
            storageManager.Relation out = manager.getRelation(tempTableName);

            // everything can be done in memory
            System.out.println(r.getNumOfBlocks());

            ArrayList<storageManager.Tuple> res = Projection.execute(manager, memory, r, out, where, false);

            if(res != null) {
                // the table is not saved and we can work with it here
                if (distinct)
                    Distinct.inMemoryDistinct(res);

                if (order != null) {
                    order = order.getFirstChild();
//                    int outNumBlocks = out.getNumOfBlocks();
                    TableAttribute att = null;
                    if (order.getBranchSize() == 1)
                        att = new TableAttribute(order.getFirstChildAttribute());
                    else if (order.getBranchSize() == 2)
                        att = new TableAttribute(order.getFirstChildAttribute(), order.getLastChildAttribute());

                    Sort.inMemorySort(res, att);
                }

                if (print) {
                    // change this to output to the stream in Agent
                    final storageManager.Schema schema = newSchema;
                    for (String _s : schema.getFieldNames()) {
                        System.out.print(_s);
                        System.out.print('\t');
                    }
                    System.out.println();
                    for (storageManager.Tuple t : res) {
                        System.out.println(t);
                    }
                    return "";
                } else {
                    // there is a more efficent way to do this.
                    Insert insert = new Insert(memory, manager);
                    for (storageManager.Tuple t : res) {
                        insert.insertTuple(tempTableName, 0, t);
                    }
                    return tempTableName;
                }
            }
            else {
                if(distinct) {
//                    System.err.println("Distinct on larget single tables is not implemented");
//                    this could be cheated
                    Distinct.execute(tempTableName, manager, memory);
                }

                Sort.sort(memory, out, order);

                if(print) {
                    Helpers.printTable(tempTableName, manager, memory, null);
                    return "";
                }
                else
                    return tempTableName;
            }
        }
        else {
            // to reduce the complexity of this code, I am going to assume that is to large to fit into memory
            List<NaturalJoinInfo> info = new ArrayList<>();
            boolean useNaturalJoin = check_schema_natural_join(tables, info);

            System.out.println(useNaturalJoin);
            for(NaturalJoinInfo i : info) {
                System.out.println(i);
            }
            // We should figure out how to move the selections to before we perform the join
            // use cross join
            if (useNaturalJoin) {
                // use the natural join algorithm and apply the condition to it
                Join.naturalJoin(manager, memory, tables, info, selections, where, 0);
                where = null;
            } else {
//                System.out.println("Performing cross join");
                Join.crossJoin(manager, memory, tables);
            }

            temptable = TemporaryTables.last();
            storageManager.Relation relation = manager.getRelation(temptable);

            Schema newSchema = Projection.get_schema(selections, relation.getSchema(), false);
            String outputTable = TemporaryTables.new_temporary_table(newSchema);
            storageManager.Relation out = manager.getRelation(outputTable);
//            Helpers.printTable(temptable, manager, memory, null);
            Projection.execute(manager, memory, relation, out, where, true);

            // distinct
            if (distinct)
                Distinct.execute(outputTable, manager, memory);

            // order
            Sort.sort(memory, out, order);

            if (print)
                Helpers.printTable(temptable, manager, memory, null);
            else
                return temptable;
        }
        return "";
    }



    boolean check_schema_natural_join(List<String> tables, List<NaturalJoinInfo> info) {
        List<storageManager.Relation> relations = new ArrayList<>();
        for(String s : tables)
            relations.add(manager.getRelation(s));

        List<NaturalJoinInfo> joinInfo = Helpers.possible_join_pairs(tables, manager);
        info.addAll(joinInfo);

        ArrayList<String> relationOrder = new ArrayList<>();
        for(NaturalJoinInfo i : info) {
            String a = i.tableS;
            String b = i.tableL;
            if(relationOrder.contains(a) && !relationOrder.contains(b)) {
                relationOrder.add(b);
            }
            else if(!relationOrder.contains(a) && !relationOrder.contains(b) && relationOrder.isEmpty()) {
                relationOrder.add(a);
                relationOrder.add(b);
            }
        }

        // special case when there are only 2 tables
        if(joinInfo.size() == 2 && tables.size() == 2) {
            return joinInfo.get(0).valid;
        }
        else if(relationOrder.size() == tables.size())
            return true;
        return false;
    }


    boolean check_where_natural_join(List<Statement> equals) {
        if(equals.isEmpty())
            return false;

        for(int i = 0; i < equals.size(); ++i) {
            Statement s = equals.get(i);
            Statement rhs = s.getFirstChild();
            Statement lhs = s.getLastChild();

            if(rhs.getLastChildAttribute().equals(lhs.getLastChildAttribute())) {
                return i == 0; // for now it can only be the first condition
            }
        }
        return false;
    }
}
