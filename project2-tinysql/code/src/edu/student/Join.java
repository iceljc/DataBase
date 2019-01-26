package edu.student;
// tuples after cross join are in the "resultTuples" list


//import org.jetbrains.annotations.NotNull;
import storageManager.Schema;
import storageManager.SchemaManager;
import storageManager.MainMemory;
import storageManager.Relation;
import storageManager.Block;
import storageManager.Tuple;
import storageManager.Field;
import storageManager.FieldType;

import javax.swing.plaf.synth.SynthTextAreaUI;
import java.util.*;


public class Join {

//    static class SubList {
//        int start;
//        int len;
//        int memoryIndex;
//        int current;
//        Block block;
//        Relation relation;
//        private MainMemory memory;
//
//        SubList(int start, int len, int index, Relation relation, MainMemory memory) {
//            this.start = start;
//            this.len = len;
//            this.memoryIndex = index;
//            this.memory = memory;
//            this.current = 0;
//            this.relation = relation;
//            this.block = null;
//
//            load();
//        }
//
//        void load() {
//            if(current >= len) {
//                block = null;
//                return;
//            }
//
//            relation.getBlock(start + current, memoryIndex);
//            block = memory.getBlock(memoryIndex);
//        }
//
//        void update() {
//            if(block.isEmpty()) {
//                current++;
//                load();
//            }
//        }
//
//        ArrayList<Tuple> getTuples() {
//            return block.getTuples();
//        }
//
//        ArrayList<Tuple> getAllTuples() {
//            int oldCurr = current;
//            ArrayList<Tuple> tuples = new ArrayList<>();
//
//            while(current < len) {
//                current++;
//            }
//            current = oldCurr;
//            return tuples;
//        }
//
//        boolean isLoaded() {
//            return block != null;
//        }
//    }

	// cross product between two relations
    // to access the result of the is join, get the last temporary table created.
	public static void crossJoin(SchemaManager schemaManager, MainMemory memory, ArrayList<String> tableNames) {
		if (tableNames.size() == 2) {
			String table1 = tableNames.get(0);
			String table2 = tableNames.get(1);

			Relation relation1 = schemaManager.getRelation(table1);
			Relation relation2 = schemaManager.getRelation(table2);

			ArrayList<String> newTableNames = new ArrayList<>();

			Relation smaller_relation = null;

			if (relation1.getNumOfBlocks() < relation2.getNumOfBlocks() - 1) {
				newTableNames.add(table1);
				newTableNames.add(table2);
				smaller_relation = relation1;
			} else {
				newTableNames.add(table2);
				newTableNames.add(table1);
				smaller_relation = relation2;
			}

			if (smaller_relation.getNumOfBlocks() < memory.getMemorySize() - 1) {
				// memory is large enough to contain the smaller table
				onePass(schemaManager, memory, newTableNames);
			} else {
				// memory is not large enough to contain the smaller table
				nestedLoop(schemaManager, memory, newTableNames);
			}

		}
		else  {
		    tableNames = Helpers.optimize_cross_join_order(tableNames, schemaManager);
            multiCrossJoin(schemaManager, memory, tableNames);
        }

	}

	// A x B x C x D x ...
	public static void multiCrossJoin(SchemaManager manager, MainMemory memory, ArrayList<String> relationNames) {

		String t1 = relationNames.get(0);
		relationNames.remove(0);
		String t2 = relationNames.get(0);
        relationNames.remove(0);

        ArrayList<String> initalJoinNames = new ArrayList<>(Arrays.asList(t1, t2));

        // i am letting cross join handle the creation of the schema
        crossJoin(manager, memory, initalJoinNames);

        for(String t : relationNames) {
            String tableName = TemporaryTables.last();

            ArrayList<String> joinNames = new ArrayList<>(Arrays.asList(t, tableName));

            crossJoin(manager, memory, joinNames);
        }
	}

	public static void multiNaturalJoin(SchemaManager manager, MainMemory memory, ArrayList<String> relationNames, List<NaturalJoinInfo> info, List<TableAttribute> selections, Statement where) {
//	    ArrayList<String> relationOrder = Helpers.get_join_order(relationNames, manager);

        ArrayList<String> relationOrder = new ArrayList<>();
        List<Integer> infoOrder = new ArrayList<>();
        for(int j = 0; j < info.size(); ++j) {
            NaturalJoinInfo i = info.get(j);
            String a = i.tableS;
            String b = i.tableL;
            if(relationOrder.contains(a) && !relationOrder.contains(b)) {
                relationOrder.add(b);
                infoOrder.add(j);
            }
            else if(!relationOrder.contains(a) && !relationOrder.contains(b) && relationOrder.isEmpty()) {
                relationOrder.add(a);
                relationOrder.add(b);
                infoOrder.add(j);
            }
        }

        ArrayList<String> previousTables = new ArrayList<>();

        String a = relationOrder.get(0);
        relationOrder.remove(0);

        String b = relationOrder.get(0);
        relationOrder.remove(0);

//        NaturalJoinInfo i = Helpers.find_info(info, a, b);


        naturalJoin(manager, memory, new ArrayList<>(List.of(a, b)), info, selections, where, infoOrder.get(0));
        infoOrder.remove(0);
        Helpers.printTable(TemporaryTables.last(), manager, memory, null);
        for(String t : relationOrder) {
            String temptable = TemporaryTables.last();
            if(temptable == null) {
                System.err.println("Join Error: Failed to get previous temporary table");
                break;
            }
            naturalJoin(manager, memory, new ArrayList<>(List.of(temptable, t)), info, selections, where, infoOrder.get(0));
            infoOrder.remove(0);
        }
    }

	public static void naturalJoin(SchemaManager manager, MainMemory memory, ArrayList<String> tableNames, List<NaturalJoinInfo> info, List<TableAttribute> selections, Statement where, int infoIndex) {
        if (tableNames.size() == 2) {
            String table1 = tableNames.get(0);
            String table2 = tableNames.get(1);

            Relation relation1 = manager.getRelation(table1);
            Relation relation2 = manager.getRelation(table2);

            ArrayList<String> newTableNames = new ArrayList<>();

            Relation smaller_relation = null;

            if (relation1.getNumOfBlocks() < relation2.getNumOfBlocks()) {
                smaller_relation = relation1;
            } else {
                smaller_relation = relation2;
            }
            // this size of info should be two

            NaturalJoinInfo join = info.get(infoIndex);

            List<String> common = join.common;

            String joinColumn = "";

            // if there is only one table in common then remove the where clause that consists of
            // that table if there is one.
            if(common.size() == 1) {
                joinColumn = common.get(0);
            }
            else {
                if(where != null) {
                    // this is an attempt to find the join key
                    List<Statement> equals = where.searchAllChildren("=");
                    for(Statement s : equals) {
                        Statement rhs = s.getFirstChild();
                        Statement lhs = s.getLastChild();
                        String ltableName = lhs.getFirstChildAttribute();
                        String rtableName = rhs.getFirstChildAttribute();

                        if(ltableName.equalsIgnoreCase(rtableName))
                            continue;

                        if(lhs.getLastChildAttribute().equalsIgnoreCase(rhs.getLastChildAttribute())) {
                            joinColumn = lhs.getLastChildAttribute();
                            if(s == where)
                                where = null;
                            else {
                                where.removeBranch(s);
                                if(where.getBranchSize() == 0)
                                    where = null;
                            }
                            break;
                        }
                    }

                    if(joinColumn.isEmpty())
                        joinColumn = common.get(0);
                }
                else
                    // though could go through an optimization that selects the best column
                    joinColumn = common.get(0);
            }

            Relation small = smaller_relation;
//            System.out.println("Join Column: " + joinColumn);
            if(small.getNumOfBlocks() <= memory.getMemorySize() - 2) {
                Join.onePhaseNaturalJoin(memory, manager, tableNames, joinColumn, selections, where);
            }
            else {
//                        System.err.println("Natural Join Error: Two pass not implemented yet");
//                        System.exit(2);
                Join.nestedNaturalJoin(memory, manager, tableNames, joinColumn, selections, where);
            }
        }
        else {
            System.err.println("Join: Multiple table natural join not implemented");
            // the where isnt used right now.
            multiNaturalJoin(manager, memory, tableNames, info, selections, where);
        }
    }

	// recursive 
	private static void getResultTupleList(List<Tuple> resultTuples, List<Tuple> prevTupleList, int relationIndex, Relation tempRelation, SchemaManager manager, MainMemory memory, ArrayList<String> relationNames) {
		String relationName = relationNames.get(relationIndex);
		Relation relation = manager.getRelation(relationName);

		for (int i = 0; i < relation.getNumOfBlocks(); ++i) {
			// read the i-th block from current relation and store it in the i-th block in memory
			relation.getBlock(i, relationIndex);
			Block b = memory.getBlock(relationIndex);

			for (int j = 0; j < b.getNumTuples(); ++j) {
				Tuple t = b.getTuple(j);
				List<Tuple> curTupleList = new ArrayList<>();

				if (relationIndex != 0) {
					for (Tuple tuple : prevTupleList) {
						curTupleList.add(tuple);
					}
				}

				curTupleList.add(t);

				if (relationIndex == relationNames.size() - 1) {
					resultTuples.add(combineTuples(tempRelation, curTupleList));
				} else {
					getResultTupleList(resultTuples, curTupleList, relationIndex + 1, tempRelation, manager, memory, relationNames);
				}
			}
		}
	}




	private static void onePass(SchemaManager manager, MainMemory memory, ArrayList<String> relationNames) {
		String smallRelationName = relationNames.get(0);
		String largeRelationName = relationNames.get(1);

		Relation smallRelation = manager.getRelation(smallRelationName);
		Relation largeRelation = manager.getRelation(largeRelationName);


		// create a temporary schema and temporary relation
		Schema tempSchema = createSchema(manager, relationNames);
		String tempRelationName = TemporaryTables.new_temporary_table(tempSchema);

		Relation tempRelation = manager.getRelation(tempRelationName);

		// read all the blocks of the smaller relation to the main memory
		// in main memory, store those blocks from index 0
		int blockNum_smallRelation = smallRelation.getNumOfBlocks();
		smallRelation.getBlocks(0, 0, blockNum_smallRelation);

		// get tuples from the smaller relation
		List<Tuple> tuplesFromSmallRelation = memory.getTuples(0, blockNum_smallRelation);

		int index_lastBlockInMem = memory.getMemorySize() - 1;

		List<Tuple> resultTuples = new ArrayList<>();

		for (int i=0; i<largeRelation.getNumOfBlocks(); ++i) {
			// read one block from the larger relation and store it in the last block in memory
			largeRelation.getBlock(i, index_lastBlockInMem);
			Block b = memory.getBlock(index_lastBlockInMem);

			for (Tuple ts : tuplesFromSmallRelation) {
				for (Tuple tr : b.getTuples()) {
					if (!ts.isNull() && !tr.isNull()) {
						appendTupleList(resultTuples, tempRelation, ts, tr);
					}
				}
			}

			// 
		}

		// I know this is slow but I don't care right now.
		Insert insert = new Insert(memory, manager);
		for(Tuple t : resultTuples) {
			insert.insertTuple(tempRelationName, 0, t);
		}
	}




	private static void nestedLoop(SchemaManager manager, MainMemory memory, ArrayList<String> relationNames) {
		String relationA_name = relationNames.get(0);
		String relationB_name = relationNames.get(1);

		Relation relationA = manager.getRelation(relationA_name);
		Relation relationB = manager.getRelation(relationB_name);

		// create a temporary schema and temporary relation
		Schema tempSchema = createSchema(manager, relationNames);
		String tempRelationName = TemporaryTables.new_temporary_table(tempSchema);
		Relation tempRelation = manager.getRelation(tempRelationName);


		List<Tuple> resultTuples = new ArrayList<>();

		for (int i=0; i<relationA.getNumOfBlocks(); ++i) {
			// read one block from relationA and store it in block 0 of memory
			relationA.getBlock(i, 0);
			Block blockA = memory.getBlock(0);

			for (int j=0; j<relationB.getNumOfBlocks(); ++j) {
				// read one block from relationB and store it in block 0 of memory
				relationB.getBlocks(j, 1, 1);
				Block blockB = memory.getBlock(1);

				for (Tuple ta : blockA.getTuples()) {
					for (Tuple tb : blockB.getTuples()) {
						if (!ta.isNull() && !tb.isNull()) {
							appendTupleList(resultTuples, tempRelation, ta, tb);
						}
					}
				}

			}

		}

	}


    public static void nestedNaturalJoin(MainMemory memory, SchemaManager manager, List<String> tables,
                                         String joinColumn, List<TableAttribute> selections, Statement where) {
        System.out.println("Using Simple nested Natural Join");
        int smallIndex, largeIndex;

        Relation f = manager.getRelation(tables.get(0));
        Relation s_ = manager.getRelation(tables.get(1));

        if(f.getNumOfBlocks() < s_.getNumOfBlocks()) {
            smallIndex = 0;
            largeIndex = 1;
        }
        else {
            smallIndex = 1;
            largeIndex = 0;
        }


        String smallName = tables.get(smallIndex);
        String largeName = tables.get(largeIndex);
        Relation small = manager.getRelation(smallName);
        Relation large = manager.getRelation(largeName);

        Schema newSchema = null;
        boolean largeIsJoin = false;
        if(largeName.contains("__")) {
            newSchema = createNaturalSchema(manager, List.of(largeName, smallName), joinColumn, selections);
            largeIsJoin = true;
        }
        else
            newSchema = createNaturalSchema(manager, tables, joinColumn, selections);

//        System.out.println("Schema from the natural join");
//        System.out.println(newSchema);
//        System.out.println(newSchema);
        String tempTable = TemporaryTables.new_temporary_table(newSchema);

        Relation out = manager.getRelation(tempTable);
        TableAttribute att = new TableAttribute(joinColumn);

        // compute an optimal block distribution
        // since this algorithm is inefficent, we want to use the blocks as efficently as possible

        int sizeSmall = small.getNumOfBlocks();
        int sizeLarge = large.getNumOfBlocks();
        System.out.println(sizeSmall);
        System.out.println(sizeLarge);
        System.out.println(joinColumn);
        // leave one for output
        int numBlocks = memory.getMemorySize() - 1;

        float sizeRatio = 0;
        if(sizeSmall == sizeLarge)
            sizeRatio = 0.5f;
        else
            sizeRatio = (float) sizeSmall / (float) sizeLarge;

        if(sizeRatio > 0.8f)
            sizeRatio = 0.5f;

        // the number of block each relation can use
        int numLargeBlocks = (int) Math.floor(sizeRatio * numBlocks);
        int numSmallBlocks = numBlocks - numLargeBlocks;
        System.out.println(sizeRatio);
        System.out.println(numSmallBlocks);
        System.out.println(numLargeBlocks);
//        System.exit(1);

        int outputMemoryIndex =  0;

        Block output = memory.getBlock(outputMemoryIndex);
        output.clear();
        int resBlock = 0;
        for(int currentSmallBlock = 0; currentSmallBlock < small.getNumOfBlocks();) {
            int smallRead = Math.min(numSmallBlocks, sizeSmall);
            if(smallRead <= 0)
                break;
            small.getBlocks(currentSmallBlock, 1, smallRead);
            List<Tuple> smallTuples = memory.getTuples(1, smallRead);
            for (int currentLargeBlock = 0; currentLargeBlock < large.getNumOfBlocks();) {
                int largeRead = Math.min(numLargeBlocks, sizeLarge);
                if(largeRead <= 0)
                    break;

                large.getBlocks(currentLargeBlock, 1 + numSmallBlocks, largeRead);
                List<Tuple> largeTuples = memory.getTuples(1 + numSmallBlocks, largeRead);

                for(Tuple s : smallTuples) {
                    for(Tuple l : largeTuples) {
                        if(Sort.compareTuple(s, l, att) == 0) {
                            Tuple res = null;
                            if(smallIndex == 0)
                                res = joinTuples(manager, s, l, out, joinColumn);
                            else
                                res = joinTuples(manager, l, s, out, joinColumn);
                            if(res == null)
                                continue;
                            if(where == null || Expression.evaluateBoolean(where, res)) {
                                if(output.isFull()) {
                                    out.setBlock(resBlock++, outputMemoryIndex);
                                    output.clear();
                                }
                                output.appendTuple(res);
                            }
                        }
                    }
                }

                currentLargeBlock += largeRead;
                sizeLarge -= largeRead;
            }
            currentSmallBlock += smallRead;
            sizeSmall -= smallRead;
        }
        if(!output.isEmpty())
            out.setBlock(resBlock++, outputMemoryIndex);
        output.clear();
    }

//    public static void twoSimplePhaseNaturalJoin(@NotNull MainMemory memory, @NotNull SchemaManager manager, @NotNull List<String> tables, String joinColumn) {
//        System.out.println("Using Simple Two Phase Natural Join");
//        String smallName = tables.get(0);
//        String largeName = tables.get(1);
//        Relation small = manager.getRelation(smallName);
//        Relation large = manager.getRelation(largeName);
//
//        Schema newSchema = createNaturalSchema(manager, tables, joinColumn, selections);
////        System.out.println("Schema from the natural join");
////        System.out.println(newSchema);
//        String outName = TemporaryTables.new_temporary_table(newSchema);
//        Relation out = manager.getRelation(outName);
//        TableAttribute att = new TableAttribute(joinColumn);
//
//        Sort.twoPassSort(small, att, memory);
//        Sort.twoPassSort(large, att, memory);
//
//
//        int smallMemoryIndex = 1;
//        int largeMemoryIndex = 2;
//
//        int outputMemoryIndex =  0;
//
//        Block output = memory.getBlock(outputMemoryIndex);
//
//        for(int currentSmallBlock = 0; currentSmallBlock < small.getNumOfBlocks(); ++currentSmallBlock) {
//            ArrayList<Tuple> minList = memory.getTuples(smallMemoryIndex, 1);
//            Tuple min = minList.get(0);
//            for(int currentLargeBlock = 0; currentLargeBlock < large.getNumOfBlocks(); ++largeMemoryIndex) {
//                ArrayList<Tuple> largeList = memory.getTuples(largeMemoryIndex, 1);
//                Tuple minLarge = minList.get(0);
//                if(Sort.compareTuple(min, minLarge, att) == 0) {
//
//                }
//            }
//        }
//    }

    public static ArrayList<Tuple> collectTuplesBy(Relation relation, int startBlock, int numBlocks, int memIndex, Tuple tuple, TableAttribute att, MainMemory memory) {
        int size = relation.getNumOfBlocks();
        size = size - startBlock;

//        size = Math.min(size, numBlocks);

        ArrayList<Tuple> tuples = new ArrayList<>();

        while(size > 0) {
            int readSize = Math.min(size, numBlocks);
            relation.getBlocks(startBlock, memIndex, readSize);
            List<Tuple> b = memory.getTuples(memIndex, readSize);
            for(Tuple _t : b) {
                if(Sort.compareTuple(tuple, _t, att) == 0) {
                    tuples.add(_t);
                }
            }
        }
        return tuples;
    }

//    public static void twoPhaseNaturalJoin(MainMemory memory, @NotNull SchemaManager manager, @NotNull List<String> tables, String joinColumn) {
//        System.out.println("Using Two Phase Natural Join");
//        String smallName = tables.get(0);
//        String largeName = tables.get(1);
//        Relation small = manager.getRelation(smallName);
//        Relation large = manager.getRelation(largeName);
//
//        Schema newSchema = createNaturalSchema(manager, tables, joinColumn);
//        System.out.println("Schema from the natural join");
//        System.out.println(newSchema);
//        String outName = TemporaryTables.new_temporary_table(newSchema);
//        Relation out = manager.getRelation(outName);
//
//        // phase one of the 2 pass algorithm
//        Helpers.phaseOnePass(memory, manager, tables, new TableAttribute(joinColumn));
//
//        // compute the sublist chunks for each schema
//        int memSize = memory.getMemorySize();
//
//        int smallSize = small.getNumOfBlocks();
//        int largeSize = large.getNumOfBlocks();
//        //                  the number of jull lists + a list for the remainder if needed
//        int numSmallList = smallSize / memSize + (smallSize % memSize == 0 ? 0 : 1);
//        int numLargeList = largeSize / memSize + (largeSize % memSize == 0 ? 0 : 1);
//
//        // for this algorithm they must fit in memory
//        assert numSmallList + numLargeList < memSize;
//
//        List<SubList> smallLists = new ArrayList<>();
//        List<SubList> largeLists = new ArrayList<>();
//
//        for(int i = 0; i < numSmallList; ++i) {
//            int start = i * memSize;
//            int length = Math.min(smallSize - start, memSize);
//            smallLists.add(new Join.SubList(start, length, i, small, memory));
//        }
//
//        for(int i = 0; i < numLargeList; ++i) {
//            int start = i * memSize;
//            int length = Math.min(largeSize - start, memSize);
//            int memloc = numSmallList + i;
//            largeLists.add(new Join.SubList(start, length, memloc, large, memory));
//        }
//
//        List<SubList> allLists = new ArrayList<>();
//        allLists.addAll(smallLists);
//        allLists.addAll(largeLists);
//
//        int outputIndex = numLargeList + numSmallList;
//        assert outputIndex < 10;
//
//        Block output = memory.getBlock(outputIndex);
//        TableAttribute att = new TableAttribute(joinColumn);
//        output.clear();
//        Tuple largeMin = getMinFromSubLists(largeLists);
//        Tuple smallMin = getMinFromSubLists(smallLists);
//        // while both lists contain sub lists
//        int resBlock = 0;
//        while(!largeLists.isEmpty() && !smallLists.isEmpty()) {
//            System.out.println("LargeList: " + largeLists.size() + " SmallList: " + smallLists.size());
//            System.out.println("LMin: " + largeMin + " SMin: " + smallMin);
//            int res = Sort.compareTuple(largeMin, smallMin, att);
//            if(res == 1) {
//                System.out.println("Removing from smalllist");
//                // // smallMin is the lesser tuple
//                removeAllFromSubList(smallLists, smallMin, att);
//                largeMin = getMinFromSubLists(largeLists);
//            }
//            else if(res == -1) {
//                System.out.println("Removing from largelist");
//                // largeMin is the greater tuple
//                removeAllFromSubList(largeLists, largeMin, att);
//                largeMin = getMinFromSubLists(largeLists);
//            }
//            else {
//                // join the tuples
//                List<Tuple> lList = collectTuples(largeLists, largeMin, att);
//                List<Tuple> sList = collectTuples(smallLists, smallMin, att);
//                for(Tuple ll : lList) {
//                    for(Tuple sl : sList) {
//                        if(output.isFull()) {
//                            out.setBlock(resBlock++, outputIndex);
//                            output.clear();
//                        }
//                        output.appendTuple(createJoinedTuple(ll, sl, out, joinColumn));
//                    }
//                }
//                if(!output.isEmpty())
//                    out.setBlock(resBlock++, outputIndex);
//                output.clear();
//                // set the min for the next iteration
//                largeMin = getMinFromSubLists(largeLists);
//                smallMin = getMinFromSubLists(smallLists);
//
//            }
//        }
//
//    }
//
//    public static Tuple getMinFromSubLists(List<SubList> sublists) {
//        List<Tuple> list = new ArrayList<>();
//        for(SubList l : sublists) {
//            l.update();
//
//            if(l.isLoaded()) {
//                list.add(l.getTuples().get(0));
//            }
//            else
//                // the list is empty remove it
//                sublists.remove(l);
//        }
//
//        Sort.inMemorySort(list, null);
//        return list.get(0);
//    }
//
//    public static void removeAllFromSubList(List<SubList> subLists, Tuple tuple, TableAttribute att) {
//        for (SubList l : subLists) {
//            ArrayList<Tuple> tuples = l.getTuples();
//            int s = tuples.size();
//            for (int i = 0; i < tuples.size();) {
//                if(Sort.compareTuple(tuple, tuples.get(i), att) == 0) {
//                    tuples.remove(i);
//                }
//                else
//                    ++i;
//            }
//            System.out.println("Removed: " + (s - tuples.size()));
//            l.block.setTuples(tuples);
//        }
//    }
//
//    public static List<Tuple> collectTuples(List<SubList> subLists, Tuple tuple, TableAttribute att) {
//        System.out.println("Collect Tuples");
//        List<Tuple> res = new ArrayList<>();
//        for (SubList l : subLists) {
//            ArrayList<Tuple> tuples = l.getTuples();
//            for (int i = 0; i < tuples.size();) {
////                System.out.println(tuples.size());
//                if(l.getTuples().isEmpty())
//                    break;
//                if(Sort.compareTuple(tuple, tuples.get(i), att) == 0) {
////                    System.out.println("Found common Tuple: " + tuples.get(i));
//                    // add it to the results list
//                    res.add(tuples.get(i));
//                    // remove it from the block
//                    tuples.remove(i);
//                }
//                else
//                    ++i;
//            }
//
////            update the content of the block to the new set of tuples
//            l.block.setTuples(tuples);
//        }
//        System.out.println("Collect Tuples Done");
//        return res;
//    }




    public static void onePhaseNaturalJoin(MainMemory memory, SchemaManager manager, List<String> tables, String joinColumn, List<TableAttribute> selections, Statement where) {
	    System.out.println("Using One Phase Natural Join");
	    int smallIndex, largeIndex;

	    Relation f = manager.getRelation(tables.get(0));
        Relation s = manager.getRelation(tables.get(1));

        if(f.getNumOfBlocks() < s.getNumOfBlocks()) {
            smallIndex = 0;
            largeIndex = 1;
        }
        else {
            smallIndex = 1;
            largeIndex = 0;
        }


        String smallName = tables.get(smallIndex);
        String largeName = tables.get(largeIndex);
        Relation small = manager.getRelation(smallName);
        Relation large = manager.getRelation(largeName);

        // build the output table
        Schema newSchema = null;
        boolean largeIsJoin = false;
        if(largeName.contains("__")) {
            newSchema = createNaturalSchema(manager, List.of(largeName, smallName), joinColumn, selections);
            largeIsJoin = true;
        }
        else
            newSchema = createNaturalSchema(manager, tables, joinColumn, selections);
//        System.out.println("Schema from the natural join");
//        System.out.println(newSchema);
        String outName = TemporaryTables.new_temporary_table(newSchema);
        Relation out = manager.getRelation(outName);

        // memory variables
        // maximizing the amount of memory used by the large table
        int smallSize = memory.getMemorySize() - 2;
        int smallActual = Math.min(smallSize, small.getNumOfBlocks());
        int oMemoryIndex = 0; // output memory block index
        int lMemorySize = smallSize - smallActual - 1; // large table memory block index

        Block output = memory.getBlock(oMemoryIndex);
        output.clear();

        assert  lMemorySize >= 1;

        small.getBlocks(0, lMemorySize + 1, smallActual);

        List<Tuple> smallTuples = memory.getTuples(lMemorySize + 1, smallActual);

        int largeBlock = 0;
        int diskBlocks = 0;
        int largeBlocks = large.getNumOfBlocks();
//        System.out.println("Small Relation Name: " + small.getRelationName());
//        System.out.println("Large Relation Name: " + large.getRelationName());
//        Insert insert = new Insert(memory, manager);
        int relLoc = 0;
        while(largeBlocks > 0) {
            int readBlocks = Math.min(lMemorySize, largeBlocks);
            // get the tuples of the large relation
            large.getBlocks(diskBlocks, 1, readBlocks);
            // iterate through all of the tuples that are loaded
            List<Tuple> tuples = memory.getTuples(1, readBlocks);
            for(storageManager.Tuple to : tuples) {
                for(Tuple st : smallTuples) {
                    Tuple t = null;
                    if(smallIndex == 0)
                        t = joinTuples(manager, st, to, out, joinColumn);
                    else
                        t = joinTuples(manager, to, st, out, joinColumn);
                    if(t != null) {
                        if(where == null || Expression.evaluateBoolean(where, t)) {
                            // if the block is full flush it to disk and clear it
                            if (output.isFull()) {
//                                out.setBlock(out.getNumOfBlocks() == 0 ? 0 : out.getNumOfBlocks() - 1, oMemoryIndex);
                                out.setBlock(relLoc++, oMemoryIndex);
                                output.clear();
                            }
                            output.appendTuple(t);
                        }
                    }
                    // nothing to add, the join for this tuple failed
                }
            }
            largeBlocks -= readBlocks;
            diskBlocks += readBlocks;
        }
        // push the remaining content to the relation
//        System.out.println(output.getTuples().get(0));
//        System.out.println(output.getTuples().get(0).getSchema());
//        System.out.println(out.getSchema());
//        out.setBlock(out.getNumOfBlocks() == 0 ? 0 : out.getNumOfBlocks() - 1, oMemoryIndex);
        if(!output.isEmpty())
            out.setBlock(relLoc, oMemoryIndex);
    }

    private static Tuple joinTuples(SchemaManager manager, Tuple t1, Tuple t2, Relation out, String joinColumn) {
	    // both tuples should have the join column name
        storageManager.Schema s1 = t1.getSchema();
        storageManager.Field f1 = null;
        for(String s : s1.getFieldNames()) {
            if(s.contains(joinColumn)) {
                f1 = t1.getField(s);
            }
        }

        storageManager.Schema s2 = t2.getSchema();
        storageManager.Field f2 = null;
        for(String s : s2.getFieldNames()) {
            if(s.contains(joinColumn)) {
                f2 = t2.getField(s);
            }
        }
	    if(f1 == null || f2 == null)
            System.err.println("Join Error: Joining tuples that do not have a common field");

	    if(f1.type != f2.type)
	        return null;
	    if(f1.type == FieldType.INT) {
	        if(f1.integer == f2.integer) {
	            return createJoinedTuple(t1, t2, out, joinColumn);
            }
            else
                return null;
        }
        else {
            if(f1.str.compareTo(f2.str) == 0) {
                return createJoinedTuple(t1, t2, out, joinColumn);
            }
            else
                return null;
        }
    }

    static Tuple createJoinedTuple(Tuple t1, Tuple t2, Relation out, String joinColumn) {
	    System.out.println("Creating a jointed to from " + t1 + " and " + t2);
	    Tuple tout = out.createTuple();
//	    System.out.println("Outtuple: " + tout);

	    int offset = 0;
	    for(int i = 0; i < t1.getNumOfFields(); ++i) {
	        Field f = t1.getField(i);
//            System.out.println("Setting Offset1: " + offset);
	        if(f.type == FieldType.INT)
	           tout.setField(offset, f.integer);
            else
                tout.setField(offset, f.str);
            ++offset;
        }
        for(int i = 0; i < t2.getNumOfFields(); ++i) {
            Field f = t2.getField(i);
            Schema s = t2.getSchema();
//            if the column is the same as the join column name this ignore it.
            if(s.getFieldName(i).equalsIgnoreCase(joinColumn))
                continue;

//            System.out.println("Setting Offset2: " + offset);
            if(f.type == FieldType.INT)
                tout.setField(offset, f.integer);
            else
                tout.setField(offset, f.str);
            ++offset;
        }
        return tout;
    }




	// combine several relations into one schema 
	private static Schema createSchema(SchemaManager manager, List<String> relationNames) {
		ArrayList<String> newFieldNames = new ArrayList<>();
		ArrayList<FieldType> newFieldTypes = new ArrayList<>();

		for (String relation_name : relationNames) {
			Relation relation = manager.getRelation(relation_name);
			Schema schema = relation.getSchema();

			for (String field_name : schema.getFieldNames()) {
				if(field_name.contains("."))
					newFieldNames.add(field_name);
				else
                    newFieldNames.add(relation_name + '.' + field_name);
				newFieldTypes.add(schema.getFieldType(field_name));
			}
		}
		return new Schema(newFieldNames, newFieldTypes);
	}

	private static Schema createNaturalSchema(SchemaManager manager, List<String> names, String columns, List<TableAttribute> selections) {
        ArrayList<String> newFieldNames = new ArrayList<>();
        ArrayList<FieldType> newFieldTypes = new ArrayList<>();

        boolean column_add = false;
//        for (String relation_name : names) {
//            Relation relation = manager.getRelation(relation_name);
//            Schema schema = relation.getSchema();
//
//            for (TableAttribute t : selections) {
//                if (t.table.equalsIgnoreCase(relation_name)) {
//                    String n = t.to_string();
//                    if (n.contains(columns))
//                        column_add = true;
//                    newFieldNames.add(n);
//                    newFieldTypes.add(schema.getFieldType(t.attribute));
//                }
//            }
//        }

        for (String relation_name : names) {
            Relation relation = manager.getRelation(relation_name);
            Schema schema = relation.getSchema();

            for (String field_name : schema.getFieldNames()) {
                // if the column has been added then ignore it for the other tables
                // it will get the table name of the first table in the list
                if(field_name.contains(columns) && column_add)
                    continue;

                if(field_name.contains("."))
                    newFieldNames.add(field_name);
                else {
                    String name = relation_name + '.' + field_name;
                    if(newFieldNames.contains(name))
                        continue;
                    newFieldNames.add(name);

                }
                newFieldTypes.add(schema.getFieldType(field_name));

                if(field_name.contains(columns) && !column_add)
                    column_add = true;
            }
        }
        for(int i = 0; i < newFieldNames.size(); ++i) {
            System.out.println(newFieldNames.get(i) + " " + newFieldTypes.get(i));
        }

        return new Schema(newFieldNames, newFieldTypes);
    }

	// append tuple to resultTuples
	private static void appendTupleList(List<Tuple> resultTuples, Relation relation, Tuple ts, Tuple tr) {
		List<Tuple> newTupleList = new ArrayList<>();
		newTupleList.add(ts);
		newTupleList.add(tr);
		Tuple t = combineTuples(relation, newTupleList);
		resultTuples.add(t);
	}

	// combine several tuples in tupleList
	private static Tuple combineTuples(Relation relation, List<Tuple> tupleList) {
		Tuple newTuple = relation.createTuple();
		int offset = 0;

		for (Tuple t : tupleList) {
			for (int i = 0; i != t.getNumOfFields(); ++i, ++offset) {
				Field field = t.getField(i);
				if (field.type == FieldType.INT) {
					newTuple.setField(offset, field.integer);
				} else {
					newTuple.setField(offset, field.str);
				}
			}
		}

		return newTuple;

	}
}
