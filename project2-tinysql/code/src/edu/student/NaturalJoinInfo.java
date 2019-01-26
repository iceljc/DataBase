package edu.student;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class NaturalJoinInfo {
    public String tableS;
    public String tableL;
    public List<String> common; // there can be multiple common fields
    public List<String> result_columns;
    public boolean valid = false;


    NaturalJoinInfo(String ts, String tl, List<String> c, List<String> res_columns) {
        tableS = ts;
        tableL = tl;
        common = c;
        result_columns = res_columns;

        if(!c.isEmpty())
            valid = true;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(tableS);
        builder.append("|");
        builder.append(tableL);
        builder.append(" common: ");
        for(String column : common) {
            builder.append(column);
            builder.append(", ");
        }
        builder.append("columns: ");
        for(String column : result_columns) {
            builder.append(column);
            builder.append(", ");
        }
        builder.append(valid);

        return builder.toString();
    }
}
