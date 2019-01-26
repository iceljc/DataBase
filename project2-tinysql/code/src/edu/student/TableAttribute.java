package edu.student;

// For use in the selection of columns
class TableAttribute {
    public String table;
    public String attribute;
    public boolean is_star;

    public TableAttribute(String table, String attribute) {
        this.table = table;
        this.attribute = attribute;
    }

    public TableAttribute(String attribute) {
        this.table = "";
        this.attribute = attribute;
    }

    public TableAttribute() {
        this.table = "";
        this.attribute = "";
        this.is_star = true;
    }

    public String to_string() {
        if(is_star)
            return "*";
        if(table.isEmpty())
            return attribute;
        else
            return table + "." + attribute;
    }
}

