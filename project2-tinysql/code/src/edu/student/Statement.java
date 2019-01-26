package edu.student;

import javax.swing.plaf.nimbus.State;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;

// define a statement tree
public class Statement {
    private String attribute;
    private List<Statement> branch;

    public Statement(){
        this.branch = new ArrayList<>();
        this.branch.clear();
    }

    public Statement(String attribute) {
        this();
        this.attribute = attribute;
    }

    public String getAttribute() {
        return this.attribute;
    }

    public int getBranchSize() {
        return this.branch.size();
    }

    public List<Statement> getBranch() {
        return this.branch;
    }

    // get child and its attribute
    public Statement getChild(int k) {
        return this.branch.get(k);
    }

    public String getChildAttribure(int k) {
        return this.branch.get(k).attribute;
    }

    public Statement getFirstChild() {
        return this.branch.get(0);
    }

    public String getFirstChildAttribute() {
        return this.getFirstChild().getAttribute();
    }

    public Statement getLastChild() {
        return this.branch.get(getBranchSize()-1);
    }

    public String getLastChildAttribute() {
        return this.getLastChild().getAttribute();
    }

    // get leaf and its attibute
    public Statement getLeaf() {
        return this.getFirstChild();
    }

    public String getLeafAttribute() {
        return this.getFirstChildAttribute();
    }

    public void addNode(Statement statement) {
        this.branch.add(statement);
    }

    public Statement getByName(String attr) {
        for(Statement s : getBranch()) {
            if(s.getAttribute().equals(attr)){
                return s;
            }
        }
        return null;
    }

    // return the first occurance of the att
    public Statement searchChildren(String att) {
        for(Statement s : getBranch()) {
            if(s.attribute.equalsIgnoreCase(att))
                return s;
        }
        return null;
    }

    // finds all occurences of the att
    public List<Statement> searchAllChildren(String att) {
        List<Statement> rets = new ArrayList<>();
        if(attribute.equals(att))
            rets.add(this);
        for(Statement s : getBranch())
            rets.addAll(s.searchAllChildren(att));
        return rets;
    }


    // set attribute and branch
    public void setAttribute(String attribute) {
        this.attribute = attribute;
    }

    public void setBranch(List<Statement> branch) {
        this.branch = branch;
    }

    private static void indentby(int i) {
        String s = "";
        while(i-- >= 0) {
            s += "  ";
        }
        System.out.print(s);
    }


    // it should be the same reference
    public boolean removeBranch(Statement s) {
        for(Statement _s : getBranch()) {
            if(_s == s) {
                this.branch.remove(s);
                return true;
            }
        }
        for(Statement _s : getBranch()) {
            if(_s.removeBranch(s))
                return true;
        }
        return false;
    }



    public void print(int indent) {
        indentby(indent);

        System.out.print(attribute);
        System.out.println(" {");
        for(Statement s : branch) {
            s.print(indent + 1);
        }
        indentby(indent);
        System.out.println("}");
    }

}
