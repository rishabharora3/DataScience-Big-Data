package edu.rit.ibd.a3;

import java.util.TreeSet;

public class FunctionalDependency {
    private TreeSet<String> leftAtr;
    private String rightAtr;

    public FunctionalDependency(TreeSet<String> leftAtr, String rightAtr) {
        this.leftAtr = leftAtr;
        this.rightAtr = rightAtr;
    }

    public TreeSet<String> getLeftAtr() {
        return leftAtr;
    }

    public void setLeftAtr(TreeSet<String> leftAtr) {
        this.leftAtr = leftAtr;
    }

    public String getRightAtr() {
        return rightAtr;
    }

    public void setRightAtr(String rightAtr) {
        this.rightAtr = rightAtr;
    }
}
