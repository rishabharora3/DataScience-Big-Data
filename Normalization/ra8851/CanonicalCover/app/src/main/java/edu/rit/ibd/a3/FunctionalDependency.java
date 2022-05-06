package edu.rit.ibd.a3;


import java.util.TreeSet;

public class FunctionalDependency {
    private TreeSet<String> leftAtr;
    private TreeSet<String> rightAtr;

    public FunctionalDependency(TreeSet<String> leftAtr, TreeSet<String> rightAtr) {
        this.leftAtr = leftAtr;
        this.rightAtr = rightAtr;
    }

    public TreeSet<String> getLeftAtr() {
        return leftAtr;
    }

    public void setLeftAtr(TreeSet<String> leftAtr) {
        this.leftAtr = leftAtr;
    }

    public TreeSet<String> getRightAtr() {
        return rightAtr;
    }

    public void setRightAtr(TreeSet<String> rightAtr) {
        this.rightAtr = rightAtr;
    }
}
