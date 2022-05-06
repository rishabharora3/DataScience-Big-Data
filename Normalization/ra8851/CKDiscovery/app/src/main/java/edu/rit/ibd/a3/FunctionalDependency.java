package edu.rit.ibd.a3;


import java.util.Set;

public class FunctionalDependency {
    private Set<String> leftAtr;
    private Set<String> rightAtr;

    public FunctionalDependency(Set<String> leftAtr, Set<String> rightAtr) {
        this.leftAtr = leftAtr;
        this.rightAtr = rightAtr;
    }

    public Set<String> getLeftAtr() {
        return leftAtr;
    }

    public void setLeftAtr(Set<String> leftAtr) {
        this.leftAtr = leftAtr;
    }

    public Set<String> getRightAtr() {
        return rightAtr;
    }

    public void setRightAtr(Set<String> rightAtr) {
        this.rightAtr = rightAtr;
    }
}
