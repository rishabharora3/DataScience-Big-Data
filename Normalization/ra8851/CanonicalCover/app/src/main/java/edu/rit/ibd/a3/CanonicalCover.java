package edu.rit.ibd.a3;

import com.google.common.collect.Sets;

import java.io.File;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

public class CanonicalCover {

    public static void main(String[] args) throws Exception {
        String relation = args[0];
        String fdsStr = args[1];
        final String outputFile = args[2];

        // This stores the attributes of the input relation.
        relation = relation.trim().replaceAll(" +", "");
        String[] attrArray = relation.substring(relation.indexOf("(") + 1, relation.indexOf(")")).split(",");
        Set<String> attributes = new HashSet<>(Arrays.asList(attrArray));
        // This stores the functional dependencies provided as input. This will be the output as well.
        Set<FunctionalDependency> fds = new HashSet<>();
        fdsStr = fdsStr.trim().replaceAll(" +", "");
        String[] fdsArray = fdsStr.split(";");
        for (String fd : fdsArray) {
            String[] leftHandSide = fd.split("->")[0].split(",");
            String[] rightHandSide = fd.split("->")[1].split(",");
            System.out.println("left fd : " + Arrays.toString(leftHandSide));
            System.out.println("right fd : " + Arrays.toString(rightHandSide));
            fds.add(new FunctionalDependency(new TreeSet<>(Arrays.asList(leftHandSide)), new TreeSet<>(Arrays.asList(rightHandSide))));
        }


        // TODO 0: Your code here!
        //
        // Parse the input relation that include its attributes. Recall that relation and attribute names can be formed by multiple letters.
        //
        // Parse the input functional dependencies. Recall that attributes can be formed by multiple letters.
        //
        // Start infinite loop:
        //		Start infinite loop:
        //			thereWasAUnion = false
        //			For each pair of functional dependencies fi, fj (fi != fj):
        //				If lhs(fi)==lhs(fj): // The left-hand sides of fi and fj are the same.
        //					Remove fi
        //					Remove fj
        //					Union fi and fj and add to functional dependencies
        //					thereWasAUnion = true
        //			If thereWasAUnion == false:
        //				Break
        //		thereWereExtraneous = false
        //		For each fi in functional dependencies:
        //			For each attribute a in lhs(fi):
        //				If a is extraneous:
        //					Remove fi
        //					Add fi without a
        //					thereWereExtraneous = true
        //			For each attribute in rhs(fi):
        //				If a is extraneous:
        //					Remove fi
        //					Add fi without a
        //					thereWereExtraneous = true
        //		If thereWereExtraneous == false:
        //			Break


        // Parse relation.

        // Parse FDs.

        // Remove extraneous attributes, each one either in the left-hand or right-hand sides.

        boolean thereWasAUnion;
        boolean thereWereExtraneous;
        do {
            do {
                thereWasAUnion = false;
                Set<FunctionalDependency> fdsTemp = new HashSet<>(fds);
                for (FunctionalDependency dependency1 : fdsTemp) {
                    for (FunctionalDependency dependency2 : fdsTemp) {
                        if (dependency1 != dependency2) {
                            if (dependency1.getLeftAtr().equals(dependency2.getLeftAtr())) {
                                fds.remove(dependency1);
                                fds.remove(dependency2);
                                dependency1.getRightAtr().addAll(dependency2.getRightAtr());
                                fds.add(dependency1);
                                fdsTemp = new HashSet<>(fds);
                                thereWasAUnion = true;
                            }
                        }
                    }
                }
            } while (thereWasAUnion);

            thereWereExtraneous = false;

            for (FunctionalDependency dependency : fds) {

                //left
                if (dependency.getLeftAtr().size() > 1) {
                    Set<String> tempLeftSet1 = new HashSet<>(dependency.getLeftAtr());
                    for (String a : tempLeftSet1) {
                        Set<String> tempLeftSet2 = new HashSet<>(dependency.getLeftAtr());
                        tempLeftSet2.remove(a);
                        if (computeClosure(fds, tempLeftSet2).containsAll(dependency.getRightAtr())) {
                            dependency.getLeftAtr().remove(a);
                            tempLeftSet1 = new HashSet<>(dependency.getLeftAtr());
                            thereWereExtraneous = true;
                        }
                    }
                }

                //right
                if (dependency.getRightAtr().size() > 1) {
                    Set<String> tempRightSet1 = new HashSet<>(dependency.getRightAtr());
                    for (String a : tempRightSet1) {
                        Set<String> tempRightSet2 = new HashSet<>(dependency.getRightAtr());
                        tempRightSet2.remove(a);
                        Set<FunctionalDependency> tempFds = new HashSet<>(fds);
                        tempFds.remove(dependency);
                        tempFds.add(new FunctionalDependency(dependency.getLeftAtr(), new TreeSet<>(tempRightSet2)));
                        if (computeClosure(tempFds, dependency.getLeftAtr()).contains(a)) {
                            dependency.getRightAtr().remove(a);
                            tempRightSet1 = new HashSet<>(dependency.getRightAtr());
                            thereWereExtraneous = true;
                        }
                    }
                }
            }
        } while (thereWereExtraneous);
        // TODO 0: End of your code.

        PrintWriter writer = new PrintWriter(new File(outputFile));
        for (FunctionalDependency fd : fds) {
            String[] leftHandSide = String.join(",", fd.getLeftAtr()).split(",");
            String[] rightHandSide = String.join(",", fd.getRightAtr()).split(",");
            String output = String.join(", ", leftHandSide) + " -> " + String.join(", ", rightHandSide);
            writer.println(output);
        }
        writer.close();
    }

    private static Set<String> computeClosure(Set<FunctionalDependency> fds, Set<String> set) {
        Set<String> result = new HashSet<>(set);
        boolean change;
        do {
            change = false;
            for (FunctionalDependency dependency : fds) {
                for (int i = 1; i <= result.size(); i++) {
                    for (Set<String> coreCombo : Sets.combinations(result, i)) {
                        if (coreCombo.containsAll(dependency.getLeftAtr())) {
                            if (!result.containsAll(dependency.getRightAtr())) {
                                result.addAll(dependency.getRightAtr());
                                change = true;
                            }
                        }
                    }
                }
            }
        } while (change);
        return result;
    }

}
