package edu.rit.ibd.a3;

import com.google.common.collect.Sets;

import java.io.File;
import java.io.PrintWriter;
import java.util.*;

public class TNFDecomposition {

    public static void main(String[] args) throws Exception {
        String relation = args[0];
        String fdsStr = args[1];
        String cksStr = args[2];
        final String outputFile = args[3];

        // This stores the attributes of the input relation.
        relation = relation.trim().replaceAll(" +", "");
        String[] attrArray = relation.substring(relation.indexOf("(") + 1, relation.indexOf(")")).split(",");
        Set<String> attributes = new HashSet<>(Arrays.asList(attrArray));
        // This stores the functional dependencies provided as input.
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
        // This stores the candidate keys provided as input.
        List<Set<String>> cks = new ArrayList<>();
        cksStr = cksStr.trim().replaceAll(" +", "");
        String[] cksArray = cksStr.split(";");
        for (String ck : cksArray) {
            String[] ckArray = ck.split(",");
            System.out.println("ck : " + Arrays.toString(ckArray));
            cks.add(new HashSet<>(Arrays.asList(ckArray)));
        }

        // This stores the final 3NF decomposition, i.e., the output.
        List<Set<String>> decomposition = new ArrayList<>();


        // TODO 0: Your code here!
        //
        // Parse the input relation that include its attributes. Recall that relation and attribute names can be formed by multiple letters.
        //
        // Parse the input functional dependencies. These are already a canonical cover. Recall that attributes can be formed by multiple letters.
        //
        // Parse the input candidate keys. Recall that attributes can be formed by multiple letters.
        //
        // Analyze whether the relation is already in 3NF:
        //	alreadyIn3NF=true
        //	For each FD A->B (A and B are sets of attributes):
        //		check = (B is included or equal in A) OR (A is superkey) OR (B \ A is contained in at least one candidate key)
        //		If !check: alreadyIn3NF=false, proceed to decompose!
        //	If alreadyIn3NF: Stop! We are done!
        //
        // Compute canonical cover of FDs (you must assume that the input is already a canonical cover).
        //
        // Decompose FDs in relations:
        //	For each FD A->B:
        //		Create a new relation ri(A union B) and add it to the decomposition
        //
        // Check at least one candidate key is present:
        //	ckWasPresent = false
        //	For each decomposed relation ri(X):
        //		If X is included in cks: ckWasPresent = true
        //	If !ckWasPresent:
        //		Add rj(Y) to the decomposition, where Y is any key in cks
        //
        // Remove redundant relations:
        //	For each ri(X) in decomposed relations:
        //		For each rj(Y) in decomposed relations:
        //			If X included or equal in Y:
        //				Remove ri(X) from the decomposition
        //				Start over checking all relations in the decomposition again

        // Parse relation.

        // Parse FDs.

        // Test whether already in 3NF.

        // If not in in3NF, decompose.

        boolean alreadyIn3NF = true;
        for (FunctionalDependency dependency : fds) {
            Set<String> rightMinusLeftSet = new HashSet<>(dependency.getRightAtr());
            rightMinusLeftSet.removeAll(dependency.getLeftAtr());
            boolean isContained = false;
            for (Set<String> candidateKey : cks) {
                if (candidateKey.containsAll(rightMinusLeftSet)) {
                    isContained = true;
                    break;
                }
            }
            boolean check = dependency.getLeftAtr().containsAll(dependency.getRightAtr()) ||
                    computeClosure(fds, dependency.getLeftAtr()).containsAll(attributes) || isContained;
            if (!check) {
                alreadyIn3NF = false;
                break;
            }
        }
        if (alreadyIn3NF) {
            decomposition.add(attributes);
        } else {
            for (FunctionalDependency dependency : fds) {
                Set<String> relationSet = new HashSet<>();
                relationSet.addAll(dependency.getLeftAtr());
                relationSet.addAll(dependency.getRightAtr());
                decomposition.add(relationSet);
            }

            boolean ckWasPresent = false;
            for (Set<String> relationInDec : decomposition) {
                for (Set<String> ck : cks) {
                    if (relationInDec.containsAll(ck)) {
                        ckWasPresent = true;
                        break;
                    }
                }
            }
            if (!ckWasPresent && !cks.isEmpty()) {
                decomposition.add(cks.get(0));
            }
            List<Set<String>> decompositionTemp = new ArrayList<>(decomposition);
            for (Set<String> decomposition1 : decompositionTemp) {
                for (Set<String> decomposition2 : decompositionTemp) {
                    if (decomposition1 != decomposition2) {
                        if (decomposition2.containsAll(decomposition1)) {
                            decomposition.remove(decomposition1);
                            decompositionTemp = new ArrayList<>(decomposition);
                        }
                    }
                }
            }
        }
        // TODO 0: End of your code.

        PrintWriter writer = new PrintWriter(new File(outputFile));
        for (Set<String> r : decomposition)
            writer.println("r(" + r.stream().sorted().collect(java.util.stream.Collectors.toList()).
                    toString().replace("[", "").replace("]", "") + ")");
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
