package edu.rit.ibd.a3;

import com.google.common.collect.Sets;

import java.io.File;
import java.io.PrintWriter;
import java.util.*;

public class CKDiscovery {

    public static void main(String[] args) throws Exception {
        String relation = args[0];
        String fdsStr = args[1];
        final String outputFile = args[2];

        // This stores the attributes of the input relation.
        // This stores the functional dependencies provided as input.
        Set<FunctionalDependency> fds = new HashSet<>();
        // This stores the candidate keys discovered; each key is a set of attributes.
        List<Set<String>> keys = new ArrayList<>();

        // TODO 0: Your code here!

        // Parse the input relation that include its attributes. Recall that relation and attribute names can be formed by multiple letters.
        //
        // Parse the input functional dependencies. Recall that attributes can be formed by multiple letters.
        //
        // For each attribute a, you must classify as case 1 (a is not in the functional dependencies), case 2 (a is only in the right-hand side),
        //	case 3 (a is only in the left-hand side), case 4 (a is in both left- and right-hand sides).
        //
        // Compute the core (cases 1 and 3) and check whether the core is candidate key based on closure.
        //
        // If the closure of the core does not contain all the attributes, proceed to combine attributes.
        //
        // For each combination of attributes starting from size 1 classified as case 4:
        //	X = comb union core
        //	If the closure of X contains all attributes of the input relation:
        //		X is superkey
        //		If X is not contained in a previous candidate key already discovered:
        //			X is a candidate key
        //	If all the combinations of size k are superkeys -> Stop

        // Parse attributes.
        relation = relation.trim().replaceAll(" +", "");
        String[] attrArray = relation.substring(relation.indexOf("(") + 1, relation.indexOf(")")).split(",");
        Set<String> attributes = new HashSet<>(Arrays.asList(attrArray));
        System.out.println("attributes = " + Arrays.toString(attrArray));

        // Parse FDs.
        fdsStr = fdsStr.trim().replaceAll(" +", "");
        String[] fdsArray = fdsStr.split(";");
        for (String fd : fdsArray) {
            String[] leftHandSide = fd.split("->")[0].split(",");
            String[] rightHandSide = fd.split("->")[1].split(",");
            System.out.println("left fd : " + Arrays.toString(leftHandSide));
            System.out.println("right fd : " + Arrays.toString(rightHandSide));
            fds.add(new FunctionalDependency(new HashSet<>(Arrays.asList(leftHandSide)), new HashSet<>(Arrays.asList(rightHandSide))));
        }

        // Discover candidate keys.

        // Split attributes by case.
        Set<String> case1 = new HashSet<>(), case2 = new HashSet<>(), case3 = new HashSet<>(), case4 = new HashSet<>();
        for (String attr : attributes) {
            boolean isLeft = false,
                    isRight = false;
            for (FunctionalDependency fd : fds) {
                if (isLeft && isRight)
                    break;
                if (!isLeft)
                    isLeft = fd.getLeftAtr().contains(attr);
                if (!isRight)
                    isRight = fd.getRightAtr().contains(attr);
            }
            if (!isLeft && !isRight) case1.add(attr);
            if (!isLeft && isRight) case2.add(attr);
            if (isLeft && !isRight) case3.add(attr);
            if (isLeft && isRight) case4.add(attr);
        }

        // Find the core.
        Set<String> core = new HashSet<>();
        core.addAll(case1);
        core.addAll(case3);

        // Compute the closure of the core.
        if (computeClosure(fds, core).containsAll(attributes))
            // Add key.
            keys.add(core);
        else {
            // If not, use Sets.combinations to find all possible combinations of attributes.
            for (int i = 1; i <= case4.size(); i++) {
                for (Set<String> combo : Sets.combinations(case4, i)) {
                    Set<String> stringSet = new HashSet<>();
                    stringSet.addAll(core);
                    stringSet.addAll(combo);
                    if (!checkForMinimal(keys, stringSet)) {
                        continue;
                    }
                    if (computeClosure(fds, stringSet).containsAll(attributes)) {
                        keys.add(stringSet);
                    }
                }
            }
        }


        // TODO 0: End of your code.

        PrintWriter writer = new PrintWriter(new File(outputFile));
        for (Set<String> key : keys)
            writer.println(key.stream().sorted().collect(java.util.stream.Collectors.toList()).
                    toString().replace("[", "").replace("]", ""));
        writer.close();
    }

    private static boolean checkForMinimal(List<Set<String>> keys, Set<String> stringSet) {
        for (Set<String> key : keys) {
            if (stringSet.containsAll(key)) {
                return false;
            }
        }
        return true;
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
