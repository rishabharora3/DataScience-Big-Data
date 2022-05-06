package edu.rit.ibd.a3;

import java.io.File;
import java.io.PrintWriter;
import java.sql.*;
import java.util.*;

import com.google.common.collect.Sets;

public class FDDiscovery {

    public static void main(String[] args) throws Exception {
        final String url = args[0];
        final String user = args[1];
        final String pwd = args[2];
        final String relationName = args[3];
        final String outputFile = args[4];

        Connection con = DriverManager.getConnection(url, user, pwd);

        // These are the attributes of the input relation.
        TreeSet<String> attributes = new TreeSet<>();
        // These are the functional dependencies discovered.
        Set<FunctionalDependency> fds = new HashSet<>();

        // TODO 0: Your code here!

        // Your program must be generic and work for any relation provided as input. You must read the names of the attributes from the input relation and store
        //	them in the attributes set.
        //
        // You must traverse the lattice of attributes starting from combinations of size 1. Remember that all the functional dependencies we will discover are
        //	of the form a1, ..., ak -> aj; therefore, there is a single attribute on the right-hand side, and one or more attributes on the left-hand side. Re-
        //	member also that we are not interested in trivial functional dependencies (right-hand side is included in left-hand side) or non-minimal (there exi-
        //	sts another functional dependency that is contained in the current one).
        //
        // To traverse the lattice, we start from single combinations of attributes in the left-hand side, then combinations of two attributes, then combinatio-
        //	ns of three attributes, etc. We stop when we have tested all possible combinations. Use Sets.combinations to generate these combinations.
        //
        // a1, a2 -> a3 is a functional dependency for relation x if the following SQL query outputs no result:
        //	SELECT * FROM x AS t1 JOIN x AS t2 ON t1.a1 = t2.a1 AND t1.a2 = t2.a2 WHERE t1.a3 <> t2.a3
        //
        //	You must compose this type of SQL for the different combinations of attributes to find functional dependencies.


        // Read attributes. Use the metadata to get the column info.
        PreparedStatement st = con.prepareStatement("SELECT * FROM " + relationName + " LIMIT 1");
        ResultSet rs = st.executeQuery();
        for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++)
            attributes.add(rs.getMetaData().getColumnName(i));
        rs.close();
        st.close();

        // Each FD has a left-hand side and a right-hand side. For LHS, start from size one and keep increasing.
        for (int size = 1; size < attributes.size(); size++) {
            // Get each combination of attributes in the left-hand side of the appropriate size.
            for (Set<String> leftHandSide : Sets.combinations(attributes, size)) {
                // Get the attributes in the right-hand side.
                for (String rightHandSide : attributes) {
                    // Make sure that the candidate FD is not trivial and minimal.
                    if (checkForTrivial(leftHandSide, rightHandSide)) {
                        System.out.println("trivial: " + leftHandSide + " " + rightHandSide);
                        continue;
                    }
                    if (!checkForMinimal(fds, leftHandSide, rightHandSide)) {
                        System.out.println("minimal: " + leftHandSide + " " + rightHandSide);
                        continue;
                    }
                    // Make sure that the candidate FD is a FD. Build a SQL query to check it.
                    String query = createQuery(leftHandSide, relationName, rightHandSide);
                    if (checkIfFD(con, query)) {
                        // Form the candidate FD using both left-hand and right-hand sides.
                        FunctionalDependency dependency = new FunctionalDependency(new TreeSet<>(leftHandSide), rightHandSide);
                        // If it is a FD, add it to the set of discovered FDs.
                        System.out.println("dependency added: " + leftHandSide + " " + rightHandSide);
                        fds.add(dependency);
                    }
                }
            }
        }

        // TODO 0: End of your code.
        // Write to file!
        PrintWriter writer = new PrintWriter(new File(outputFile));
        for (FunctionalDependency fd : fds) {
            String[] leftHandSide = String.join(",", fd.getLeftAtr()).split(",");
            String output = String.join(", ", leftHandSide) + " -> " + fd.getRightAtr();
            System.out.println("output: " + output);
            writer.println(output);
        }
        writer.close();
        con.close();
    }

    private static boolean checkIfFD(Connection con, String query) throws SQLException {
        PreparedStatement preparedStatement = con.prepareStatement(query);
        ResultSet rs2 = preparedStatement.executeQuery();
        if (!rs2.next()) {
            return true;
        }
        rs2.close();
        return false;
    }

    private static String createQuery(Set<String> leftHandSide, String relationName, String rightHandSide) {
        StringBuilder query = new StringBuilder("SELECT * FROM " + relationName + " AS t1 JOIN " + relationName + " AS t2 ON ");
        int count = 0;
        for (String attr : leftHandSide) {
            if (count == leftHandSide.size() - 1) {
                query.append("t1.").append(attr).append(" = t2.").append(attr);
            } else {
                query.append("t1.").append(attr).append(" = t2.").append(attr).append(" AND ");
            }
            count++;
        }
        query.append(" WHERE t1.").append(rightHandSide).append(" <> t2.").append(rightHandSide);
        query.append(" LIMIT 1");
        System.out.println(query);
        return query.toString();
    }

    private static boolean checkForMinimal(Set<FunctionalDependency> fds, Set<String> leftHandSide, String rightHandSide) {
        for (FunctionalDependency dependency : fds) {
            if (dependency.getRightAtr().equals(rightHandSide) && leftHandSide.containsAll(dependency.getLeftAtr())) {
                return false;
            }
        }
        return true;
    }

    private static boolean checkForTrivial(Set<String> leftHandSide, String rightHandSide) {
        return leftHandSide.contains(rightHandSide);
    }

}
