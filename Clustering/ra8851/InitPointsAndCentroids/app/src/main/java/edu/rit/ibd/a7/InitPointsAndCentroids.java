package edu.rit.ibd.a7;

import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.*;
import java.util.Arrays;

import org.bson.Document;
import org.bson.types.Decimal128;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class InitPointsAndCentroids {
    public enum Scaling {None, MinMax, Mean, ZScore}

    public static void main(String[] args) throws Exception {
        final String jdbcURL = args[0];
        final String jdbcUser = args[1];
        final String jdbcPwd = args[2];
        final String sqlQuery = args[3];
        final String mongoDBURL = args[4];
        final String mongoDBName = args[5];
        final String mongoCol = args[6];
        final Scaling scaling = Scaling.valueOf(args[7]);
        final int k = Integer.valueOf(args[8]);


//        final String jdbcURL = "jdbc:mysql://localhost:3306/imdbNew?useCursorFetch=true";
//        final String jdbcUser = "root";
//        final String jdbcPwd = "password";
//        final String sqlQuery = "SELECT CAST(CONCAT(mid, pid) AS UNSIGNED) AS id, year AS dim_0, rating AS dim_1, byear AS dim_2, runtime AS dim_4, totalvotes AS dim_3 FROM Actor JOIN Movie AS m ON m.id=mid JOIN Person AS p ON pid=p.id WHERE \tbyear IS NOT NULL AND rating IS NOT NULL AND runtime IS NOT NULL AND year BETWEEN 1990 AND 2010 AND totalvotes > 1000 AND mid IN (SELECT mid FROM MovieGenre JOIN Genre ON id=gid WHERE name LIKE '%c%') UNION SELECT CAST(CONCAT(mid, pid) AS UNSIGNED) AS id, year AS dim_0, rating AS dim_1, byear AS dim_2, runtime AS dim_4, totalvotes AS dim_3 FROM Director JOIN Movie AS m ON m.id=mid JOIN Person AS p ON pid=p.id WHERE \tbyear IS NOT NULL AND rating IS NOT NULL AND runtime IS NOT NULL AND year BETWEEN 1990 AND 2010 AND totalvotes > 1000 AND mid IN (SELECT mid FROM MovieGenre JOIN Genre ON id=gid WHERE name LIKE '%c%') UNION SELECT CAST(CONCAT(mid, pid) AS UNSIGNED) AS id, year AS dim_0, rating AS dim_1, byear AS dim_2, runtime AS dim_4, totalvotes AS dim_3 FROM Producer JOIN Movie AS m ON m.id=mid JOIN Person AS p ON pid=p.id WHERE \tbyear IS NOT NULL AND rating IS NOT NULL AND runtime IS NOT NULL AND year BETWEEN 1990 AND 2010 AND totalvotes > 1000 AND mid IN (SELECT mid FROM MovieGenre JOIN Genre ON id=gid WHERE name LIKE '%c%') UNION SELECT CAST(CONCAT(mid, pid) AS UNSIGNED) AS id, year AS dim_0, rating AS dim_1, byear AS dim_2, runtime AS dim_4, totalvotes AS dim_3 FROM Writer JOIN Movie AS m ON m.id=mid JOIN Person AS p ON pid=p.id WHERE \tbyear IS NOT NULL AND rating IS NOT NULL AND runtime IS NOT NULL AND year BETWEEN 1990 AND 2010 AND totalvotes > 1000 AND mid IN (SELECT mid FROM MovieGenre JOIN Genre ON id=gid WHERE name LIKE '%c%')";
//        final String mongoDBURL = "None";
//        final String mongoDBName = "Kmeans";
//        final String mongoCol = "Points_0";
//        final Scaling scaling = Scaling.valueOf("MinMax");
//        final int k = Integer.valueOf("75");

        Connection con = DriverManager.getConnection(jdbcURL, jdbcUser, jdbcPwd);

        MongoClient client = getClient(mongoDBURL);
        MongoDatabase db = client.getDatabase(mongoDBName);

        MongoCollection<Document> collection = db.getCollection(mongoCol);

        // TODO Your code here!

        /*
         *
         * The SQL query has a column named id with the id of each point (always long), and a number of columns dim_i that form the point with n dimensions.
         * 	You should store the value of each dimension as a Decimal128. In order to do so, use the readAttribute method provided.
         *
         * All your computations must use BigDecimal/Decimal128. Note that x.add(y), where both x and y are BigDecimal, will not update x, so you need to
         * 	assign it to a BigDecimal, i.e., z = x.add(y). When dividing, use MathContext.DECIMAL128 to keep the desired precision. If you implement your
         * 	calculations using MongoDB, do not use {$divide : [x, y]}; instead, you must do: {$multiply : [x, {$pow: [y, -1]}]}.
         *
         * Each point must be of the form: {_id: p_123, point: {dim_0:_, dim_1:_, ...}}; each centroid: {_id: c_7, centroid: {}}.
         *
         * Compute stat values per dimension and store them in a document whose id is 'limits'. For each dimension i, dim_i:{min:_, max:_, mean:_, std:_}.
         * 	Note that you can use Java or MongoDB to compute these. There is a stdDevPop in MongoDB to compute standard deviation; unfortunately, it does
         * 	not return Decimal128, so you need to find an alternate way.
         *
         * Using the limits, you must scale the value using MinMax, Mean or ZScore according to the input. This only applies to the points.
         *
         */
        Document limits = createLimits(con, sqlQuery, collection);
        scaleUpPoints(con, sqlQuery, collection, scaling, limits);
        addCentroids(collection, k);

        // TODO End of your code!

        client.close();
        con.close();
    }

    private static void addCentroids(MongoCollection<Document> collection, int k) {
        for (int i = 0; i < k; i++) {
            Document centroid = new Document();
            centroid.append("_id", "c_" + i);
            centroid.append("centroid", new Document());
            collection.insertOne(centroid);
        }
    }

    private static void scaleUpPoints(Connection con, String sqlQuery, MongoCollection<Document> collection, Scaling scaling, Document limits) {
        try {
            PreparedStatement st = con.prepareStatement(sqlQuery);
            st.setFetchSize(100);
            ResultSet rs = st.executeQuery();
            ResultSetMetaData resultSetMetaData = rs.getMetaData();
            int columnCount = resultSetMetaData.getColumnCount();
            while (rs.next()) {
                switch (scaling) {
                    case MinMax -> scaleUpMinMax(rs, collection, limits, columnCount);
                    case Mean -> scaleUpMean(rs, collection, limits, columnCount);
                    case ZScore -> scaleUpZScore(rs, collection, limits, columnCount);
                    default -> addPointToCollectionWithoutScale(rs, collection, columnCount);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void addPointToCollectionWithoutScale(ResultSet rs, MongoCollection<Document> collection, int columnCount) throws SQLException {
        BigDecimal[] unscaled = new BigDecimal[columnCount - 1];
        for (int i = 0; i < columnCount - 1; i++) {
            Decimal128 dimen = readAttribute(rs, "dim_" + i);
            unscaled[i] = dimen.bigDecimalValue();
        }
        addPointToCollection(rs, collection, columnCount, unscaled);
    }

    private static void scaleUpZScore(ResultSet rs, MongoCollection<Document> collection, Document limits, int columnCount) throws SQLException {
        BigDecimal[] scaled = new BigDecimal[columnCount - 1];
        for (int i = 0; i < columnCount - 1; i++) {
            Decimal128 dimen = readAttribute(rs, "dim_" + i);
            BigDecimal std = limits.get("dim_" + i, Document.class).get("std",  BigDecimal.class);
            BigDecimal mean = limits.get("dim_" + i, Document.class).get("mean",  BigDecimal.class);
            BigDecimal numerator = dimen.bigDecimalValue().subtract(mean);
            scaled[i] = numerator.multiply(std.pow(-1, MathContext.DECIMAL128), MathContext.DECIMAL128);
        }
        addPointToCollection(rs, collection, columnCount, scaled);
    }

    private static void scaleUpMean(ResultSet rs, MongoCollection<Document> collection, Document limits, int columnCount) throws SQLException {
        BigDecimal[] scaled = new BigDecimal[columnCount - 1];
        for (int i = 0; i < columnCount - 1; i++) {
            Decimal128 dimen = readAttribute(rs, "dim_" + i);
            BigDecimal min = limits.get("dim_" + i, Document.class).get("min", Decimal128.class).bigDecimalValue();
            BigDecimal max = limits.get("dim_" + i, Document.class).get("max", Decimal128.class).bigDecimalValue();
            BigDecimal mean = limits.get("dim_" + i, Document.class).get("mean", BigDecimal.class);
            BigDecimal numerator = dimen.bigDecimalValue().subtract(mean);
            BigDecimal denominator = max.subtract(min);
            scaled[i] = numerator.multiply(denominator.pow(-1, MathContext.DECIMAL128), MathContext.DECIMAL128);
        }
        addPointToCollection(rs, collection, columnCount, scaled);
    }

    private static void scaleUpMinMax(ResultSet rs, MongoCollection<Document> collection, Document limits, int columnCount) throws SQLException {
        BigDecimal[] scaled = new BigDecimal[columnCount - 1];
        for (int i = 0; i < columnCount - 1; i++) {
            Decimal128 dimen = readAttribute(rs, "dim_" + i);
            BigDecimal min = limits.get("dim_" + i, Document.class).get("min", Decimal128.class).bigDecimalValue();
            BigDecimal max = limits.get("dim_" + i, Document.class).get("max", Decimal128.class).bigDecimalValue();
            BigDecimal numerator = dimen.bigDecimalValue().subtract(min);
            BigDecimal denominator = max.subtract(min);
            scaled[i] = numerator.multiply(denominator.pow(-1, MathContext.DECIMAL128), MathContext.DECIMAL128);
        }
        addPointToCollection(rs, collection, columnCount, scaled);
    }

    private static void addPointToCollection(ResultSet rs, MongoCollection<Document> collection, int columnCount, BigDecimal[] points) throws SQLException {
        Document point = new Document();
        for (int i = 0; i < columnCount - 1; i++) {
            point.append("dim_" + i, points[i]);
        }
        Document pointWithId = new Document();
        pointWithId.append("_id", "p_" + rs.getLong("id"));
        pointWithId.append("point", point);
        collection.insertOne(pointWithId);
    }

    private static Document createLimits(Connection con, String sqlQuery, MongoCollection<Document> collection) throws SQLException {
        PreparedStatement st = con.prepareStatement(sqlQuery);
        st.setFetchSize(100);
        ResultSet rs = st.executeQuery();
        ResultSetMetaData resultSetMetaData = rs.getMetaData();
        int columnCount = resultSetMetaData.getColumnCount();
        int rowCount = 0;
        Decimal128[] maxArr = new Decimal128[columnCount - 1];
        Arrays.fill(maxArr, Decimal128.NEGATIVE_INFINITY);
        Decimal128[] minArr = new Decimal128[columnCount - 1];
        Arrays.fill(minArr, Decimal128.POSITIVE_INFINITY);
        BigDecimal[] sumArray = new BigDecimal[columnCount - 1];
        Arrays.fill(sumArray, BigDecimal.ZERO);
        BigDecimal[] stdArray = new BigDecimal[columnCount - 1];
        Arrays.fill(stdArray, BigDecimal.ZERO);
        while (rs.next()) {
            rowCount++;
            for (int i = 0; i < columnCount - 1; i++) {
                Decimal128 dimen = readAttribute(rs, "dim_" + i);
                if (dimen.compareTo(minArr[i]) < 0) {
                    minArr[i] = dimen;
                }
                if (dimen.compareTo(maxArr[i]) > 0) {
                    maxArr[i] = dimen;
                }
                sumArray[i] = sumArray[i].add(dimen.bigDecimalValue());
            }
        }
        for (int i = 0; i < columnCount - 1; i++) {
            sumArray[i] = sumArray[i].divide(new BigDecimal(rowCount), MathContext.DECIMAL128);
        }
        rs = st.executeQuery();
        while (rs.next()) {
            for (int i = 0; i < columnCount - 1; i++) {
                Decimal128 dimen = readAttribute(rs, "dim_" + i);
                stdArray[i] = stdArray[i].add(dimen.bigDecimalValue().subtract(sumArray[i]).pow(2, MathContext.DECIMAL128));
            }
        }
        for (int i = 0; i < columnCount - 1; i++) {
            stdArray[i] = stdArray[i].divide(new BigDecimal(rowCount), MathContext.DECIMAL128);
            stdArray[i] = stdArray[i].sqrt(MathContext.DECIMAL128);
        }
        Document limits = new Document();
        limits.append("_id", "limits");
        for (int i = 0; i < columnCount - 1; i++) {
            limits.append("dim_" + i, new Document("min", minArr[i])
                    .append("max", maxArr[i])
                    .append("mean", sumArray[i])
                    .append("std", stdArray[i]));
        }
        System.out.println(limits);
        collection.insertOne(limits);
        return limits;
    }

    private static Decimal128 readAttribute(ResultSet rs, String label) throws SQLException {
        // From: https://stackoverflow.com/questions/9482889/set-specific-precision-of-a-bigdecimal
        BigDecimal x = rs.getBigDecimal(label);
        x = x.setScale(x.scale() + MathContext.DECIMAL128.getPrecision() - x.precision(), MathContext.DECIMAL128.getRoundingMode());
        return new Decimal128(x);
    }

    private static MongoClient getClient(String mongoDBURL) {
        MongoClient client = null;
        if (mongoDBURL.equals("None"))
            client = new MongoClient();
        else
            client = new MongoClient(new MongoClientURI(mongoDBURL));
        return client;
    }

}
