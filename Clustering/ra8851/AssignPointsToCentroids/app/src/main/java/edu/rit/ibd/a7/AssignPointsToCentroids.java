package edu.rit.ibd.a7;

import ch.obermuhlner.math.big.BigDecimalMath;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.types.Decimal128;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Arrays;
import java.util.HashMap;

public class AssignPointsToCentroids {
    public enum Distance {Manhattan, Euclidean}

    public enum Mean {Arithmetic, Geometric}

    public static void main(String[] args) throws Exception {
		final String mongoDBURL = args[0];
		final String mongoDBName = args[1];
		final String mongoCol = args[2];
		final Distance distance = Distance.valueOf(args[3]);
		final Mean mean = Mean.valueOf(args[4]);

//        final String mongoDBURL = "None";
//        final String mongoDBName = "Kmeans";
//        final String mongoCol = "Points_0";
//        final Distance distance = Distance.valueOf("Manhattan");
//        final Mean mean = Mean.valueOf("Arithmetic");

        MongoClient client = getClient(mongoDBURL);
        MongoDatabase db = client.getDatabase(mongoDBName);

        MongoCollection<Document> collection = db.getCollection(mongoCol);

        // TODO Your code here!

        /*
         *
         * Points have _id=p_XYZ and centroids have _id=c_ABC. A new centroid derived from an existing centroid c_i must have _id=new_c_i.
         *
         * To perform one epoch, each point should be assigned to the closest centroid using the input distance (Manhattan or Euclidean).
         * 	That is, if point p_i has the minimum distance to centroid c_j, p_i.label = c_j.
         *
         * Once the point assignment has been made, SSE is the sum of the square distance between point and centroid. Each centroid c_i
         * 	will contain a field sse that will store the SSE of all the points assigned to it. Furthermore, you need to include a field
         * 	with the total count of points assigned to the centroid (total_points), and whether must be reinitialized (reinitialize).
         *
         * A centroid must be reinitialized at the end of the epoch if it has no points assigned to it.
         *
         * Each new centroid is derived from a centroid that must not be reinitialized. A centroid that must be reinitialized has no
         * 	points assigned to it, so it is not possible to compute a new centroid. Assuming c_i is a centroid with points assigned, the
         * 	new centroid new_c_i is the (arithmetic or geometric) mean of all the points assigned to it. That is, for each dimension k,
         * 	the dimension k of new_c_i is the mean of all the dimensions k of the points assigned to c_i.
         *
         */
        collection.createIndex(new Document("_id", 1));
        HashMap<String, Centroid> centroidHashMap = getCentroids(collection);
        findClosestCentroid(collection, centroidHashMap, distance);
        pushDataToCentroids(collection, centroidHashMap);
        findNewCentroids(collection, mean);


        // TODO End of your code!

        client.close();
    }

    private static void findNewCentroids(MongoCollection<Document> collection, Mean mean) {
        for (Document document : collection.find(Document.parse("{_id: /^c_/}")).batchSize(100)) {
            if (!document.getBoolean("reinitialize")) {
                String centroidId = document.getString("_id");
                int columnCount = document.get("centroid", Document.class).size();
                BigDecimal[] sumArray = new BigDecimal[columnCount];
                Arrays.fill(sumArray, BigDecimal.ZERO);
                BigDecimal total = BigDecimal.ZERO;
                Document newCentroid = new Document();
                newCentroid.append("_id", "new_" + centroidId);
                for (Document pointDocument : collection.find(new Document("label", centroidId)).batchSize(100)) {
                    Document dim = pointDocument.get("point", Document.class);
                    total = total.add(BigDecimal.ONE, MathContext.DECIMAL128);
                    switch (mean) {
                        case Arithmetic:
                            for (int i = 0; i < dim.size(); i++) {
                                sumArray[i] = sumArray[i].add(dim.get("dim_" + i, Decimal128.class).bigDecimalValue(), MathContext.DECIMAL128);
                            }
                            break;
                        case Geometric:
                            for (int i = 0; i < dim.size(); i++) {
                                sumArray[i] = sumArray[i].add(BigDecimalMath.log(dim.get("dim_" + i, Decimal128.class).bigDecimalValue(), MathContext.DECIMAL128),
                                        MathContext.DECIMAL128);
                            }
                            break;
                    }
                }
                if (total.compareTo(BigDecimal.ZERO) == 0) {
                    continue;
                }
                Document newDim = new Document();
                switch (mean) {
                    case Arithmetic:
                        for (int i = 0; i < sumArray.length; i++) {
                            sumArray[i] = sumArray[i].multiply(total.pow(-1, MathContext.DECIMAL128), MathContext.DECIMAL128);
                            newDim.append("dim_" + i, sumArray[i]);
                        }
                        break;
                    case Geometric:
                        for (int i = 0; i < sumArray.length; i++) {
                            sumArray[i] = BigDecimalMath.exp(sumArray[i].multiply(total.pow(-1, MathContext.DECIMAL128), MathContext.DECIMAL128), MathContext.DECIMAL128);
                            newDim.append("dim_" + i, sumArray[i]);
                        }
                        break;
                }
                newCentroid.append("centroid", newDim);
                collection.insertOne(newCentroid);
            }
        }
    }

    private static void pushDataToCentroids(MongoCollection<Document> collection, HashMap<String, Centroid> centroidHashMap) {
        for (Document document : collection.find(Document.parse("{_id: /^c_/}")).batchSize(100)) {
            String centroidId = document.getString("_id");
            Centroid centroid = centroidHashMap.get(centroidId);
            collection.updateOne(new Document("_id", centroidId), new Document("$set", document
                    .append("total_points", centroid.totalPoints)
                    .append("reinitialize", centroid.reinitialize)
                    .append("sse", centroid.sse)));
        }
    }

    private static void findClosestCentroid(MongoCollection<Document> collection, HashMap<String, Centroid> centroidHashMap, Distance distance) {
        for (Document document : collection.find(Document.parse("{_id: /^p_/}")).batchSize(100)) {
            String pointId = document.getString("_id");
            String centroidId = getClosestCentroid(collection, document, centroidHashMap, distance);
            collection.updateOne(new Document("_id", pointId), new Document("$set", document.append("label", centroidId)));
        }
    }

    private static String getClosestCentroid(MongoCollection<Document> collection, Document document, HashMap<String, Centroid> centroidHashMap, Distance distance) {
        String closestCentroidId = null;
        BigDecimal minDistance = BigDecimal.valueOf(Double.MAX_VALUE);
        for (Document centroid : collection.find(Document.parse("{_id: /^c_/}")).batchSize(100)) {
            BigDecimal distanceToCentroid = getDistance(centroid, document, distance);
            if (distanceToCentroid.compareTo(minDistance) < 0) {
                minDistance = distanceToCentroid;
                closestCentroidId = centroid.getString("_id");
            }
        }
        updateCentroid(centroidHashMap, closestCentroidId, minDistance);
        return closestCentroidId;
    }

    private static void updateCentroid(HashMap<String, Centroid> centroidHashMap, String closestCentroidId, BigDecimal minDistance) {
        Centroid centroid = centroidHashMap.get(closestCentroidId);
        centroid.reinitialize = false;
        centroid.sse = centroid.sse.add(minDistance.pow(2, MathContext.DECIMAL128), MathContext.DECIMAL128);
        centroid.totalPoints++;
        centroidHashMap.put(closestCentroidId, centroid);
    }

    private static BigDecimal getDistance(Document centroid, Document document, Distance distance) {
        return switch (distance) {
            case Euclidean -> getEuclideanDistance(centroid, document);
            case Manhattan -> getManhattanDistance(centroid, document);
        };
    }

    private static BigDecimal getManhattanDistance(Document centroid, Document document) {
        BigDecimal distance = BigDecimal.ZERO;
        Document centroidPoints = centroid.get("centroid", Document.class);
        Document points = document.get("point", Document.class);
        for (int i = 0; i < centroidPoints.size(); i++) {
            BigDecimal dimCentroid = centroidPoints.get("dim_" + i, Decimal128.class).bigDecimalValue();
            BigDecimal dim = points.get("dim_" + i, Decimal128.class).bigDecimalValue();
            distance = distance.add(dimCentroid.subtract(dim, MathContext.DECIMAL128).abs(), MathContext.DECIMAL128);
        }
        return distance;
    }

    private static BigDecimal getEuclideanDistance(Document centroid, Document document) {
        BigDecimal distance = BigDecimal.ZERO;
        Document centroidPoints = centroid.get("centroid", Document.class);
        Document points = document.get("point", Document.class);
        for (int i = 0; i < centroidPoints.size(); i++) {
            BigDecimal dimCentroid = centroidPoints.get("dim_" + i, Decimal128.class).bigDecimalValue();
            BigDecimal dim = points.get("dim_" + i, Decimal128.class).bigDecimalValue();
            distance = distance.add(dimCentroid.subtract(dim, MathContext.DECIMAL128).pow(2, MathContext.DECIMAL128), MathContext.DECIMAL128);
        }
        return distance.sqrt(MathContext.DECIMAL128);
    }

    private static HashMap<String, Centroid> getCentroids(MongoCollection<Document> collection) {
        HashMap<String, Centroid> centroidHashMap = new HashMap<>();
        for (Document document : collection.find(Document.parse("{_id: /^c_/}")).batchSize(100)) {
            String id = document.getString("_id");
            centroidHashMap.put(id, new Centroid());
        }
        return centroidHashMap;
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
