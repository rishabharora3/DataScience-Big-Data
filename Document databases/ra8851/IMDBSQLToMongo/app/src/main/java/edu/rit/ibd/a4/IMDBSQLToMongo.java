package edu.rit.ibd.a4;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.types.Decimal128;

import java.sql.*;
import java.util.Date;

public class IMDBSQLToMongo {

    public static void main(String[] args) throws Exception {
        final String dbURL = args[0];
        final String user = args[1];
        final String pwd = args[2];
        final String mongoDBURL = args[3];
        final String mongoDBName = args[4];

//        final String dbURL = "jdbc:mysql://localhost:3306/imdbNew?useCursorFetch=true";
//        final String user = "root";
//        final String pwd = "password";
//        final String mongoDBURL = "None";
//        final String mongoDBName = "imdb";

        System.out.println(new Date() + " -- Started");

        Connection con = DriverManager.getConnection(dbURL, user, pwd);

        MongoClient client = getClient(mongoDBURL);
        MongoDatabase db = client.getDatabase(mongoDBName);

        // TODO 0: Your code here!

        /*
         *
         * Everything in MongoDB is a document (both data and queries). To create a document, I use primarily two options but there are others
         * 	if you ask the Internet. You can use org.bson.Document as follows:
         *
         * 		Document d = new Document();
         * 		d.append("name_of_the_field", value);
         *
         * 	The type of the field will be the conversion of the Java type of the value.
         *
         * 	Another option is to parse a string representing the document:
         *
         * 		Document d = Document.parse("{ _id:1, name:\"Name\" }");
         *
         * 	It will parse only well-formed documents. Note that the previous approach will use the Java data types as the types of the pieces of
         * 		data to insert in MongoDB. However, the latter approach will not have that info as everything is a string; therefore, be mindful
         * 		of these differences and use the approach it will fit better for you.
         *
         * If you wish to create an embedded document, you can use the following:
         *
         * 		Document outer = new Document();
         * 		Document inner = new Document();
         * 		outer.append("doc", inner);
         *
         * To connect to a MongoDB database server, use the getClient method above. If your server is local, just provide "None" as input.
         *
         * You must extract data from MySQL and load it into MongoDB. Note that, in general, the data in MongoDB is denormalized, which means that it includes
         * 	redundancy. You must think of ways of extracting such redundant data in batches, that is, you should think of a bunch of queries that will retrieve
         * 	the whole database in a format it will be convenient for you to load in MongoDB. Performing many small SQL queries will not work.
         *
         * If you execute a SQL query that retrieves large amounts of data, all data will be retrieved at once and stored in main memory. To avoid such behavior,
         * 	the JDBC URL will have the following parameter: 'useCursorFetch=true' (already added by the grading software). Then, you can control the number of
         * 	tuples that will be retrieved and stored in memory as follows:
         *
         * 		PreparedStatement st = con.prepareStatement("SELECT ...");
         * 		st.setFetchSize(batchSize);
         *
         * where batchSize is the number of rows.
         *
         * Null values in MySQL must be translated as documents without such fields.
         *
         * Once you have composed a specific document with the data retrieved from MySQL, insert the document into the appropriate collection as follows:
         *
         * 		MongoCollection<Document> col = db.getCollection(COLLECTION_NAME);
         *
         * 		...
         *
         * 		Document d = ...
         *
         * 		...
         *
         * 		col.insertOne(d);
         *
         * You should focus first on inserting all the documents you need (movies and people). Once those documents are already present, you should deal with
         * 	the mapping relations. To do so, MongoDB is optimized to make small updates of documents referenced by their keys (different than MySQL). As a
         * 	result, it is a good idea to update one document at a time as follows:
         *
         * 		PreparedStatement st = con.prepareStatement("SELECT ..."); // Select from mapping table.
         * 		st.setFetchSize(batchSize);
         * 		ResultSet rs = st.executeQuery();
         * 		while (rs.next()) {
         * 			col.updateOne(Document.parse("{ _id : "+rs.get(...)+" }"), Document.parse(...));
         * 			...
         *
         * The updateOne method updates one single document based on the filter criterion established in the first document (the _id of the document to fetch
         * 	in this case). The second document provided as input is the update operation to perform. There are several updates operations you can perform (see
         * 	https://docs.mongodb.com/v3.6/reference/operator/update/). If you wish to update arrays, $push and $addToSet are the best options but have slightly
         * 	different semantics. Make sure you read and understand the differences between them.
         *
         * When dealing with arrays, another option instead of updating one by one is gathering all values for a specific document and perform a single update.
         *
         * Note that array fields that are empty are not allowed, so you should not generate them.
         *
         */

        MongoCollection<Document> movies = db.getCollection("Movies");
        MongoCollection<Document> moviesDenorm = db.getCollection("MoviesDenorm");

        addMovieData(con, movies, moviesDenorm);

        MongoCollection<Document> people = db.getCollection("People");
        MongoCollection<Document> peopleDenorm = db.getCollection("PeopleDenorm");

        addPeopleData(con, people, peopleDenorm);

        addMovieGenre(con, movies);

        addData(con, moviesDenorm, peopleDenorm, "actors", "acted", "actor");
        addData(con, moviesDenorm, peopleDenorm, "directors", "directed", "director");
        addData(con, moviesDenorm, peopleDenorm, "producers", "produced", "producer");
        addData(con, moviesDenorm, peopleDenorm, "writers", "written", "writer");
        addData(con, moviesDenorm, peopleDenorm, "", "knownfor", "knownfor");

        // TODO 0: End of your code.

        client.close();
        con.close();
    }

    private static void addData(Connection con, MongoCollection<Document> moviesDenorm, MongoCollection<Document>
            peopleDenorm, String tag1, String tag2, String tableName) throws SQLException {
        PreparedStatement st = con.prepareStatement("SELECT * from " + tableName);
        st.setFetchSize(100);
        ResultSet rs = st.executeQuery();
        while (rs.next()) {
            if (!tag1.isEmpty())
                moviesDenorm.updateOne(Filters.eq("_id", rs.getInt("mid")),
                        new Document().append("$push", new Document(tag1, rs.getInt("pid")))
                );
            if (!tag2.isEmpty())
                peopleDenorm.updateOne(Filters.eq("_id", rs.getInt("pid")),
                        new Document().append("$push", new Document(tag2, rs.getInt("mid")))
                );
        }
        rs.close();
        st.close();
    }


    private static void addMovieGenre(Connection con, MongoCollection<Document> movies) throws SQLException {
        PreparedStatement st = con.prepareStatement("SELECT g.name, mg.mid FROM genre as g JOIN moviegenre as mg ON g.id = mg.gid order by mg.mid");
        st.setFetchSize(100);
        ResultSet rs = st.executeQuery();
        while (rs.next()) {
            movies.updateOne(Document.parse("{_id:" + rs.getInt("mid") + "}"), Document.parse("{$addToSet :{ genres:'" + rs.getString("name") + "'}}"));
        }
        rs.close();
        st.close();
    }

    private static void addPeopleData(Connection con, MongoCollection<Document> people, MongoCollection<Document> peopleDenorm) throws SQLException {
        PreparedStatement st = con.prepareStatement("SELECT * from person");
        st.setFetchSize(100);
        ResultSet rs = st.executeQuery();
        while (rs.next()) {
            Document d = new Document();
            d.append("_id", rs.getInt("id"));
            peopleDenorm.insertOne(d);
            if (rs.getString("name") != null)
                d.append("name", rs.getString("name"));
            if (rs.getString("byear") != null)
                d.append("byear", rs.getInt("byear"));
            if (rs.getString("dyear") != null) {
                d.append("dyear", rs.getInt("dyear"));
            }
            people.insertOne(d);
        }
        rs.close();
        st.close();
    }

    private static void addMovieData(Connection con, MongoCollection<Document> movies, MongoCollection<Document> moviesDenorm) throws SQLException {
        PreparedStatement st = con.prepareStatement("SELECT * from movie");
        st.setFetchSize(100);
        ResultSet rs = st.executeQuery();
        while (rs.next()) {
            Document d = new Document();
            d.append("_id", rs.getInt("id"));
            moviesDenorm.insertOne(d);
            if (rs.getString("otitle") != null)
                d.append("otitle", rs.getString("otitle"));
            if (rs.getString("ptitle") != null)
                d.append("ptitle", rs.getString("ptitle"));
            d.append("adult", rs.getBoolean("adult"));
            if (rs.getString("year") != null) {
                d.append("year", rs.getInt("year"));
            }
            if (rs.getString("runtime") != null) {
                d.append("runtime", rs.getInt("runtime"));
            }
            if (rs.getString("rating") != null) {
                d.append("rating", new Decimal128(rs.getBigDecimal("rating")));
            }
            if (rs.getString("totalvotes") != null) {
                d.append("totalvotes", rs.getInt("totalvotes"));
            }
            movies.insertOne(d);
        }
        rs.close();
        st.close();
    }

    private static MongoClient getClient(String mongoDBURL) {
        MongoClient client;
        if (mongoDBURL.equals("None"))
            client = new MongoClient();
        else
            client = new MongoClient(new MongoClientURI(mongoDBURL));
        return client;
    }

}
