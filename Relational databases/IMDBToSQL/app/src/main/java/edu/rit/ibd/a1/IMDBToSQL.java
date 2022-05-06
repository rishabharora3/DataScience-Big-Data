package edu.rit.ibd.a1;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

/**
 * Author: Rishabh Arora
 */
public class IMDBToSQL {
    private static final String NULL_STR = "\\N";
    private static final int STEP = 1000;
    private static int genreCount = 0;
    private static List<MovieData> movieDataList = new ArrayList<>();
    private static HashMap<String, Integer> genreMap = new HashMap<>();
    private static final HashSet<Integer> movieIdSet = new HashSet<>();

    /**
     * arguments
     * @param args arguments
     * @throws Exception io/sql exceptions
     */
    public static void main(String[] args) throws Exception {
        final String jdbcURL = args[0];
        final String jdbcUser = args[1];
        final String jdbcPwd = args[2];
        final String folderToIMDBGZipFiles = args[3];
        String[] tableNames = new String[]{"Movie", "Genre", "MovieGenre", "Person", "Actor", "Director", "KnownFor", "Producer", "Writer"};
        Connection con = DriverManager.getConnection(jdbcURL, jdbcUser, jdbcPwd);
        con.setAutoCommit(false);
        dropTables(con, tableNames);
        createTables(con, tableNames);
        insertMovieTable(con, folderToIMDBGZipFiles);
        updateRatingInMovieTable(con, folderToIMDBGZipFiles);
        insertGenreTable(con);
        insertMovieGenreTable(con);
        insertPersonTable(con, folderToIMDBGZipFiles);
        insertPersonTypesTables(con, folderToIMDBGZipFiles);
        insertKnownForTable(con, folderToIMDBGZipFiles);
        insertAdditionalDirAndWriter(con, folderToIMDBGZipFiles);
        deleteAdditionalData(con);
        con.close();
    }

    /**
     * dropping tables in the beginning
     * @param con connection variable
     * @param tableNames
     * @throws SQLException
     */
    private static void dropTables(Connection con, String[] tableNames) throws SQLException {
        PreparedStatement st = null;
        try {
            for (String table : tableNames) {
                st = con.prepareStatement("DROP TABLE IF EXISTS " + table);
                st.execute();
                con.commit();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (st != null) {
                st.close();
            }
        }
    }

    /**
     * creating tables all at once in the beginning
     * @param con
     * @param tableNames
     * @throws SQLException
     */
    private static void createTables(Connection con, String[] tableNames) throws SQLException {
        PreparedStatement st = null;
        try {
            String[] sqlQueries = getSqlQueries(tableNames);
            for (String query : sqlQueries) {
                st = con.prepareStatement(query);
                st.execute();
                con.commit();
            }
            for (int i = 4; i < tableNames.length; i++) {
                String sqlCommon = "CREATE TABLE " + tableNames[i] +
                        "(mid INTEGER, " +
                        " pid INTEGER, " +
                        " PRIMARY KEY(mid,pid))";
                st = con.prepareStatement(sqlCommon);
                st.execute();
                con.commit();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (st != null) {
                st.close();
            }
        }
    }

    /**
     * sql queries array
     * @param tableNames
     * @return
     */
    private static String[] getSqlQueries(String[] tableNames) {
        String sqlMovie = "CREATE TABLE " + tableNames[0] +
                "(id INTEGER, " +
                " ptitle VARCHAR(600), " +
                " otitle VARCHAR(600), " +
                " adult BOOLEAN, " +
                " year INTEGER, " +
                " runtime INTEGER, " +
                " rating FLOAT, " +
                " totalvotes INTEGER, " +
                " PRIMARY KEY (id))";

        String sqlGenre = "CREATE TABLE " + tableNames[1] +
                "(id INTEGER, " +
                " name VARCHAR(600), " +
                " PRIMARY KEY (id))";

        String sqlMovieGenre = "CREATE TABLE " + tableNames[2] +
                "(mid INTEGER, " +
                " gid INTEGER, " +
                " PRIMARY KEY(mid,gid))";

        String sqlPerson = "CREATE TABLE " + tableNames[3] +
                "(id INTEGER, " +
                " name VARCHAR(600), " +
                " byear INTEGER, " +
                " dyear INTEGER, " +
                " PRIMARY KEY(id))";
        return new String[]{sqlMovie, sqlGenre, sqlMovieGenre, sqlPerson};
    }

    /**
     * inserting data in movie table
     * @param con connection variable
     * @param folderToIMDBGZipFiles zip folder path
     * @throws SQLException
     */
    private static void insertMovieTable(Connection con, String folderToIMDBGZipFiles) throws SQLException {
        PreparedStatement st = null;
        try (InputStream gzipStreamMovies = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles + "title.basics.tsv.gz"));
             InputStream gzipStreamRatings = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles + "title.ratings.tsv.gz"));
             Scanner scMovies = new Scanner(gzipStreamMovies, "UTF-8");
             Scanner scRatings = new Scanner(gzipStreamRatings, "UTF-8");
        ) {
            int cnt = 0;
            scMovies.nextLine();
            scRatings.nextLine();
            st = con.prepareStatement("INSERT IGNORE INTO Movie(id, ptitle, otitle, adult, year, runtime, rating, totalvotes) VALUES(?,?,?,?,?,?,?,?)");
            while (scMovies.hasNextLine()) {
                // Split the line
                String[] splitLine = scMovies.nextLine().split("\t");
                if ("movie".equalsIgnoreCase(splitLine[1])) {
                    cnt++;
                    int movieId = Integer.parseInt(splitLine[0].replace("tt", ""));
                    st.setInt(1, movieId); //id
                    movieIdSet.add(movieId);
                    if (NULL_STR.equals(splitLine[2]))
                        st.setNull(2, Types.VARCHAR);//ptitle
                    else
                        st.setString(2, splitLine[2]);//ptitle
                    if (NULL_STR.equals(splitLine[3]))
                        st.setNull(3, Types.VARCHAR);//otitle
                    else
                        st.setString(3, splitLine[3]);//otitle
                    if (NULL_STR.equals(splitLine[4]))
                        st.setNull(4, Types.BOOLEAN);//adult
                    else
                        st.setBoolean(4, "1".equalsIgnoreCase(splitLine[4]));//adult
                    if (NULL_STR.equals(splitLine[5]))
                        st.setNull(5, Types.INTEGER);//year
                    else
                        st.setInt(5, Integer.parseInt(splitLine[5]));//year
                    if (NULL_STR.equals(splitLine[7]))
                        st.setNull(6, Types.INTEGER);//runtime
                    else
                        st.setInt(6, Integer.parseInt(splitLine[7]));//runtime
                    st.setNull(7, Types.FLOAT);//rating
                    st.setNull(8, Types.INTEGER);//totalvotes
                    if (!NULL_STR.equals(splitLine[8])) {
                        createGenreList(splitLine[8].split(","));
                        createMovieData(movieId, splitLine[8].split(","));
                    }
                    st.addBatch();
                    if (cnt % STEP == 0) {
                        System.out.println("Movies inserted: " + cnt);
                        st.executeBatch();
                        con.commit();
                    }
                }
            }
            // Leftovers.
            st.executeBatch();
            con.commit();
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        } finally {
            if (st != null) {
                st.close();
            }
        }
    }

    /**
     * updating rating in movie table
     * @param con
     * @param folderToIMDBGZipFiles
     */
    private static void updateRatingInMovieTable(Connection con, String folderToIMDBGZipFiles) {
        try (PreparedStatement st = con.prepareStatement("UPDATE Movie SET rating = ?, totalvotes = ? WHERE id = ?")) {
            int cnt = 0;
            InputStream gzipStreamRating = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles + "title.ratings.tsv.gz"));
            Scanner sc = new Scanner(gzipStreamRating, "UTF-8");
            sc.nextLine();
            while (sc.hasNextLine()) {
                cnt++;
                String[] splitLine = sc.nextLine().split("\t");
                if (NULL_STR.equals(splitLine[1]))
                    st.setNull(1, Types.FLOAT);
                else
                    st.setFloat(1, Float.parseFloat(splitLine[1]));
                if (NULL_STR.equals(splitLine[2]))
                    st.setNull(2, Types.INTEGER);
                else
                    st.setInt(2, Integer.parseInt(splitLine[2]));
                int movieId = Integer.parseInt(splitLine[0].replace("tt", ""));
                st.setInt(3, movieId);
                st.addBatch();
                if (cnt % STEP == 0) {
                    st.executeBatch();
                    con.commit();
                }
            }
            gzipStreamRating.close();
            st.executeBatch();
            con.commit();
            sc.close();
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * creating movie data to add data in Movie Genre Table
     * @param movieId
     * @param genre
     */
    private static void createMovieData(int movieId, String[] genre) {
        MovieData movieData = new MovieData(movieId, genre);
        movieDataList.add(movieData);
    }

    /**
     * creating genre list to insert data in MovieGenre
     * @param genres
     */
    private static void createGenreList(String[] genres) {
        for (String genre : genres) {
            if (genreMap.get(genre) == null)
                genreMap.put(genre, genreCount++);
        }
    }

    /**
     * Inserting move and genres
     * @param con
     */
    private static void insertMovieGenreTable(Connection con) {
        try (PreparedStatement st = con.prepareStatement("INSERT IGNORE INTO MovieGenre(mid,gid) VALUES(?,?)")) {
            for (MovieData movieData : movieDataList) {
                for (String genre : movieData.genreList) {
                    st.setInt(1, movieData.movieId);
                    st.setInt(2, genreMap.get(genre));
                    st.addBatch();
                }
            }
            st.executeBatch();
            con.commit();
            genreMap = null;
            movieDataList = null;
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * inseting genre in table
     * @param con
     */
    private static void insertGenreTable(Connection con) {
        try (PreparedStatement st = con.prepareStatement("INSERT IGNORE INTO Genre(id,name) VALUES(?,?)")) {
            for (Map.Entry<String, Integer> entry : genreMap.entrySet()) {
                st.setInt(1, entry.getValue());
                st.setString(2, entry.getKey());
                st.addBatch();
            }
            st.executeBatch();
            con.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * inerting in person table
     * @param con
     * @param folderToIMDBGZipFiles
     * @throws SQLException
     */
    private static void insertPersonTable(Connection con, String folderToIMDBGZipFiles) throws SQLException {
        int cnt = 0;
        try (PreparedStatement st = con.prepareStatement("INSERT INTO Person(id, name, byear, dyear) VALUES(?,?,?,?)")) {
            InputStream gzipStreamPersons = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles + "name.basics.tsv.gz"));
            Scanner sc = new Scanner(gzipStreamPersons, "UTF-8");
            sc.nextLine();
            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                cnt++;
                String[] splitLine = line.split("\t");
                int personId = Integer.parseInt(splitLine[0].replace("nm", ""));
                st.setInt(1, personId);
                st.setString(2, splitLine[1]);
                if (NULL_STR.equals(splitLine[2]))
                    st.setNull(3, Types.INTEGER);//byear
                else {
                    st.setInt(3, Integer.parseInt(splitLine[2]));//byear
                }
                if (NULL_STR.equals(splitLine[3]))
                    st.setNull(4, Types.INTEGER);//dyear
                else {
                    st.setInt(4, Integer.parseInt(splitLine[3]));//dyear
                }
                st.addBatch();
                if (cnt % STEP == 0) {
                    st.executeBatch();
                    con.commit();
                }
            }
            gzipStreamPersons.close();
            sc.close();
            st.executeBatch();
            con.commit();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * inserting data in different categories of person tables
     * @param con connection variable
     * @param folderToIMDBGZipFiles zip folder path
     * @throws SQLException
     */
    private static void insertPersonTypesTables(Connection con, String folderToIMDBGZipFiles) {
        try (PreparedStatement stActor = con.prepareStatement("INSERT IGNORE INTO Actor(pid, mid) VALUES(?,?)");
             PreparedStatement stDirector = con.prepareStatement("INSERT IGNORE INTO Director(pid, mid) VALUES(?,?)");
             PreparedStatement stProducer = con.prepareStatement("INSERT IGNORE INTO Producer(pid, mid) VALUES(?,?)");
             PreparedStatement stWriter = con.prepareStatement("INSERT IGNORE INTO Writer(pid, mid) VALUES(?,?)")) {
            int cnt = 0;
            InputStream gzipStreamPersonTypes = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles + "title.principals.tsv.gz"));
            Scanner sc = new Scanner(gzipStreamPersonTypes, "UTF-8");
            sc.nextLine();
            while (sc.hasNextLine()) {
                String[] splitLine = sc.nextLine().split("\t");
                int movieId = Integer.parseInt(splitLine[0].replace("tt", ""));
                if (!movieIdSet.contains(movieId))
                    continue;
                int personId = Integer.parseInt(splitLine[2].replace("nm", ""));
                cnt++;
                if ("self".equalsIgnoreCase(splitLine[3]) || splitLine[3].contains("act")) {
                    stActor.setInt(1, personId);
                    stActor.setInt(2, movieId);
                    stActor.addBatch();
                } else if ("director".equalsIgnoreCase(splitLine[3])) {
                    stDirector.setInt(1, personId);
                    stDirector.setInt(2, movieId);
                    stDirector.addBatch();
                } else if ("producer".equalsIgnoreCase(splitLine[3])) {
                    stProducer.setInt(1, personId);
                    stProducer.setInt(2, movieId);
                    stProducer.addBatch();
                } else if ("writer".equalsIgnoreCase(splitLine[3])) {
                    stWriter.setInt(1, personId);
                    stWriter.setInt(2, movieId);
                    stWriter.addBatch();
                }
                if (cnt % STEP == 0) {
                    System.out.println("Person Types inserted: " + cnt);
                    stActor.executeBatch();
                    stDirector.executeBatch();
                    stProducer.executeBatch();
                    stWriter.executeBatch();
                    con.commit();
                }
            }
            gzipStreamPersonTypes.close();
            sc.close();
            stActor.executeBatch();
            stDirector.executeBatch();
            stProducer.executeBatch();
            stWriter.executeBatch();
            con.commit();
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * insert in known for table
     * @param con connection variable
     * @param folderToIMDBGZipFiles zip folder path
     */
    private static void insertKnownForTable(Connection con, String folderToIMDBGZipFiles) {
        int cnt = 0;
        try (PreparedStatement st = con.prepareStatement("INSERT IGNORE INTO KnownFor(pid, mid) VALUES(?,?)")) {
            InputStream gzipStreamPersons = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles + "name.basics.tsv.gz"));
            Scanner sc = new Scanner(gzipStreamPersons, "UTF-8");
            sc.nextLine();
            while (sc.hasNextLine()) {
                String[] splitLine = sc.nextLine().split("\t");
                if (!NULL_STR.equals(splitLine[5])) {
                    String[] movieIdArray = splitLine[5].split(",");
                    for (String movieId : movieIdArray) {
                        int movieId1 = Integer.parseInt(movieId.replace("tt", ""));
                        if (!movieIdSet.contains(movieId1))
                            continue;
                        cnt++;
                        int personId = Integer.parseInt(splitLine[0].replace("nm", ""));
                        st.setInt(1, personId);
                        st.setInt(2, Integer.parseInt(movieId.replace("tt", "")));
                        st.addBatch();
                        if (cnt % STEP == 0) {
                            System.out.println("Known For inserted: " + cnt);
                            st.executeBatch();
                            con.commit();
                        }
                    }
                }
            }
            gzipStreamPersons.close();
            sc.close();
            st.executeBatch();
            con.commit();
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * inserting additional directors and writers
     * @param con connection variable
     * @param folderToIMDBGZipFiles
     */
    private static void insertAdditionalDirAndWriter(Connection con, String folderToIMDBGZipFiles) {
        try (PreparedStatement stDirector = con.prepareStatement("INSERT IGNORE INTO Director(pid, mid) VALUES(?,?) ");
             PreparedStatement stWriter = con.prepareStatement("INSERT IGNORE INTO Writer(pid, mid) VALUES(?,?)")) {
            InputStream gzipStreamAdditionalData = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles + "title.crew.tsv.gz"));
            Scanner sc = new Scanner(gzipStreamAdditionalData, "UTF-8");
            sc.nextLine();
            int cnt = 0;
            while (sc.hasNextLine()) {
                cnt++;
                String[] splitLine = sc.nextLine().split("\t");
                int movieId = Integer.parseInt(splitLine[0].replace("tt", ""));
                if (!movieIdSet.contains(movieId))
                    continue;
                if (!NULL_STR.equals(splitLine[1])) {
                    String[] directorList = splitLine[1].split(",");
                    for (String director : directorList) {
                        stDirector.setInt(1, Integer.parseInt(director.replace("nm", "")));
                        stDirector.setInt(2, movieId);
                        stDirector.addBatch();
                    }
                }
                if (!NULL_STR.equals(splitLine[2])) {
                    String[] writerList = splitLine[2].split(",");
                    for (String writer : writerList) {
                        stWriter.setInt(1, Integer.parseInt(writer.replace("nm", "")));
                        stWriter.setInt(2, movieId);
                        stWriter.addBatch();
                    }
                }
                if (cnt % STEP == 0) {
                    stDirector.executeBatch();
                    stWriter.executeBatch();
                    con.commit();
                }
            }
            gzipStreamAdditionalData.close();
            sc.close();
            stDirector.executeBatch();
            stWriter.executeBatch();
            con.commit();
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * deleting additional data
     * @param con
     */
    private static void deleteAdditionalData(Connection con) {
        try {
            deleteData(con, "DELETE from Actor where pid not in (SELECT id from Person)");
            deleteData(con, "DELETE from Director where pid not in (SELECT id from Person)");
            deleteData(con, "DELETE FROM KnownFor where pid not in (SELECT id from Person)");
            deleteData(con, "DELETE from Writer where pid not in (SELECT id from Person)");
            deleteData(con, "DELETE from Producer where pid not in (SELECT id from Person)");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * deleting data common method
     * @param con
     * @param s
     * @throws SQLException
     */
    private static void deleteData(Connection con, String s) throws SQLException {
        PreparedStatement st = con.prepareStatement(s);
        st.executeUpdate();
        st.close();
        con.commit();
    }

    /**
     * helper class for creating a list
     */
    static class MovieData {
        int movieId;
        String[] genreList;

        public MovieData(int movieId, String[] genreList) {
            this.movieId = movieId;
            this.genreList = genreList;
        }
    }

}
