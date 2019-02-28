import com.couchbase.client.java.analytics.AnalyticsQuery;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.spark.connection.CouchbaseAnalyticsRow;
import com.couchbase.spark.japi.CouchbaseSparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;

import java.util.Arrays;

import static com.couchbase.spark.japi.CouchbaseSparkContext.couchbaseContext;

public class App {

  public static void main(String... args) {
    // Setup
    SparkConf conf = new SparkConf()
      .setAppName("javaTest")
      .setMaster("local[*]")
      .set("com.couchbase.username", "Administrator")
      .set("com.couchbase.password", "password")
      .set("com.couchbase.bucket.cars", "")
      .set("com.couchbase.bucket.locations", "");


    // Create the contexts we need
    JavaSparkContext jsc = new JavaSparkContext(conf);
    CouchbaseSparkContext csc = couchbaseContext(jsc);
    SQLContext sql = new SQLContext(jsc);

    try {
      run(sql, csc);
    } finally {
      // Shutdown
      jsc.stop();
    }
  }

  private static void run(final SQLContext sql, final CouchbaseSparkContext csc) {

    // This query loads some car data and joins it with the locations on latitude and longitude
    // note how the geo points are truncated at the 3rd decimal since that is significant enough
    // and otherwise we didn't find matches - too little data and too close
    AnalyticsQuery query = AnalyticsQuery.simple(
      "select c.EngineTemp, c.OutsideTemp, c.Speed " +
      "from cars c " +
      "join locations l on " +
      "trunc(double(l.LON), 3) = trunc(c.Location.long, 3) " +
      "and trunc(double(l.LAT), 3) = trunc(c.Location.lat, 3)"
    );

    // run the query and cache the results in memory, otherwise it would query over and over
    // again potentially.
    JavaRDD<JsonObject> carsWithLocation = csc
      .couchbaseAnalytics(query, "cars")
      .map(CouchbaseAnalyticsRow::value)
      .cache();

    // map the json data from the query to a dense vector of doubles that the clustering
    // algo needs to be trained.
    JavaRDD<Vector> denseVector = carsWithLocation.map(r -> Vectors.dense(new double[]{
      r.getDouble("EngineTemp"),
      r.getDouble("OutsideTemp"),
      r.getDouble("Speed")}))
      .cache();

    // Train the model with our vector RDD
    KMeansModel model = KMeans.train(denseVector.rdd(), 10, 10);

    // Once trained run all again through the trained model and enrich with the prediction
    Dataset<Row> carsMapped = sql
      .read()
      .json(carsWithLocation.map(r -> {
        Vector v = Vectors.dense(new double[]{
          r.getDouble("EngineTemp"),
          r.getDouble("OutsideTemp"),
          r.getDouble("Speed")});
        int predicted = model.predict(v);
        return r.put("predicted", predicted);
      })
      .map(JsonObject::toString));

    // For a sane output, group the data by their predicted cluster and count the number
    // of found cars per cluster
    carsMapped
      .groupBy(new Column("predicted"))
      .agg(functions.count("*"))
      .show();

    // Print out the cluster centers so a group has some meaning
    int i = 0;
    System.out.println(Arrays.asList("EngineTemp", "OutsideTemp", "Speed"));
    for (Vector center : model.clusterCenters()) {
      System.out.println((i++) + ": " + center);
    }

  }

}
