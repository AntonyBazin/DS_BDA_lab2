package bdtc.lab2;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;

import java.util.List;


/**
 * Считает количество событий syslog разного уровная log level по часам.
 */
@Slf4j
public class SparkSQLApplication {

    /**
     * @param args - args[0]: входной файл, args[1] - выходная папка
     */
    public static void main(String[] args) {
        log.info("Application entrypoint");
        if (args.length < 2) {
            throw new RuntimeException("Usage: java -jar SparkSQLApplication.jar input.file outputDirectory");
        }
        log.info("Application started");
        SparkSession ss = SparkSession
                .builder()
                .master("local")
                .appName("SparkApplication")
                .getOrCreate();

        try (CqlSession session = CqlSession.builder().build()) {
            List<Row> list_of_rows = session.execute("SELECT * FROM hw2_1.flights").all();

            SparkContext sc = ss.sparkContext();
            System.out.println(list_of_rows);
            JavaSparkContext jsc = new JavaSparkContext(sc);
            JavaRDD<Row> df = jsc.parallelize(list_of_rows);
            log.info("===============COUNTING...================");
            JavaRDD<String> result = LogLevelEventCounter.countLogFlightsPerCountry(df);
            log.info("============SAVING FILE TO " + args[1] + " directory============");
            result.saveAsTextFile(args[1]);
        } catch (Exception e){
            log.info("Session error", e);
        }
        }
}
