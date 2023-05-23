package bdtc.lab2;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
//import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;


/**
 * Считает количество событий syslog разного уровная log level по часам.
 */
@Slf4j
public class SparkSQLApplication {

    /**
     * @param args - args[0]: входной файл, args[1] - выходная папка
     */
    public static void main(String[] args) {
        if (args.length < 2) {
            throw new RuntimeException("Usage: java -jar SparkSQLApplication.jar input.file outputDirectory");
        }
//        log.info("Appliction started!");
//        log.debug("Application started");
//        SparkSession sc = SparkSession
//                .builder()
//                .master("local")
//                .appName("SparkSQLApplication")
//                .getOrCreate();
//
//        Dataset<String> df = sc.read().text(args[0]).as(Encoders.STRING());
//        log.info("===============COUNTING...================");
//        JavaRDD<Row> result = LogLevelEventCounter.countLogLevelPerHour(df);
//        log.info("============SAVING FILE TO " + args[1] + " directory============");
//        result.saveAsTextFile(args[1]);

        try (CqlSession session = CqlSession.builder().build()) {                                   // (1)
            ResultSet rs = session.execute("select release_version from system.local");              // (2)
            Row row = rs.one();
            System.out.println(row.getString("release_version"));                                    // (3)
        }
    }
}
