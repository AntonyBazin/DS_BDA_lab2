package bdtc.lab2;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import com.datastax.oss.driver.api.core.cql.Row;
import java.time.format.DateTimeFormatter;

@AllArgsConstructor
@Slf4j
public class LogLevelEventCounter {

    // Формат времени логов - н-р, '2023-04-09T00:30:00+00:00'
    private static final DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    /**
     * Функция подсчета
     * @param inputRDD - входной RDD для анализа
     * @return результат подсчета в формате JavaRDD
     */
    public static JavaRDD<String> countLogFlightsPerCountry(JavaRDD<Row> inputRDD) {
        JavaRDD<String> mappedRDD = inputRDD.map(LogLevelEventCounter::mapRows);
        return mappedRDD.map(LogLevelEventCounter::reduceRows);
    }

    public static String mapRows(Row row){
        String row_str = row.toString();
        return row_str;
    }

    public static String reduceRows(String row){
        return row;
    }

}
