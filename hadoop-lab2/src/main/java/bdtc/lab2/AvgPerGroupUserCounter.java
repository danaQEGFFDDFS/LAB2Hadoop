package bdtc.lab2;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.var;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import static java.time.temporal.ChronoField.YEAR;

@AllArgsConstructor
@Slf4j
public class AvgPerGroupUserCounter {

    // Формат времени  - н-р, 'Oct 26 13:54:06'
    private static DateTimeFormatter formatter = new DateTimeFormatterBuilder()
            .appendPattern("MMM dd HH:mm:ss")
            .parseDefaulting(YEAR, 2018)
            .toFormatter();

    /**
     * Функция подсчета количества логов разного уровня в час.
     * Парсит строку лога, в т.ч. уровень логирования и час, в который событие было зафиксировано.
     * @param inputDataset - входной DataSet для анализа
     * @return результат подсчета в формате JavaRDD
     */
    public static JavaRDD<Row> countAvgPerGroupUser(Dataset<String> inputDataset, Dataset<String> inputMap) {
        //split by row using newline symbol)
        Dataset<String> words = inputDataset.map(s -> Arrays.toString(s.split("\n")).replace("[", "").replace("]", ""), Encoders.STRING());
        Dataset<String> maps = inputMap.map(s -> Arrays.toString(s.split("\n")).replace("[", "").replace("]", ""), Encoders.STRING());

        Dataset<AeroportUserMap> mapDataset = maps.map(s -> {
            String[] fields = s.split(",");
            return new AeroportUserMap(fields[0], fields[1]);
        }, Encoders.bean(AeroportUserMap.class)).coalesce(1);

//        System.out.println(mapDataset.get(0).get(0));
//        System.out.println(mapDataset.get(0).get(0));

        Dataset<LogLevelHour> aeroportDataset = words.map(s -> {
                    String[] fields = s.split(",");
                    long time = Long.parseLong(fields[1]);
                    Date date = new java.util.Date(time);
                    Calendar calendar = Calendar.getInstance();
                    calendar.setTime(date);
                    calendar.set(Calendar.SECOND, 0);
                    calendar.set(Calendar.MILLISECOND, 0);
                    return new LogLevelHour(calendar.getTimeInMillis(), fields[2], fields[3]);
                }, Encoders.bean(LogLevelHour.class))
                .coalesce(1);

        Dataset<Row> joined = aeroportDataset
                .join(mapDataset, mapDataset.col("aeroport").equalTo(aeroportDataset.col("aeroport2")))
                .withColumnRenamed("country", "country2").drop("aeroport2").drop("aeroport")
                .join(mapDataset, mapDataset.col("aeroport").equalTo(aeroportDataset.col("aeroport1"))).drop("aeroport1").drop("aeroport");

//        joined.collect();
        joined.show();

//        // Группирует по значениям часа и уровня логирования Group by sender
        Dataset<Row> t = joined.groupBy("hour", "country2", "country")
                .count()
                .toDF("hour", "country2", "country", "count");

        log.info("===========RESULT=========== ");
        t.show();
        return t.toJavaRDD();
    }

}
