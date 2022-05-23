package bdtc.lab2;

import lombok.var;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static bdtc.lab2.AvgPerGroupUserCounter.countAvgPerGroupUser;

public class SparkTest {

//    final String testString1 = "1,6,Oct 26 13:54:06,fccd8a5f3a42,rsyslogd:, [origin software=\"rsyslogd\" swVersion=\"8.16.0\" x-pid=\"1401\" x-info=\"http://www.rsyslog.com\"] start\n";
//    final String testString2 = "2,3,Oct 26 14:54:06,fccd8a5f3a42,rsyslogd:, [origin software=\"rsyslogd\" swVersion=\"8.16.0\" x-pid=\"1401\" x-info=\"http://www.rsyslog.com\"] start\n";
//    final String testString3 = "3,6,Oct 26 14:54:06,fccd8a5f3a42,rsyslogd:, [origin software=\"rsyslogd\" swVersion=\"8.16.0\" x-pid=\"1401\" x-info=\"http://www.rsyslog.com\"] start\n";

    SparkSession ss = SparkSession
            .builder()
            .master("local")
            .appName("SparkSQLApplication")
            .getOrCreate();

    final String testString1 = "1,1510670903054,AEROPORT1,AEROPORT4";
    final String testString2 = "2,1510670908381,AEROPORT10,AEROPORT6";
    final String testString3 = "3,1510670915059,AEROPORT8,AEROPORT1";
    final String testString4 = "4,1510670918991,AEROPORT3,AEROPORT10";
    final String testString5 = "5,1510670923118,AEROPORT1,AEROPORT10";
    final String testString6 = "6,1510670925957,AEROPORT4,AEROPORT1";
    final String testString7 = "7,1510670927161,AEROPORT9,AEROPORT4";
    final String testString8 = "8,1510670931600,AEROPORT4,AEROPORT9";
    final String testString9 = "9,1510670936840,AEROPORT9,AEROPORT3";
    final String testString10 = "10,1510670941138,AEROPORT6,AEROPORT9";

    final String testString11 = "AEROPORT1,COUNTRY2";
    final String testString12 = "AEROPORT2,COUNTRY2";
    final String testString13 = "AEROPORT3,COUNTRY1";
    final String testString14 = "AEROPORT4,COUNTRY2";
    final String testString15 = "AEROPORT5,COUNTRY1";
    final String testString16 = "AEROPORT6,COUNTRY2";
    final String testString17 = "AEROPORT7,COUNTRY1";
    final String testString18 = "AEROPORT8,COUNTRY2";
    final String testString19 = "AEROPORT9,COUNTRY1";
    final String testString20 = "AEROPORT10,COUNTRY1";



    @Test
    public void testOneLog() {

        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<String> dudu = sc.parallelize(Arrays.asList(testString1,testString2,testString3,testString4,testString5,testString6,testString7,testString8,testString9,testString10));
        JavaRDD<String> dudu2 = sc.parallelize(Arrays.asList(testString11,testString12,testString13,testString14,testString15,testString16,testString17,testString18,testString19,testString20));
        JavaRDD<Row> result = countAvgPerGroupUser(ss.createDataset(dudu.rdd(), Encoders.STRING()), ss.createDataset(dudu2.rdd(), Encoders.STRING()));

        var res1 = result.collect();
        var r1 = res1.iterator().next().getLong(0) ;
        var r2 = res1.iterator().next().getString(1);
        var r3 = res1.iterator().next().getString(2);
        var r4 = res1.iterator().next().getLong(3);

        assert r1 == 1510670880000L;
        assert r2.equals("COUNTRY1");
        assert r3.equals("COUNTRY2");
        assert r4 == 2L;
    }
}
