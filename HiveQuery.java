import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class HiveQuery {
    final static String HEADERSTART = "Date";
    public static double min(JavaRDD<Double> rdd){
         return rdd.reduce((entry,min) -> entry<min ? entry : min);
    }
    public static double max(JavaRDD<Double> rdd){
        return rdd.reduce((entry,max) -> entry>max ? entry : max);
    }
    public static double avg(JavaRDD<Double> rdd){
        return rdd.reduce((entry,sec) -> (entry+sec)/2 );
    }
    public static Dataset dataSet(JavaRDD<String> rdd, SQLContext sqlContext){
        JavaRDD<StockQuote> sqRDD =  rdd.filter(x -> (!x.contains(HEADERSTART))).map(new Function<String, StockQuote>() {
            @Override
            public StockQuote call(String line) throws Exception {
                String[] data = line.split(",");
                StockQuote sq = new StockQuote();
                sq.setDate(data[0]);
                sq.setHigh(Double.parseDouble(data[2]));
                sq.setOpen(Double.parseDouble(data[1]));
                sq.setLow(Double.parseDouble(data[3]));
                sq.setClose(Double.parseDouble(data[4]));
                sq.setAdjClose(Double.parseDouble(data[5]));
                sq.setVolume(Double.parseDouble(data[6]));
                return sq;
            }
        });

        Dataset<Row> ds = sqlContext.createDataFrame(sqRDD, StockQuote.class);
        ds.registerTempTable("AAPL");
        return ds;
    }
    public static void main(String[] args){
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("Read").setMaster("local");
        JavaSparkContext sc =new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
        JavaRDD<String> rdd = sc.textFile("C:\\Users\\kathi\\Desktop\\AAPL.csv",5);
        System.out.println("Count : "+rdd.filter(x -> (!x.contains(HEADERSTART) && x.trim().length()>0)).count());
        JavaRDD<Double> values = rdd.filter(x -> (!x.contains(HEADERSTART))).map(x ->Double.parseDouble(x.split(",",-1)[5]));
        System.out.println(min(values));
        System.out.println(max(values));
        System.out.println(avg(values));
        Dataset<Row> dataFrame = dataSet(rdd, sqlContext);
        dataFrame.show();
        dataFrame.select("date","high").where("low = 115.639999").show();
    }

}
