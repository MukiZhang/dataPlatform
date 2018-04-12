package util;


import org.apache.ivy.util.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.RFormula;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Properties;

public class SparkUtil {

    public static SparkSession spark;

    static {
        connectSparkContext();
    }

    static public void connectSparkContext() {
        try{
            Properties properties = new Properties();
            properties.load(SparkUtil.class.getResourceAsStream("/cluster.properties"));
            String master = properties.getProperty("spark_master_ip");
            String port = properties.getProperty("spark_port");
//            SparkConf conf = new SparkConf().setAppName("data-platform").setMaster("spark://" + master + ":" + port);
            //SparkConf conf = new SparkConf().setAppName("data-platform").setMaster("local");
            SparkConf conf = new SparkConf().setAppName("data-platform").setMaster("local")
                    .set("spark.sql.warehouse.dir", "file:///C:/Users/Muki/workspace/GitHub/dataPlatform/spark-warehouse/")
                    .set("spark.local.dir","C:\\Users\\Muki\\workspace\\GitHub\\dataPlatform\\temp");
            spark = SparkSession.builder().config(conf).getOrCreate();
//            spark.sparkContext().addJar("jars/mysql-connector-java-5.1.46.jar");
        }catch (Exception e) {
            e.printStackTrace();
        }

    }

    static public Dataset readFromHDFS(String path, String dataFormat) throws Exception {
        if(!HDFSFileUtil.checkFile(path))
            throw new FileNotFoundException(path + " not found on hadoop!");
        return spark.read().format(dataFormat).load(HDFSFileUtil.HDFSPath(path));
    }

    static public Dataset readFromLocal(String path, String dataFormat) throws Exception{
        if (!new File(path).exists())
            throw new FileNotFoundException(path + " not found on local!");
        return spark.read().format(dataFormat).load(path);
    }

    static public Dataset readFromSQL(String sql) {
        return spark.sql(sql);
    }


}
