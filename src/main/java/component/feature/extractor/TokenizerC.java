package component.feature.extractor;


import component.Component;
import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.library.*;
import org.ansj.recognition.impl.StopRecognition;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import java.io.Serializable;


public class TokenizerC extends Component implements Serializable{

    private StopRecognition filter = new StopRecognition();
    private String inputCol;
    private String outputCol;

    public void run(){

        Dataset dataset = inputs.get("data").getDataset();
        Encoder<String> encoder = Encoders.STRING();
        Dataset data = dataset.map(new MapFunction <Row, String>() {
            public String call(Row row) {
                int index = row.fieldIndex(inputCol);
                String s = (String)row.get(index);
                Result result = ToAnalysis.parse(s).recognition(filter);
                StringBuffer sb = new StringBuffer();
                for (Term term: result.getTerms()) {
                    sb.append(term.getName());
                    sb.append(" ");
                }
                return sb.toString();
            }
        }, encoder);
        Dataset newDataset = data.withColumn(outputCol, data.col("value"));
        newDataset.show();
        if(outputs.containsKey("data"))
            outputs.get("data").setDataset(newDataset);
    }

    public void setParameters(JSONObject parameters) throws JSONException {

        if(parameters.has("dictPath") && !parameters.getJSONObject("dictPath").getString("value").equals(""))
            DicLibrary.put("dict", parameters.getJSONObject("dictPath").getString("value"));
        if(parameters.has("stopwordsPath") && !parameters.getJSONObject("stopwordsPath").getString("value").equals(""))
            StopLibrary.put("stop", parameters.getJSONObject("stopwordsPath").getString("value"));
        if(parameters.has("inputCol"))
            this.inputCol = parameters.getJSONObject("inputCol").getString("value");
        if(parameters.has("outputCol"))
            this.outputCol = parameters.getJSONObject("outputCol").getString("value");
    }

    @Test
    public void test() throws Exception{

        SparkConf conf = new SparkConf().setAppName("data-platform").setMaster("local");

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        DataFrameReader reader = spark.read().format("jdbc") ;
        String url = String.format("jdbc:mysql://%s:%d/%s", "localhost", 3306, "shop");
        reader.option("url",url);
        reader.option("dbtable", "(SELECT shohin_bunrui from shohin1) as tmp");

        reader.option("driver","com.mysql.jdbc.Driver");
        reader.option("user", "root");
        reader.option("password", "ma0722");
        Dataset<Row> dataset = reader.load();
        dataset.show();

        this.inputCol = "shohin_bunrui";
        Encoder<String> encoder = Encoders.STRING();

        Dataset data = dataset.map(new MapFunction <Row, String>() {
            public String call(Row row) {
                int index = row.fieldIndex(inputCol);
                String s = (String)row.get(index);
                Result result = ToAnalysis.parse(s).recognition(filter);
                StringBuffer sb = new StringBuffer();
                for (Term term: result.getTerms()) {
                    sb.append(term.getName());
                    sb.append(" ");
                }
                return sb.toString();
            }
        }, encoder);
        data.show();
    }

}
