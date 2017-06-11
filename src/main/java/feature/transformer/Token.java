package feature.transformer;

import feature.Feature;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.json.JSONException;
import org.json.JSONObject;


public class Token implements Feature {

    public Dataset<Row> run(Dataset dataset, String inputCol, JSONObject paramPair) throws JSONException {
        Tokenizer tokenizer = new Tokenizer()
                .setInputCol(inputCol)
                .setOutputCol(inputCol + this.getClass().getName());
        return tokenizer.transform(dataset);

    }
}
