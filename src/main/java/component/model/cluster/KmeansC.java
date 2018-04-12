package component.model.cluster;

import component.Component;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;
import util.SparkUtil;

import java.io.IOException;

public class KmeansC extends Component {

    private KMeans model = new KMeans();
    private KMeansModel model_;

    private String path;

    public void run() throws Exception {
        Dataset dataset = inputs.get("data").getDataset();
        model_ = model.fit(dataset);
        Vector[] vectors = model_.clusterCenters();
        if(outputs.containsKey("model"))
            outputs.get("model").setModel(model_);
        if(outputs.containsKey("vectors"))
            outputs.get("vectors").setVectors(vectors);
        if(path != null && !path.equals("")) {
            save();
        }
    }

    public void setParameters(JSONObject parameters) throws JSONException {
        if(parameters.has("K"))
            model.setK(parameters.getJSONObject("K").getInt("value"));
        if(parameters.has("seed"))
            model.setSeed(parameters.getJSONObject("seed").getLong("value"));
        if(parameters.has("initSteps"))
            model.setInitSteps(parameters.getJSONObject("initSteps").getInt("value"));
        if(parameters.has("maxIter"))
            model.setMaxIter(parameters.getJSONObject("maxIter").getInt("value"));
        if(parameters.has("tol"))
            model.setTol(parameters.getJSONObject("tol").getDouble("value"));
        if(parameters.has("savePath"))
            this.path = parameters.getJSONObject("savePath").getString("value");
        if(parameters.has("features"))
            model.setFeaturesCol(parameters.getJSONObject("features").getString("value"));
    }

    public void save() throws IOException {
        model_.save(path);
    }
    
    
    @Test
    public void test() throws Exception{
        this.path = "/mode/Kmeans";
        Dataset dataset =  SparkUtil.readFromHDFS("/data/sample_cluster_data.txt", "libsvm");
        model_ = model.fit(dataset);
        if(path != null && !path.equals("")){
//            save();
            System.out.println("model saved success on " + this.path);
        }
    }
}
