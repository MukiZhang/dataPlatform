package component.data;

import component.Component;
import org.apache.spark.sql.Dataset;
import org.json.JSONException;
import org.json.JSONObject;
import util.SparkUtil;


public class LocalData extends Component {

    private String path;
    private String dataFormat;

    public void run() throws  Exception {
        Dataset dataset = SparkUtil.readFromLocal(path, dataFormat);
        if(outputs.containsKey("vectors"))
            outputs.get("data").setDataset(dataset);
    }

    public void setParameters(JSONObject parameters) throws JSONException {
        if(parameters.has("dataFormat"))
            this.dataFormat = parameters.getJSONObject("dataFormat").getString("value");
        if(parameters.has("path"))
            this.path = parameters.getJSONObject("path").getString("value");
    }

}
