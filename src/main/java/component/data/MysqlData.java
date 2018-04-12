package component.data;

import component.Component;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;
import util.SparkUtil;

public class MysqlData extends Component {

    private String ip;
    private String username;
    private String password;
    private String database;
    private int port;
    private String sql;

    public void run() throws  Exception {

        DataFrameReader reader = SparkUtil.spark.read().format("jdbc");
        String url = String.format("jdbc:mysql://%s:%d/%s", ip, port, database);
        reader.option("url",url);
        reader.option("dbtable", sql);
        reader.option("driver","com.mysql.jdbc.Driver");
        reader.option("user", username);
        reader.option("password", password);

        System.out.println(url);
        Dataset<Row> dataset = reader.load();
//        for (String v : dataset.columns())
//            System.out.println(v);
        dataset.show();
        if(outputs.containsKey("data"))
            outputs.get("data").setDataset(dataset);

    }

    public void setParameters(JSONObject parameters) throws JSONException {
        if(parameters.has("ip"))
            this.ip = parameters.getJSONObject("ip").getString("value");
        if(parameters.has("password"))
            this.password = parameters.getJSONObject("password").getString("value");
        if(parameters.has("username"))
            this.username = parameters.getJSONObject("username").getString("value");
        if(parameters.has("database"))
            this.database = parameters.getJSONObject("database").getString("value");
        if(parameters.has("port"))
            this.port = parameters.getJSONObject("port").getInt("value");
        if(parameters.has("sql"))
            this.sql = parameters.getJSONObject("sql").getString("value");
    }
    
    @Test
    public void test() throws Exception{
//        this.ip = "localhost";
//        this.username = "root";
//        this.password = "ma0722";
//        this.database = "bridge";
//        this.port = 3306;
//        this.sql = "(SELECT rawdata_id, created_at, updated_at from rawdata) as tmp";

        this.ip = "10.109.247.63";
        this.username = "root";
        this.password = "hadoop";
        this.database = "db_weibo";
        this.port = 3306;
        this.sql = "(SELECT * from weibo_original limit 10) as tmp";
        run();
    }
}
