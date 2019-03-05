package org.gbif.ala.spatial;

import static java.util.stream.Collectors.toList;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.gbif.kvs.SaltedKeyGenerator;
import org.gbif.kvs.geocode.GeocodeKVStoreConfiguration;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.kvs.indexing.options.ConfigurationMapper;
import org.supercsv.io.CsvListReader;
import org.supercsv.io.ICsvListReader;
import org.supercsv.prefs.CsvPreference;
import retrofit2.Call;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;
import retrofit2.http.Field;
import retrofit2.http.FormUrlEncoded;
import retrofit2.http.GET;
import retrofit2.http.POST;
import retrofit2.http.Path;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * A utility to load the crawled csv to hbase.
 *
 * Throwaway code quality!
 */
public class LayerLoader {

  public static void main(String[] args) throws IOException {

    final SaltedKeyGenerator keyGenerator = new SaltedKeyGenerator(10);

    Configuration configuration = HBaseConfiguration.create();
    configuration.set("hbase.zookeeper.quorum",
        "c3zk1.gbif-dev.org:2181,c3zk2.gbif-dev.org:2181,c3zk3.gbif-dev.org:2181");

    try (
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf("australia_kv"))
    ) {
      for (File source : Paths.get("/Users/tsj442/dev/au/csv").toFile().listFiles()) {
        System.out.println(source);

        try (
            InputStream is = new FileInputStream(source);
            BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF8"))
        ) {

          ICsvListReader csvReader = new CsvListReader(br, CsvPreference.STANDARD_PREFERENCE);

          List<String> header = csvReader.read();
          List<String> row;
          while ((row = csvReader.read()) != null) {

            LatLng key = new LatLng(Double.parseDouble(row.get(0)), Double.parseDouble(row.get(1)));
            //System.out.println(key.toString());
            Map<String,String> values = new HashMap<>();

            for (int i=2; i<header.size()&& i<row.size(); i++) {
              values.put(header.get(i), row.get(i));
            }
            Gson gson = new GsonBuilder().create();
            String payload = String.format("{\"layers\": %s}", gson.toJson(values));

            Put put = new Put(keyGenerator.computeKey(key.getLogicalKey()))
                .addColumn(Bytes.toBytes("v"), Bytes.toBytes("json"), Bytes.toBytes(payload));

            table.put(put);

            if (row.size() != 533) {
              System.out.println(row.get(0) + " has unexpected size: " + row.size());
            }
          }
        }
      }
    }
  }
}
