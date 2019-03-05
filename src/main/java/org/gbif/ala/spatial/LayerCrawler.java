package org.gbif.ala.spatial;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import okhttp3.OkHttpClient;
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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

/**
 * A utility to crawl the ALA layers. Requires an input csv containing lat, lng (no header)
 * and an output directory.
 *
 * Throwaway code quality!
 */
public class LayerCrawler {
  private static final String BASE_URL = "https://sampling.ala.org.au/";

  private static Retrofit retrofit =
      new Retrofit.Builder()
          .baseUrl(BASE_URL)
          .addConverterFactory(JacksonConverterFactory.create())
          .validateEagerly(true)
          .build();


  public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
    // Get a list of all layer IDs to query for
    System.out.println("Getting layers");
    SamplingService service = retrofit.create(SamplingService.class);
    String layers = service.getLayers().execute().body().stream()
        .map(l -> String.valueOf(l.getId()))
        .collect(Collectors.joining(","));

    // partition the coordinates into batches of N to submit
    System.out.println("Partitioning coordinates " + Paths.get(LayerCrawler.class.getResource("/au.csv").toURI()));

    Stream<String> coordinateStream = Files.lines(Paths.get(LayerCrawler.class.getResource("/au.csv").toURI()));
    Collection<List<String>> partitioned = partition(coordinateStream, 10000);

    for (List<String> partition : partitioned) {
      String coords = String.join(",", partition);

      // Submit a job to generate a join
      Response<SamplingService.Batch> submit = service.submitIntersectBatch(layers, coords).execute();
      String batchId = submit.body().getBatchId();

      String state = "unknown";
      while (!state.equals("finished")  && !state.equals("error")) {
        Response<SamplingService.BatchStatus> status = service.getBatchStatus(batchId).execute();
        SamplingService.BatchStatus batchStatus = status.body();
        state = batchStatus.getStatus();
        System.out.println(batchId + ": " + state);

        if (!state.equals("finished")) {
          Thread.sleep(1000);

        } else {
          new File("/tmp/au").mkdirs();
          System.out.println("Downloading");
          try (
              ReadableByteChannel readableByteChannel = Channels.newChannel(new URL(batchStatus.getDownloadUrl()).openStream());
              FileOutputStream fileOutputStream = new FileOutputStream("/tmp/au/" + batchId + ".zip");
              FileChannel fileChannel = fileOutputStream.getChannel();
          ) {
            fileChannel.transferFrom(readableByteChannel, 0, Long.MAX_VALUE);

          }

          try (ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream("/tmp/au/" + batchId + ".zip"))) {
            ZipEntry entry = zipInputStream.getNextEntry();
            while (entry != null) {
              System.out.println("Unzipping " + entry.getName());
              java.nio.file.Path filePath = Paths.get("/tmp/au/" + batchId + ".csv");
              if (!entry.isDirectory()) {
                unzipFiles(zipInputStream, filePath);
              } else {
                Files.createDirectories(filePath);
              }



              zipInputStream.closeEntry();
              entry = zipInputStream.getNextEntry();
            }
          }

          System.out.println("Done");

        }
      }
    }
  }

  /**
   * Simple client to the ALA sampling service.
   */
  private interface SamplingService {

    /**
     * Return an inventory of layers in the ALA spatial portal
     */
    @GET("sampling-service/fields")
    Call<List<Layer>> getLayers();

    /**
     * Return an inventory of layers in the ALA spatial portal
     */
    @GET("sampling-service/intersect/batch/{id}")
    Call<BatchStatus> getBatchStatus(@Path("id") String id);

    /**
     * Trigger a job to run the intersection and return the job key to poll.
     * @param layerIds The layers of interest in comma separated form
     * @param coordinatePairs The coordinates in lat,lng,lat,lng... format
     * @return The batch submited
     */
    @FormUrlEncoded
    @POST("sampling-service/intersect/batch")
    Call<Batch> submitIntersectBatch(@Field("fids") String layerIds,
                                     @Field("points") String coordinatePairs);


    @JsonIgnoreProperties(ignoreUnknown = true)
    class Layer {
      private String id;

      public String getId() {
        return id;
      }

      public void setId(String id) {
        this.id = id;
      }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    class Batch {
      private String batchId;

      public String getBatchId() {
        return batchId;
      }

      public void setBatchId(String batchId) {
        this.batchId = batchId;
      }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    class BatchStatus {
      private String status;
      private String downloadUrl;

      public String getStatus() {
        return status;
      }

      public void setStatus(String status) {
        this.status = status;
      }

      public String getDownloadUrl() {
        return downloadUrl;
      }

      public void setDownloadUrl(String downloadUrl) {
        this.downloadUrl = downloadUrl;
      }
    }
  }

  /**
   * Util to partition a stream into fixed size windows.
   * See https://e.printstacktrace.blog/divide-a-list-to-lists-of-n-size-in-Java-8/
   */
  private static <T> Collection<List<T>> partition(Stream<T> stream, int size) {
    final AtomicInteger counter = new AtomicInteger(0);

    return stream
        .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / size))
        .values();
  }

  /**
   * Unzip the file to the path.
   */
  public static void unzipFiles(final ZipInputStream zipInputStream, final java.nio.file.Path unzipFilePath) throws IOException {

    try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(unzipFilePath.toAbsolutePath().toString()))) {
      byte[] bytesIn = new byte[1024];
      int read = 0;
      while ((read = zipInputStream.read(bytesIn)) != -1) {
        bos.write(bytesIn, 0, read);
      }
    }
  }
}
