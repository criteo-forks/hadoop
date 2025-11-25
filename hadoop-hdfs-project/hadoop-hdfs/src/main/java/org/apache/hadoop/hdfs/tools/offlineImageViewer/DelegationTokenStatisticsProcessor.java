package org.apache.hadoop.hdfs.tools.offlineImageViewer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf;
import org.apache.hadoop.hdfs.server.namenode.FSImageUtil;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
import org.apache.hadoop.util.LimitInputStream;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.text.NumberFormat;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class DelegationTokenStatisticsProcessor {

  private final Configuration conf;
  private long totalTokens = 0L;
  private long totalKeys = 0L;

  private final Map<String, Integer> tokensByOwner = new HashMap<>();
  private final Map<String, Integer> tokensByRealUser = new HashMap<>();

  // Where to output results (can be System.out, a log stream, etc.)
  private final PrintStream out;

  public DelegationTokenStatisticsProcessor(Configuration conf, PrintStream out) {
    this.conf = conf;
    this.out = out;
  }

  void output() throws IOException {
    // Print global stats
    out.println("=== Delegation Token Statistics ===");
    out.println("Total number of delegation keys: " + totalKeys);
    out.println("Total number of delegation tokens: " + totalTokens);

    NumberFormat nf = NumberFormat.getInstance();

    out.println("\nCounts by owner:");
    tokensByOwner.entrySet().stream().sorted(Map.Entry.comparingByValue()).forEach(e ->
      out.println("  owner=" + e.getKey() + " -> " + nf.format(e.getValue()))
    );

    out.println("\nCounts by real user (a real user exists only when there is impersonation):");
    tokensByRealUser.entrySet().stream().sorted(Map.Entry.comparingByValue()).forEach(e ->
      out.println("  realUser=" + e.getKey() + " -> " + nf.format(e.getValue()))
    );
  }

  void visit(RandomAccessFile file) throws IOException {
    if (!FSImageUtil.checkFileFormat(file)) {
      throw new IOException("Unrecognized FSImage");
    }

    FsImageProto.FileSummary summary = FSImageUtil.loadSummary(file);
    try (FileInputStream in = new FileInputStream(file.getFD())) {
      for (FsImageProto.FileSummary.Section s : summary.getSectionsList()) {
        if (FSImageFormatProtobuf.SectionName.fromString(s.getName()) != FSImageFormatProtobuf.SectionName.SECRET_MANAGER) {
          continue;
        }

        in.getChannel().position(s.getOffset());
        InputStream is = FSImageUtil.wrapInputStreamForCompression(conf,
                summary.getCodec(), new BufferedInputStream(new LimitInputStream(
                        in, s.getLength())));
        run(is);
        output();
      }
    }
  }

  private void run(InputStream in) throws IOException {
    FsImageProto.SecretManagerSection s = FsImageProto.SecretManagerSection.parseDelimitedFrom(in);

    for (int i = 0; i < s.getNumKeys(); i++) {
      FsImageProto.SecretManagerSection.DelegationKey k = FsImageProto.SecretManagerSection.DelegationKey.parseDelimitedFrom(in);
      totalKeys++;
    }

    for (int i = 0; i < s.getNumTokens(); i++) {
      FsImageProto.SecretManagerSection.PersistToken t = FsImageProto.SecretManagerSection.PersistToken.parseDelimitedFrom(in);
      totalTokens++;
      increment(tokensByOwner, t.getOwner());
      increment(tokensByRealUser, t.getRealUser());
    }
  }

  /**
   * Helper to increment a counter in a map.
   */
  private void increment(Map<String, Integer> map, String key) {
    if (key == null) {
      key = "<null>";
    }
    Integer current = map.get(key);
    if (current == null) {
      current = 0;
    }
    map.put(key, current + 1);
  }

}