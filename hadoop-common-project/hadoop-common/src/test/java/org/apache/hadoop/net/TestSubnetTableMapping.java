package org.apache.hadoop.net;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.NET_TOPOLOGY_SUBNET_TABLE_MAPPING_KEY_FILE_KEY;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.thirdparty.com.google.common.io.Files;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class TestSubnetTableMapping {

  @Test
  public void testShouldFailOnSetConf() {
    SubnetTableMapping mapping = new SubnetTableMapping();

    Configuration conf = new Configuration();
    conf.set(NET_TOPOLOGY_SUBNET_TABLE_MAPPING_KEY_FILE_KEY, "/this/file/does/not/exist");
    assertThrows(Exception.class, () -> mapping.setConf(conf));
  }

  @Test
  public void testResolve() throws IOException {
    File mapFile = File.createTempFile(getClass().getSimpleName() +
            ".testResolve", ".txt");
    mapFile.deleteOnExit();
    Files.asCharSink(mapFile, StandardCharsets.UTF_8).write(
            "10.180.246.0/25=/rack1\n"+
                    "10.176.76.0/25=/rack2\n"+
                    "10.176.0.0/14=/rack3\n"+
                    "0.0.0.0/0=/root\n" +
                    "fd09:0:b00::/64=/rack5\n" +
                    "fd00:0:1110::/64=/rack6\n" +
                    "::/0=/root\n"
    );

    SubnetTableMapping mapping = new SubnetTableMapping();

    Configuration conf = new Configuration();
    conf.set(NET_TOPOLOGY_SUBNET_TABLE_MAPPING_KEY_FILE_KEY, mapFile.getCanonicalPath());
    mapping.setConf(conf);

    List<String> names = new ArrayList<>();
    names.add("10.176.76.12");
    names.add("10.180.246.32");
    names.add("10.176.5.4");
    names.add("10.5.6.7");
    names.add("fd09:0:b00:0:b00:b00:b00:b00");
    names.add("fd00:0:1110:0:1110:1110:1110:1110");
    names.add("fd08:0:b00:b00:b00:b00:b00:b00");

    List<String> result = mapping.resolve(names);
    assertEquals(names.size(), result.size());
    assertEquals("/rack2", result.get(0));
    assertEquals("/rack1", result.get(1));
    assertEquals("/rack3", result.get(2));
    assertEquals("/root", result.get(3));
    assertEquals("/rack5", result.get(4));
    assertEquals("/rack6", result.get(5));
    assertEquals("/root", result.get(6));
  }

  @Test
  public void testReload() throws IOException {
    File mapFile = File.createTempFile(getClass().getSimpleName() +
            ".testResolve", ".txt");
    mapFile.deleteOnExit();
    Files.asCharSink(mapFile, StandardCharsets.UTF_8).write(
            "10.180.246.0/25=/rack1\n"
    );
    //First file is correct

    SubnetTableMapping mapping = new SubnetTableMapping();
    Configuration conf = new Configuration();
    conf.set(NET_TOPOLOGY_SUBNET_TABLE_MAPPING_KEY_FILE_KEY, mapFile.getCanonicalPath());
    mapping.setConf(conf);

    List<String> names = new ArrayList<>();
    names.add("10.180.246.3");
    List<String> results = mapping.resolve(names);
    assertEquals(1, results.size());
    assertEquals("/rack1", results.get(0));

    Files.asCharSink(mapFile, StandardCharsets.UTF_8).write(
            "10.180.246.0/25=/rack2\n"
    );
    //now it is on /rack2

    mapping.reloadCachedMappings();

    //resolve provides the same answer
    results = mapping.resolve(names);
    assertEquals(1, results.size());
    assertEquals("/rack2", results.get(0));
  }


  @Test
  public void testShouldNotFailOnReload() throws IOException {
    File mapFile = File.createTempFile(getClass().getSimpleName() +
            ".testResolve", ".txt");
    mapFile.deleteOnExit();
    Files.asCharSink(mapFile, StandardCharsets.UTF_8).write(
            "10.180.246.0/25=/rack1\n"
    );
    //First file is correct

    SubnetTableMapping mapping = new SubnetTableMapping();
    Configuration conf = new Configuration();
    conf.set(NET_TOPOLOGY_SUBNET_TABLE_MAPPING_KEY_FILE_KEY, mapFile.getCanonicalPath());
    mapping.setConf(conf);

    List<String> names = new ArrayList<>();
    names.add("10.180.246.3");
    List<String> results = mapping.resolve(names);
    assertEquals(1, results.size());
    assertEquals("/rack1", results.get(0));

    Files.asCharSink(mapFile, StandardCharsets.UTF_8).write(
            "10.180.246.0/25/rack1\n"
    );
    //New file is incorrect

    mapping.reloadCachedMappings();

    //resolve provides the same answer
    results = mapping.resolve(names);
    assertEquals(1, results.size());
    assertEquals("/rack1", results.get(0));
  }

  @Test
  public void testInvalidLocationFormat() throws IOException {
    File mapFile = File.createTempFile(getClass().getSimpleName() +
            ".testResolve", ".txt");
    mapFile.deleteOnExit();

    testInvalidLocationFormat(mapFile, "root/fr4");
    testInvalidLocationFormat(mapFile, "/root//fr4");
    testInvalidLocationFormat(mapFile, "/root/");
    testInvalidLocationFormat(mapFile, "/root/fr4/");
    testInvalidLocationFormat(mapFile, "/root/fr4/ ");
    testInvalidLocationFormat(mapFile, "/root/fr4/!rack");
  }

  private void testInvalidLocationFormat(File mapFile, String location) throws IOException {
    Files.asCharSink(mapFile, StandardCharsets.UTF_8).write(
            "10.180.246.0/25=" + location + "\n"
    );

    try {
      SubnetTableMapping mapping = new SubnetTableMapping();
      Configuration conf = new Configuration();
      conf.set(NET_TOPOLOGY_SUBNET_TABLE_MAPPING_KEY_FILE_KEY, mapFile.getCanonicalPath());
      mapping.setConf(conf);
      fail("Expected to fail on bad location format: " + location);
    } catch (Exception e) {}
  }


}
