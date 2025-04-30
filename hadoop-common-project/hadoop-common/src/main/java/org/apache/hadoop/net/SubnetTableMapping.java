package org.apache.hadoop.net;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.NET_TOPOLOGY_SUBNET_TABLE_MAPPING_KEY_FILE_KEY;

import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import inet.ipaddr.ipv4.IPv4Address;
import inet.ipaddr.ipv4.IPv4AddressAssociativeTrie;
import inet.ipaddr.ipv4.IPv4AddressAssociativeTrie.IPv4AssociativeTrieNode;
import inet.ipaddr.ipv6.IPv6Address;
import inet.ipaddr.ipv6.IPv6AddressAssociativeTrie;
import inet.ipaddr.ipv6.IPv6AddressAssociativeTrie.IPv6AssociativeTrieNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class SubnetTableMapping extends CachedDNSToSwitchMapping {

  private static final Logger LOG = LoggerFactory.getLogger(SubnetTableMapping.class);

  public SubnetTableMapping() {
    super(new RawSubnetTableMapping());
  }

  private RawSubnetTableMapping getRawMapping() {
    return (RawSubnetTableMapping) rawMapping;
  }

  @Override
  public Configuration getConf() {
    return getRawMapping().getConf();
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    getRawMapping().setConf(conf);
  }

  @Override
  public void reloadCachedMappings() {
    super.reloadCachedMappings();
    getRawMapping().reloadCachedMappings();
  }

  private static class RawSubnetTableMapping extends Configured implements DNSToSwitchMapping {

    private static final Pattern LOCATION_PATTERN = Pattern.compile("^/([a-zA-Z0-9.]+)(/[a-zA-Z0-9.]+)*$");

    private IPv4AddressAssociativeTrie<String> iPv4AddressTrie;
    private IPv6AddressAssociativeTrie<String> iPv6AddressTrie;

    @Override
    public void setConf(Configuration conf) {
      super.setConf(conf);
      if (conf != null) {
        load(true);
      }
    }

    private boolean isValidLocation(String path) {
      return LOCATION_PATTERN.matcher(path).matches();
    }

    private void load(boolean firstTime) {
      IPv4AddressAssociativeTrie<String> tmpIPv4AddressTrie = new IPv4AddressAssociativeTrie<>();
      IPv6AddressAssociativeTrie<String> tmpIPv6AddressTrie = new IPv6AddressAssociativeTrie<>();

      String filename = getConf().get(NET_TOPOLOGY_SUBNET_TABLE_MAPPING_KEY_FILE_KEY, null);
      if (StringUtils.isBlank(filename)) {
        if (firstTime) {
          throw new RuntimeException("No subnet table mapping file specified");
        } else {
          LOG.warn("No subnet table mapping file specified, this is not expected to happen.");
          LOG.warn("Keeping original parsed configuration for safety");
          return;
        }
      }

      try (BufferedReader reader =
                   new BufferedReader(new InputStreamReader(
                           Files.newInputStream(Paths.get(filename)),
                           StandardCharsets.UTF_8))) {
        String line = reader.readLine();
        while (line != null) {
          line = line.trim();
          String[] columns = line.split("=");
          if (columns.length == 2) {
            String ip = columns[0];
            String location = columns[1];
            if (isValidLocation(location)) {
              IPAddress ipAddress = new IPAddressString(ip).getAddress();
              if (ipAddress.isIPv4()) {
                IPv4Address iPv4Address = ipAddress.toIPv4();
                tmpIPv4AddressTrie.put(iPv4Address, location);
              } else if (ipAddress.isIPv6()) {
                IPv6Address iPv6Address = ipAddress.toIPv6();
                tmpIPv6AddressTrie.put(iPv6Address, location);
              }
            } else {
              if (firstTime) {
                throw new RuntimeException("Subnet table mapping file has a corrupted format: " +
                        "Invalid location: " + location + "Expected pattern: " + LOCATION_PATTERN.pattern());
              } else {
                LOG.warn("Subnet table mapping file has a corrupted format: " +
                        "Invalid location: {} Expected pattern: {}", location, LOCATION_PATTERN.pattern());
                LOG.warn("Keeping original parsed configuration for safety");
              }
            }
          } else {
            if (firstTime) {
              throw new RuntimeException("Subnet table mapping file has a corrupted format: " + line);
            } else {
              LOG.warn("Subnet table mapping file has a corrupted format: {}", line);
              LOG.warn("Keeping original parsed configuration for safety");
              return;
            }
          }
          line = reader.readLine();
        }
      } catch (Exception e) {
        if (firstTime) {
          throw new RuntimeException("Error reading subnet table mapping file", e);
        } else {
          LOG.warn("Error reading subnet table mapping file", e);
          LOG.warn("Keeping original parsed configuration for safety");
          return;
        }
      }

      synchronized (this) {
        iPv4AddressTrie = tmpIPv4AddressTrie;
        iPv6AddressTrie = tmpIPv6AddressTrie;

        if (LOG.isInfoEnabled()) {
          LOG.info("Loaded SubnetTableMapping from {}", filename);
          LOG.info("IPv4 mapping:");
          LOG.info(iPv4AddressTrie.toString());
          LOG.info("IPv6 mapping:");
          LOG.info(iPv6AddressTrie.toString());
        }
      }
    }

    @Override
    public synchronized List<String> resolve(List<String> names) {
      List<String> results = new ArrayList<>(names.size());
      for (String name : names) {
        boolean added = false;
        IPAddress ipAddress = new IPAddressString(name).getAddress();
        if (ipAddress.isIPv4()) {
          IPv4AssociativeTrieNode<String> node = iPv4AddressTrie.longestPrefixMatchNode(ipAddress.toIPv4());
          if (node != null) {
            results.add(node.getValue());
            added = true;
          }
        } else if (ipAddress.isIPv6()) {
          IPv6AssociativeTrieNode<String> node = iPv6AddressTrie.longestPrefixMatchNode(ipAddress.toIPv6());
          if (node != null) {
            results.add(node.getValue());
            added = true;
          }
        }

        if (!added) {
          LOG.warn("Found no subnet for {}, setting NetworkTopology.DEFAULT_RACK", name);
          results.add(NetworkTopology.DEFAULT_RACK);
        }

      }
      return results;
    }

    @Override
    public void reloadCachedMappings() {
      load(false);
    }

    @Override
    public void reloadCachedMappings(List<String> names) {
      reloadCachedMappings();
    }
  }
}
