package org.apache.hadoop.net;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.NET_TOPOLOGY_SUBNET_TABLE_MAPPING_KEY_FILE_KEY;

import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import inet.ipaddr.ipv4.IPv4Address;
import inet.ipaddr.ipv4.IPv4AddressTrie;
import inet.ipaddr.ipv6.IPv6Address;
import inet.ipaddr.ipv6.IPv6AddressTrie;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    private IPv4AddressTrie iPv4AddressTrie;
    private Map<IPv4Address, String> iPv4AddressToLocation;
    private IPv6AddressTrie iPv6AddressTrie;
    private Map<IPv6Address, String> iPv6AddressToLocation;

    @Override
    public void setConf(Configuration conf) {
      super.setConf(conf);
      if (conf != null) {
        load(true);
      }
    }

    private void load(boolean firstTime) {
      IPv4AddressTrie tmpIPv4AddressTrie = new IPv4AddressTrie();
      IPv6AddressTrie tmpIPv6AddressTrie = new IPv6AddressTrie();
      Map<IPv4Address, String> tmpIPv4AddressToLocation = new HashMap<>();
      Map<IPv6Address, String> tmpIPv6AddressToLocation = new HashMap<>();

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
            IPAddress ipAddress = new IPAddressString(ip).getAddress();
            if (ipAddress.isIPv4()) {
              IPv4Address iPv4Address = ipAddress.toIPv4();
              tmpIPv4AddressTrie.add(iPv4Address);
              tmpIPv4AddressToLocation.put(iPv4Address, location);
            } else if (ipAddress.isIPv6()) {
              IPv6Address iPv6Address = ipAddress.toIPv6();
              tmpIPv6AddressTrie.add(iPv6Address);
              tmpIPv6AddressToLocation.put(iPv6Address, location);
            }
          } else {
            if (firstTime) {
              throw new RuntimeException("Subnet table mapping file has a corrupted format: " + line);
            } else{
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
        iPv4AddressToLocation = tmpIPv4AddressToLocation;
        iPv6AddressTrie = tmpIPv6AddressTrie;
        iPv6AddressToLocation = tmpIPv6AddressToLocation;
      }
    }

    @Override
    public synchronized List<String> resolve(List<String> names) {
      List<String> results = new ArrayList<>(names.size());
      for(String name : names) {
        IPAddress ipAddress = new IPAddressString(name).getAddress();
        if (ipAddress.isIPv4()) {
          IPv4Address iPv4Address = ipAddress.toIPv4();
          IPv4Address iPv4Subnet = iPv4AddressTrie.longestPrefixMatch(iPv4Address);
          String result = iPv4AddressToLocation.get(iPv4Subnet);
          if (result != null) {
            results.add(result);
          } else {
            LOG.warn("Found no location for subnet {}, setting NetworkTopology.DEFAULT_RACK", iPv4Subnet);
            results.add(NetworkTopology.DEFAULT_RACK);
          }
        } else if (ipAddress.isIPv6()) {
          IPv6Address iPv6Address = ipAddress.toIPv6();
          IPv6Address iPv6Subnet = iPv6AddressTrie.longestPrefixMatch(iPv6Address);
          String result = iPv6AddressToLocation.get(iPv6Subnet);
          if (result != null) {
            results.add(result);
          } else {
            LOG.warn("Found no location for subnet {}, setting NetworkTopology.DEFAULT_RACK", iPv6Subnet);
            results.add(NetworkTopology.DEFAULT_RACK);
          }
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
