package com.himanshu.poc.mapreduce;


import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;

public class WindowsLocalFileSystem extends LocalFileSystem {

  /**
   * When running Hadoop job in Windows machine, it is required to set this file as FileSystem impl.
   * e.g.
   * https://bigdatanerd.wordpress.com/2013/11/14/mapreduce-running-mapreduce-in-windows-file-system-debug-mapreduce-in-eclipse/
   *
   * Configuration conf = getConf();
   * conf.set("fs.file.impl", "com.himanshu.poc.mapreduce.WindowsLocalFileSystem");
   * Job job = new Job(conf);
   * see @{@link WordCount}
   *
   */
  public WindowsLocalFileSystem() {
    super();

  }

  public boolean mkdirs(
      final Path path,
      final FsPermission permission)
      throws IOException {
    final boolean result = super.mkdirs(path);
    this.setPermission(path, permission);
    return result;
  }

  public void setPermission(
      final Path path,
      final FsPermission permission)
      throws IOException {
    try {
      super.setPermission(path, permission);
    } catch (final IOException e) {
      System.err.println("Cant help it, hence ignoring IOException setting persmission for path \"" + path +
          "\": " + e.getMessage());
    }
  }


}