/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.util;

import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * The class to test {@link ReadWriteDiskValidator} and
 * {@link ReadWriteDiskValidatorMetrics}.
 */
public class TestCriteoReadWriteDiskValidator {

  private File testDir;

  @Before
  public void setUp() throws Exception {
    testDir = Files.createTempDirectory(
            Paths.get(System.getProperty("test.build.data")), "test").toFile();
  }

  @After
  public void tearDown() throws Exception {
    testDir.delete();
  }

  @Test
  public void testReadWriteDiskValidator()
      throws DiskErrorException, InterruptedException {
    int count = 100;
    CriteoReadWriteDiskValidator dv =
        (CriteoReadWriteDiskValidator) DiskValidatorFactory.getInstance(
            CriteoReadWriteDiskValidator.NAME);

    for (int i = 0; i < count; i++) {
      dv.checkStatus(testDir);
    }
  }

  @Test
  public void testCreatesAbsentFolders() throws DiskErrorException {
    CriteoReadWriteDiskValidator dv =
            (CriteoReadWriteDiskValidator) DiskValidatorFactory.getInstance(
                    CriteoReadWriteDiskValidator.NAME);

    File newDir = new File(testDir, "newDir");

    dv.checkStatus(newDir);
  }

  @Test
  public void testCheckFailures() throws Throwable {
    CriteoReadWriteDiskValidator dv =
        (CriteoReadWriteDiskValidator) DiskValidatorFactory.getInstance(
            CriteoReadWriteDiskValidator.NAME);

    try {
      Shell.execCommand(Shell.getSetPermissionCommand("000", false,
          testDir.getAbsolutePath()));
    } catch (Exception e){
      testDir.delete();
      throw e;
    }

    try {
      dv.checkStatus(testDir);
      fail("Disk check should fail.");
    } catch (DiskErrorException e) {
      assertEquals("Disk Check failed!", e.getMessage());
    }

    try {
      dv.checkStatus(testDir);
      fail("Disk check should fail.");
    } catch (DiskErrorException e) {
      assertEquals("Disk Check failed!", e.getMessage());
    }
  }

  @Test
  public void testShouldNotFailOnInterruptedThread() throws Throwable {

    AtomicReference<Throwable> error = new AtomicReference<>();

    CriteoReadWriteDiskValidator dv =
            (CriteoReadWriteDiskValidator) DiskValidatorFactory.getInstance(
                    CriteoReadWriteDiskValidator.NAME);

    Thread thread = new Thread(() -> {
      try {
        // Simulate that this thread was interrupted earlier
        Thread.currentThread().interrupt();

        dv.checkStatus(testDir);
      } catch (Throwable t) {
        error.set(t);
      }

      //Check that the interrupt flag is still present after existing the disk validator
      if (!Thread.currentThread().isInterrupted()) {
        error.set(new RuntimeException("The thread was supposed to be interrupted"));
      }
    });

    thread.start();
    thread.join();

    if (error.get() != null) {
      throw error.get();
    }
  }
}
