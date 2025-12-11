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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * CriteoWriteDiskValidator is equivalent to ReadWriteDiskValidator but fixes several issues:
 *   - It will create folders if they do not exist, as this is the behavior expected by LocalDirsHandler
 *   - It will not produce metrics as it is used to create folders for all new containers,
 *     leading to constantly growing count of metrics
 *
 * Issues raised:
 *   - https://issues.apache.org/jira/browse/YARN-11906
 *   - https://issues.apache.org/jira/browse/YARN-11908
 *
 * Handling thread interruption is critical here. In at least one real scenario,
 * the thread invoking this validator may already be interrupted:
 *
 *   • During NodeManager restart, the ResourceLocalizer thread is often the first
 *     to call LocalDirsHandler.
 *
 *   • LocalDirsHandler builds a LocalDirsHandler.Context, which invokes this
 *     DiskValidator for every YARN local directory.
 *
 *   • The ResourceLocalizer thread may be interrupted as part of Hadoop’s normal
 *     cooperative shutdown mechanism (e.g., when a resource localization failure
 *     occurs, such as a missing container dependency).
 *
 *   • If the thread is interrupted while this validator performs I/O,
 *     java.nio.channels operations will fail with ClosedByInterruptException.
 *
 *   • If such an interruption happens while the LocalDirsHandler.Context is being
 *     built for the first time, the context may end up with no valid disks.
 *     This is problematic because:
 *
 *         – The context is initialized from an initial list of “good” directories.
 *         – It is recomputed only if that list changes — which is unlikely (happens only on real disk failure).
 *
 *   • As a result, even when the disks themselves are perfectly healthy, all
 *     subsequent container scheduling attempts will see an empty context and
 *     fail with: “No space available in any of the local directories.”
 *
 * To avoid this incorrect state, interruptions must be neutralized during the
 * validation process and restored afterward.
 *
 */
public class CriteoReadWriteDiskValidator implements DiskValidator {

  public static final String NAME = "criteo-read-write";
  private static final Random RANDOM = new Random();

  @Override
  public void checkStatus(File dir) throws DiskErrorException {

    // Get and clear the interruption state
    boolean interrupted = Thread.interrupted();

    Path tmpFile = null;
    try {

      // check the directory presence and permission.
      DiskChecker.checkDir(dir);

      // create a tmp file under the dir
      tmpFile = Files.createTempFile(dir.toPath(), "test", "tmp");

      // write 16 bytes into the tmp file
      byte[] inputBytes = new byte[16];
      RANDOM.nextBytes(inputBytes);
      Files.write(tmpFile, inputBytes);

      // read back
      byte[] outputBytes = Files.readAllBytes(tmpFile);

      // validation
      if (!Arrays.equals(inputBytes, outputBytes)) {
        throw new DiskErrorException("Data in file has been corrupted.");
      }
    } catch (IOException e) {
      throw new DiskErrorException("Disk Check failed!", e);
    } finally {
      // delete the file
      if (tmpFile != null) {
        try {
          Files.delete(tmpFile);
        } catch (IOException e) {
          throw new DiskErrorException("File deletion failed!", e);
        }
      }
      // set back the interrupt flag if needed
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
