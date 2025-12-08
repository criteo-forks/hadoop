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
 * BasicAndReadWriteDiskValidator will call BasicDiskValidator and ReadWriteDiskValidator
 * in serial.
 * This helps pieces of code that were designed supposing that BasicDiskValidator would create
 * a folder for them (as part of the validation).
 *
 * An issue was raised https://issues.apache.org/jira/browse/YARN-11906
 */
public class BasicAndReadWriteDiskValidator implements DiskValidator {

  public static final String NAME = "basic-and-read-write";

  private static final BasicDiskValidator basicDiskValidator = new BasicDiskValidator();

  private static final ReadWriteDiskValidator readWriteDiskValidator = new ReadWriteDiskValidator();

  @Override
  public void checkStatus(File dir) throws DiskErrorException {
    basicDiskValidator.checkStatus(dir);
    readWriteDiskValidator.checkStatus(dir);
  }
}
