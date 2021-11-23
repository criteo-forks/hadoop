/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.DU;
import org.apache.hadoop.fs.GetSpaceUsed;
import org.apache.hadoop.hdfs.server.datanode.BlockPoolSliceStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Same as ReplicaCachingGetSpaceUsed but uses DU to account for trash folder during rolling upgrade.
 * A custom DU class is used to avoid performing du when the folder does not exists.
 * <p>
 * The DU are performed asynchronously and their result is added to ReplicaCachingGetSpaceUsed calculation.
 * The total size actually used eventually converges to its true value.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ReplicaCachingGetSpaceUsedWithTrash extends ReplicaCachingGetSpaceUsed {
    static final Logger LOG =
            LoggerFactory.getLogger(ReplicaCachingGetSpaceUsedWithTrash.class);

    private final String bpid;
    private final File rootPath;
    private final GetSpaceUsed trashDU;

    public ReplicaCachingGetSpaceUsedWithTrash(Builder builder) throws IOException {
        super(builder);
        bpid = builder.getBpid();
        rootPath = builder.getPath();
        File trashPath = new File(rootPath, BlockPoolSliceStorage.TRASH_ROOT_DIR);
        LOG.info(
                "Setup disk size monitoring for [{} -> {}]. Trash folder: {}",
                bpid, rootPath, trashPath
        );
        trashDU = new GetSpaceUsed.Builder()
                .setPath(trashPath)
                .setConf(builder.getConf())
                .setKlass(TrashDU.class)
                .build();
    }

    @Override
    protected void setUsed(long usedValue) {
        /* Evaluate trash size before committing the definitive value
         * Since we rely on DU, the evaluation is asynchronous
         * Here we only read the last value computed
         */
        long trashUsed;

        try {
            trashUsed = trashDU.getUsed();
        } catch (IOException e) {
            trashUsed = 0;
            LOG.warn("Could not retrieved trash space used", e);
        }

        LOG.info("Reporting [{} -> {}] size. Blocks: {} Trash: {}",
                bpid, rootPath, usedValue, trashUsed);

        super.setUsed(usedValue + trashUsed);
    }

    public static class TrashDU extends DU {

        private final File trashPath;

        public TrashDU(Builder builder) throws IOException {
            super(builder);
            trashPath = builder.getPath();
        }

        @Override
        protected synchronized void refresh() {
            if (trashPath.exists()) {
                super.refresh();
            } else {
                LOG.info("Trash path {} does not exists, skipping size evaluation", trashPath);
                setUsed(0);
            }
        }
    }

}
