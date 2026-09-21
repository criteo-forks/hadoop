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

#if __linux

#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <sys/eventfd.h>
#include <sys/stat.h>

#include <linux/limits.h>

/*
This file implements a standard cgroups out of memory listener.
*/

typedef struct _oom_listener_descriptors {
  /*
   * Command line that was called to run this process.
   */
  const char *command;
  /*
   * Event descriptor to watch.
   * It is filled in by the function,
   * if not specified, yet.
   */
  int event_fd;
  /*
   * cgroup.event_control file handle
   */
  int event_control_fd;
  /*
   * memory.oom_control file handle
   */
  int oom_control_fd;
  /*
   * cgroup.event_control path
   */
  char event_control_path[PATH_MAX];
  /*
   * memory.oom_control path
   */
  char oom_control_path[PATH_MAX];
  /*
   * Control command to write to
   * cgroup.event_control
   * Filled by the function.
   */
  char oom_command[25];
  /*
   * Length of oom_command filled by the function.
   */
  size_t oom_command_len;
  /*
   * Directory watch timeout
   */
  int watch_timeout;
} _oom_listener_descriptors;

/*
 Clean up allocated resources in a descriptor structure
*/
inline void cleanup(_oom_listener_descriptors *descriptors) {
  close(descriptors->event_fd);
  descriptors->event_fd = -1;
  close(descriptors->event_control_fd);
  descriptors->event_control_fd = -1;
  close(descriptors->oom_control_fd);
  descriptors->oom_control_fd = -1;
  descriptors->watch_timeout = 1000;
}

/*
 * Enable an OOM listener on the memory cgroup cgroup
 * descriptors: Structure that holds state for testing purposes
 * cgroup: cgroup path to watch. It has to be a memory cgroup
 * fd: File to forward events to. Normally this is stdout
 */
int oom_listener(_oom_listener_descriptors *descriptors, const char *cgroup, int fd);

/*
 * Size of the buffer memory.events is read into. The file holds a handful of
 * "<key> <count>" lines, so this is generous.
 */
#define OOM_LISTENER_EVENTS_BUFFER 4096

/*
 * State of the cgroup v2 listener. cgroup v2 has no cgroup.event_control and
 * no eventfd: memory.events is polled for POLLPRI directly and its counters
 * are compared against the previous reading.
 */
typedef struct _oom_listener_v2_descriptors {
  /*
   * Command line that was called to run this process.
   */
  const char *command;
  /*
   * memory.events file handle
   */
  int events_fd;
  /*
   * memory.pressure file handle carrying the PSI trigger, or -1 when the
   * kernel does not expose pressure information. Optional by design.
   */
  int pressure_fd;
  /*
   * memory.events path
   */
  char events_path[PATH_MAX];
  /*
   * memory.pressure path
   */
  char pressure_path[PATH_MAX];
  /*
   * The previous reading of the memory.events counters. high and max drive
   * the events we forward, oom and oom_kill are only reported on stderr.
   */
  uint64_t last_high;
  uint64_t last_max;
  uint64_t last_oom;
  uint64_t last_oom_kill;
  /*
   * Directory watch timeout
   */
  int watch_timeout;
} _oom_listener_v2_descriptors;

/*
 Clean up allocated resources in a v2 descriptor structure
*/
inline void cleanup_v2(_oom_listener_v2_descriptors *descriptors) {
  if (descriptors->events_fd != -1) {
    close(descriptors->events_fd);
    descriptors->events_fd = -1;
  }
  if (descriptors->pressure_fd != -1) {
    close(descriptors->pressure_fd);
    descriptors->pressure_fd = -1;
  }
  descriptors->watch_timeout = 1000;
}

/*
 * Enable an OOM listener on a cgroup v2 memory cgroup
 * descriptors: Structure that holds state for testing purposes
 * cgroup: cgroup path to watch. It has to be a cgroup v2 cgroup
 * fd: File to forward events to. Normally this is stdout
 */
int oom_listener_v2(_oom_listener_v2_descriptors *descriptors,
                    const char *cgroup, int fd);

#endif
