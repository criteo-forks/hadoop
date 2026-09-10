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

#include <sys/param.h>
#include <poll.h>
#include "oom_listener.h"

/*
 * Print an error.
*/
static inline void print_error(const char *file, const char *message,
                        ...) {
  fprintf(stderr, "%s ", file);
  va_list arguments;
  va_start(arguments, message);
  vfprintf(stderr, message, arguments);
  va_end(arguments);
}

/*
 * The memory.pressure trigger we install when the kernel exposes pressure
 * information: 15% of a one second window with every non-idle task of the
 * cgroup stalled on memory at once. The kernel only accepts windows between
 * 500 ms and 10 s, and notifies at most once per window.
 */
#define OOM_LISTENER_PSI_STALL_US 150000
#define OOM_LISTENER_PSI_WINDOW_US 1000000

/*
 * Compose <cgroup>/<file>, coping with a cgroup path that already ends in a
 * separator.
 */
static int build_cgroup_file_path(const char *command, char *target,
                                  size_t size, const char *cgroup,
                                  const char *file) {
  const char *pattern =
          cgroup[MAX(strlen(cgroup), 1) - 1] == '/'
          ? "%s%s" :"%s/%s";
  int written = snprintf(target, size, pattern, cgroup, file);
  if (written < 0 || (size_t) written >= size) {
    print_error(command, "path too long %s/%s\n", cgroup, file);
    return -1;
  }
  return 0;
}

/*
 * Listen to OOM events in a memory cgroup. See declaration for details.
 */
int oom_listener(_oom_listener_descriptors *descriptors, const char *cgroup, int fd) {
  const char *pattern =
          cgroup[MAX(strlen(cgroup), 1) - 1] == '/'
          ? "%s%s" :"%s/%s";

  /* Create an event handle, if we do not have one already*/
  if (descriptors->event_fd == -1 &&
      (descriptors->event_fd = eventfd(0, 0)) == -1) {
    print_error(descriptors->command, "eventfd() failed. errno:%d %s\n",
                errno, strerror(errno));
    return EXIT_FAILURE;
  }

  /*
   * open the file to listen to (memory.oom_control)
   * and write the event handle and the file handle
   * to cgroup.event_control
   */
  if (snprintf(descriptors->event_control_path,
               sizeof(descriptors->event_control_path),
               pattern,
               cgroup,
               "cgroup.event_control") < 0) {
    print_error(descriptors->command, "path too long %s\n", cgroup);
    return EXIT_FAILURE;
  }

  if ((descriptors->event_control_fd = open(
      descriptors->event_control_path,
      O_WRONLY|O_CREAT, 0600)) == -1) {
    print_error(descriptors->command, "Could not open %s. errno:%d %s\n",
                descriptors->event_control_path,
                errno, strerror(errno));
    return EXIT_FAILURE;
  }

  if (snprintf(descriptors->oom_control_path,
               sizeof(descriptors->oom_control_path),
               pattern,
               cgroup,
               "memory.oom_control") < 0) {
    print_error(descriptors->command, "path too long %s\n", cgroup);
    return EXIT_FAILURE;
  }

  if ((descriptors->oom_control_fd = open(
      descriptors->oom_control_path,
      O_RDONLY)) == -1) {
    print_error(descriptors->command, "Could not open %s. errno:%d %s\n",
                descriptors->oom_control_path,
                errno, strerror(errno));
    return EXIT_FAILURE;
  }

  if ((descriptors->oom_command_len = (size_t) snprintf(
      descriptors->oom_command,
      sizeof(descriptors->oom_command),
      "%d %d",
      descriptors->event_fd,
      descriptors->oom_control_fd)) < 0) {
    print_error(descriptors->command, "Could print %d %d\n",
                descriptors->event_control_fd,
                descriptors->oom_control_fd);
    return EXIT_FAILURE;
  }

  if (write(descriptors->event_control_fd,
            descriptors->oom_command,
            descriptors->oom_command_len) == -1) {
    print_error(descriptors->command, "Could not write to %s errno:%d\n",
                descriptors->event_control_path, errno);
    return EXIT_FAILURE;
  }

  if (close(descriptors->event_control_fd) == -1) {
    print_error(descriptors->command, "Could not close %s errno:%d\n",
                descriptors->event_control_path, errno);
    return EXIT_FAILURE;
  }
  descriptors->event_control_fd = -1;

  /*
   * Listen to events as long as the cgroup exists
   * and forward them to the fd in the argument.
   */
  for (;;) {
    uint64_t u;
    ssize_t ret = 0;
    struct stat stat_buffer = {0};
    struct pollfd poll_fd = {
        .fd = descriptors->event_fd,
        .events = POLLIN
    };

    ret = poll(&poll_fd, 1, descriptors->watch_timeout);
    if (ret < 0) {
      /* Error calling poll */
      print_error(descriptors->command,
                  "Could not poll eventfd %d errno:%d %s\n", ret,
                  errno, strerror(errno));
      return EXIT_FAILURE;
    }

    if (ret > 0) {
      /* Event counter values are always 8 bytes */
      if ((ret = read(descriptors->event_fd, &u, sizeof(u))) != sizeof(u)) {
        print_error(descriptors->command,
                    "Could not read from eventfd %d errno:%d %s\n", ret,
                    errno, strerror(errno));
        return EXIT_FAILURE;
      }

      /* Forward the value to the caller, typically stdout */
      if ((ret = write(fd, &u, sizeof(u))) != sizeof(u)) {
        print_error(descriptors->command,
                    "Could not write to pipe %d errno:%d %s\n", ret,
                    errno, strerror(errno));
        return EXIT_FAILURE;
      }
    } else if (ret == 0) {
      /* Timeout has elapsed*/

      /* Quit, if the cgroup is deleted */
      if (stat(cgroup, &stat_buffer) != 0) {
        break;
      }
    }
  }
  return EXIT_SUCCESS;
}

/*
 * Read the counters of memory.events, a flat keyed file with one
 * "<key> <count>" pair per line. It is read from offset 0 every time: the
 * counters are cumulative and we compare successive readings.
 * Returns -1 when the file could not be read, which is how a cgroup that was
 * removed under us shows up.
 */
static int read_memory_events(_oom_listener_v2_descriptors *descriptors,
                              uint64_t *high, uint64_t *max,
                              uint64_t *oom, uint64_t *oom_kill) {
  char buffer[OOM_LISTENER_EVENTS_BUFFER];
  char *line;
  char *state = NULL;
  ssize_t length = pread(descriptors->events_fd, buffer,
                         sizeof(buffer) - 1, 0);
  if (length < 0) {
    return -1;
  }
  buffer[length] = '\0';

  for (line = strtok_r(buffer, "\n", &state);
       line != NULL;
       line = strtok_r(NULL, "\n", &state)) {
    char key[64];
    unsigned long long value;
    if (sscanf(line, "%63s %llu", key, &value) != 2) {
      continue;
    }
    if (strcmp(key, "high") == 0) {
      *high = (uint64_t) value;
    } else if (strcmp(key, "max") == 0) {
      *max = (uint64_t) value;
    } else if (strcmp(key, "oom") == 0) {
      *oom = (uint64_t) value;
    } else if (strcmp(key, "oom_kill") == 0) {
      *oom_kill = (uint64_t) value;
    }
  }
  return 0;
}

/*
 * Try to install a memory.pressure trigger, so that we also wake up when the
 * kernel reports that memory really stalled.
 *
 * This is optional and failing to install it is the expected case: RHEL ships
 * PSI compiled in but switched off, so without psi=1 on the kernel command
 * line the file is absent. Report it once and carry on with memory.events
 * alone. The listener must never exit non-zero, and never degrade its
 * memory.events handling, because pressure information is unavailable.
 */
static void open_pressure_trigger(_oom_listener_v2_descriptors *descriptors,
                                  const char *cgroup) {
  char trigger[64];
  int written;

  descriptors->pressure_fd = -1;
  if (build_cgroup_file_path(descriptors->command, descriptors->pressure_path,
                             sizeof(descriptors->pressure_path),
                             cgroup, "memory.pressure") != 0) {
    return;
  }

  /* A trigger can only be installed through a writable descriptor. */
  if ((descriptors->pressure_fd =
           open(descriptors->pressure_path, O_RDWR | O_NONBLOCK)) == -1) {
    print_error(descriptors->command,
                "kernel memory pressure information unavailable at %s,"
                " watching memory.events alone. errno:%d %s\n",
                descriptors->pressure_path, errno, strerror(errno));
    return;
  }

  written = snprintf(trigger, sizeof(trigger), "full %d %d",
                     OOM_LISTENER_PSI_STALL_US, OOM_LISTENER_PSI_WINDOW_US);
  if (written < 0 || (size_t) written >= sizeof(trigger) ||
      write(descriptors->pressure_fd, trigger, (size_t) written) == -1) {
    print_error(descriptors->command,
                "could not install the memory pressure trigger '%s' on %s,"
                " watching memory.events alone. errno:%d %s\n",
                trigger, descriptors->pressure_path, errno, strerror(errno));
    close(descriptors->pressure_fd);
    descriptors->pressure_fd = -1;
  }
}

/*
 * Listen to OOM events in a cgroup v2 memory cgroup.
 * See declaration for details.
 */
int oom_listener_v2(_oom_listener_v2_descriptors *descriptors,
                    const char *cgroup, int fd) {
  if (build_cgroup_file_path(descriptors->command, descriptors->events_path,
                             sizeof(descriptors->events_path),
                             cgroup, "memory.events") != 0) {
    return EXIT_FAILURE;
  }

  if ((descriptors->events_fd =
           open(descriptors->events_path, O_RDONLY)) == -1) {
    print_error(descriptors->command, "Could not open %s. errno:%d %s\n",
                descriptors->events_path, errno, strerror(errno));
    return EXIT_FAILURE;
  }

  /*
   * Take the baseline before polling, so that the first notification is
   * compared against the state of the cgroup as it was at startup.
   */
  if (read_memory_events(descriptors, &descriptors->last_high,
                         &descriptors->last_max, &descriptors->last_oom,
                         &descriptors->last_oom_kill) != 0) {
    print_error(descriptors->command, "Could not read %s. errno:%d %s\n",
                descriptors->events_path, errno, strerror(errno));
    return EXIT_FAILURE;
  }

  open_pressure_trigger(descriptors, cgroup);

  /*
   * Listen to events as long as the cgroup exists and forward them to the fd
   * in the argument.
   */
  for (;;) {
    struct pollfd poll_fds[2];
    int poll_count = 0;
    int ret;
    struct stat stat_buffer = {0};
    uint64_t high = descriptors->last_high;
    uint64_t max = descriptors->last_max;
    uint64_t oom = descriptors->last_oom;
    uint64_t oom_kill = descriptors->last_oom_kill;

    poll_fds[poll_count].fd = descriptors->events_fd;
    poll_fds[poll_count].events = POLLPRI;
    poll_fds[poll_count].revents = 0;
    ++poll_count;
    if (descriptors->pressure_fd != -1) {
      poll_fds[poll_count].fd = descriptors->pressure_fd;
      poll_fds[poll_count].events = POLLPRI;
      poll_fds[poll_count].revents = 0;
      ++poll_count;
    }

    /*
     * Only POLLPRI is requested, and the two descriptors in the set have to
     * be read differently. Both claims below are from the kernel sources.
     *
     * memory.events is a plain kernfs file. kernfs_generic_poll() sleeps
     * until the file's event counter moves and then returns
     * DEFAULT_POLLMASK|EPOLLERR|EPOLLPRI (fs/kernfs/file.c, the same in 4.19
     * and in 6.12, only refactored out of kernfs_fop_poll in between). So
     * POLLERR accompanies every legitimate notification here and must never
     * be read as an error, and POLLHUP is never set at all: a cgroup that
     * goes away is caught by the stat() below and by the read failing.
     *
     * memory.pressure is not. psi_trigger_poll() (kernel/sched/psi.c)
     * returns DEFAULT_POLLMASK|EPOLLPRI for a trigger that fired, and adds
     * EPOLLERR only once the trigger is gone or PSI is switched off. There
     * POLLERR is terminal and has to be acted on, because such a descriptor
     * reports POLLPRI on every poll and would spin this loop.
     *
     * Asking for POLLIN would spin on both, since kernfs files always report
     * themselves readable. It also means a plain file, which is what the
     * unit test fixture is, only ever times out here. That is what turns
     * this into a poll of memory.events at watch_timeout and makes the mock
     * test possible, so the counters are compared on the timeout path too.
     */
    ret = poll(poll_fds, (nfds_t) poll_count, descriptors->watch_timeout);
    if (ret < 0) {
      if (errno == EINTR) {
        continue;
      }
      print_error(descriptors->command,
                  "Could not poll %s %d errno:%d %s\n",
                  descriptors->events_path, ret, errno, strerror(errno));
      return EXIT_FAILURE;
    }

    /* Quit, if the cgroup is deleted */
    if (stat(cgroup, &stat_buffer) != 0) {
      break;
    }

    /* A pressure trigger that died is dropped, and that is all. */
    if (poll_count > 1 &&
        (poll_fds[1].revents & (POLLERR | POLLHUP | POLLNVAL)) != 0) {
      print_error(descriptors->command,
                  "the memory pressure trigger on %s is gone,"
                  " watching memory.events alone\n",
                  descriptors->pressure_path);
      close(descriptors->pressure_fd);
      descriptors->pressure_fd = -1;
    }

    if (read_memory_events(descriptors, &high, &max, &oom, &oom_kill) != 0) {
      /* The cgroup went away under us */
      break;
    }

    /*
     * The kernel got there before us. Neither cgroup.kill nor a SIGKILL
     * through container-executor goes through the OOM killer, so an increment
     * here is the kernel's own kill and nothing else. The java side reads
     * this stream and counts it.
     */
    if (oom_kill > descriptors->last_oom_kill) {
      print_error(descriptors->command,
                  "kernel OOM: oom_kill increased by %llu to %llu in %s\n",
                  (unsigned long long) (oom_kill - descriptors->last_oom_kill),
                  (unsigned long long) oom_kill, cgroup);
    }
    if (oom > descriptors->last_oom) {
      print_error(descriptors->command,
                  "kernel OOM: oom increased by %llu to %llu in %s\n",
                  (unsigned long long) (oom - descriptors->last_oom),
                  (unsigned long long) oom, cgroup);
    }

    if (high > descriptors->last_high || max > descriptors->last_max) {
      /*
       * One event per wake up, whatever the size of the counter deltas. The
       * java side re-reads the state of the cgroup itself and decides there,
       * so telling it twice about the same wake up buys nothing.
       */
      uint64_t event = 1;
      ssize_t written = write(fd, &event, sizeof(event));
      if (written != (ssize_t) sizeof(event)) {
        print_error(descriptors->command,
                    "Could not write to pipe %d errno:%d %s\n",
                    (int) written, errno, strerror(errno));
        return EXIT_FAILURE;
      }
    }

    descriptors->last_high = high;
    descriptors->last_max = max;
    descriptors->last_oom = oom;
    descriptors->last_oom_kill = oom_kill;
  }
  return EXIT_SUCCESS;
}

#endif
