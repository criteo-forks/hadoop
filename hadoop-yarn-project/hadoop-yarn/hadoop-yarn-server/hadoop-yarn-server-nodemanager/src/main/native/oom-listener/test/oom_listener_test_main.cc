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

extern "C" {
#include "oom_listener.h"
#include <time.h>
#include <sys/wait.h>
}

#include <gtest/gtest.h>
#include <fstream>
#include <mutex>
#include <string>

#define CGROUP_ROOT "/sys/fs/cgroup/memory/"
#define TEST_ROOT "/tmp/test-oom-listener/"
#define CGROUP_TASKS "tasks"
#define CGROUP_OOM_CONTROL "memory.oom_control"
#define CGROUP_LIMIT_PHYSICAL "memory.limit_in_bytes"
#define CGROUP_LIMIT_SWAP "memory.memsw.limit_in_bytes"
#define CGROUP_EVENT_CONTROL "cgroup.event_control"
#define CGROUP_LIMIT (5 * 1024 * 1024)

// We try multiple cgroup directories
// We try first the official path to test
// in production
// If we are running as a user we fall back
// to mock cgroup
static const char *cgroup_candidates[] = { CGROUP_ROOT, TEST_ROOT };

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

class OOMListenerTest : public ::testing::Test {
private:
  char cgroup[PATH_MAX];
  const char* cgroup_root;
public:
  OOMListenerTest() : cgroup_root(NULL) {}

  virtual ~OOMListenerTest() = default;
  virtual const char* GetCGroup() { return cgroup; }
  virtual void SetUp() {
    struct stat cgroup_memory = {};
    for (unsigned int i = 0; i < GTEST_ARRAY_SIZE_(cgroup_candidates); ++i) {
      cgroup_root = cgroup_candidates[i];

      // Try to create the root.
      // We might not have permission and
      // it may already exist
      mkdir(cgroup_root, 0700);

      if (0 != stat(cgroup_root, &cgroup_memory)) {
        printf("%s missing. Skipping test\n", cgroup_root);
        continue;
      }

      timespec timespec1 = {};
      if (0 != clock_gettime(CLOCK_MONOTONIC, &timespec1)) {
        ASSERT_TRUE(false) << " clock_gettime failed\n";
      }

      if (snprintf(cgroup, sizeof(cgroup), "%s%lx/",
                        cgroup_root, timespec1.tv_nsec) <= 0) {
        cgroup[0] = '\0';
        printf("%s snprintf failed\n", cgroup_root);
        continue;
      }

      // Create a cgroup named the current timestamp
      // to make it quasi unique
      if (0 != mkdir(cgroup, 0700)) {
        printf("%s not writable.\n", cgroup);
        continue;
      }
      break;
    }

    ASSERT_EQ(0, stat(cgroup, &cgroup_memory))
                  << "Cannot use or simulate cgroup " << cgroup;
  }
  virtual void TearDown() {
    if (cgroup[0] != '\0') {
      rmdir(cgroup);
    }
    if (cgroup_root != NULL &&
        cgroup_root != cgroup_candidates[0]) {
      rmdir(cgroup_root);
    }
  }
};

/*
  Unit test for cgroup testing. There are two modes.
  If the unit test is run as root and we have cgroups
  we try to crate a cgroup and generate an OOM.
  If we are not running as root we just sleep instead of
  hogging memory and simulate the OOM by sending
  an event in a mock event fd mock_oom_event_as_user.
*/
TEST_F(OOMListenerTest, test_oom) {
  // Disable OOM killer
  std::ofstream oom_control;
  std::string oom_control_file =
      std::string(GetCGroup()).append(CGROUP_OOM_CONTROL);
  oom_control.open(oom_control_file.c_str(), oom_control.out);
  oom_control << 1 << std::endl;
  oom_control.close();

  // Set a low enough limit for physical
  std::ofstream limit;
  std::string limit_file =
      std::string(GetCGroup()).append(CGROUP_LIMIT_PHYSICAL);
  limit.open(limit_file.c_str(), limit.out);
  limit << CGROUP_LIMIT << std::endl;
  limit.close();

  // Set a low enough limit for physical + swap
  std::ofstream limitSwap;
  std::string limit_swap_file =
      std::string(GetCGroup()).append(CGROUP_LIMIT_SWAP);
  limitSwap.open(limit_swap_file.c_str(), limitSwap.out);
  limitSwap << CGROUP_LIMIT << std::endl;
  limitSwap.close();

  // Event control file to set
  std::string memory_control_file =
      std::string(GetCGroup()).append(CGROUP_EVENT_CONTROL);

  // Tasks file to check
  std::string tasks_file =
      std::string(GetCGroup()).append(CGROUP_TASKS);

  int mock_oom_event_as_user = -1;
  struct stat stat1 = {};
  if (0 != stat(memory_control_file.c_str(), &stat1)) {
    // We cannot tamper with cgroups
    // running as a user, so simulate an
    // oom event
    mock_oom_event_as_user = eventfd(0, 0);
  }
  const int simulate_cgroups =
      mock_oom_event_as_user != -1;

  pid_t mem_hog_pid = fork();
  if (!mem_hog_pid) {
    // Child process to consume too much memory
    if (simulate_cgroups) {
      std::cout << "Simulating cgroups OOM" << std::endl;
      for (;;) {
        sleep(1);
      }
    } else {
      // Wait until we are added to the cgroup
      // so that it is accounted for our mem
      // usage
      pid_t cgroupPid;
      do {
        std::ifstream tasks;
        tasks.open(tasks_file.c_str(), tasks.in);
        tasks >> cgroupPid;
        tasks.close();
      } while (cgroupPid != getpid());

      // Start consuming as much memory as we can.
      // cgroup will stop us at CGROUP_LIMIT
      const int bufferSize = 1024 * 1024;
      std::cout << "Consuming too much memory" << std::endl;
      for (;;) {
        auto buffer = (char *) malloc(bufferSize);
        if (buffer != NULL) {
          for (int i = 0; i < bufferSize; ++i) {
            buffer[i] = (char) std::rand();
          }
        }
      }
    }
  } else {
    // Parent test
    ASSERT_GE(mem_hog_pid, 1) << "Fork failed " << errno;

    // Put child into cgroup
    std::ofstream tasks;
    tasks.open(tasks_file.c_str(), tasks.out);
    tasks << mem_hog_pid << std::endl;
    tasks.close();

    // Create pipe to get forwarded eventfd
    int test_pipe[2];
    ASSERT_EQ(0, pipe(test_pipe));

    // Launch OOM listener
    // There is no race condition with the process
    // running out of memory. If oom is 1 at startup
    // oom_listener will send an initial notification
    pid_t listener = fork();
    if (listener == 0) {
      // child listener forwarding cgroup events
      _oom_listener_descriptors descriptors = {
          "test",
          mock_oom_event_as_user,
          -1,
          -1,
          {0},
          {0},
          {0},
          0,
          100
      };
      int ret = oom_listener(&descriptors, GetCGroup(), test_pipe[1]);
      cleanup(&descriptors);
      close(test_pipe[0]);
      close(test_pipe[1]);
      exit(ret);
    } else {
    // Parent test
      uint64_t event_id = 1;
      if (simulate_cgroups) {
        // We cannot tamper with cgroups
        // running as a user, so simulate an
        // oom event
        ASSERT_EQ(sizeof(event_id),
                  write(mock_oom_event_as_user,
                        &event_id,
                        sizeof(event_id)));
      }
      ASSERT_EQ(sizeof(event_id),
                read(test_pipe[0],
                     &event_id,
                     sizeof(event_id)))
                    << "The event has not arrived";
      close(test_pipe[0]);
      close(test_pipe[1]);

      // Simulate OOM killer
      ASSERT_EQ(0, kill(mem_hog_pid, SIGKILL));

      // Verify that process was killed
      int* mem_hog_status = {};
      pid_t exited0 = wait(mem_hog_status);
      ASSERT_EQ(mem_hog_pid, exited0)
        << "Wrong process exited";
      ASSERT_EQ(NULL, mem_hog_status)
        << "Test process killed with invalid status";

      if (mock_oom_event_as_user != -1) {
        ASSERT_EQ(0, unlink(oom_control_file.c_str()));
        ASSERT_EQ(0, unlink(limit_file.c_str()));
        ASSERT_EQ(0, unlink(limit_swap_file.c_str()));
        ASSERT_EQ(0, unlink(tasks_file.c_str()));
        ASSERT_EQ(0, unlink(memory_control_file.c_str()));
      }
      // Once the cgroup is empty delete it
      ASSERT_EQ(0, rmdir(GetCGroup()))
                << "Could not delete cgroup " << GetCGroup();

      // Check that oom_listener exited on the deletion of the cgroup
      int* oom_listener_status = {};
      pid_t exited1 = wait(oom_listener_status);
      ASSERT_EQ(listener, exited1)
        << "Wrong process exited";
      ASSERT_EQ(NULL, oom_listener_status)
        << "Listener process exited with invalid status";
    }
  }
}

#define CGROUP_MEMORY_EVENTS "memory.events"

// How long to give the listener to take its baseline reading of
// memory.events before the counters are moved under it, and how long to wait
// for an event that must not come. Both are several times the watch_timeout
// the tests configure.
#define V2_SETTLE_US 500000

/*
  The cgroup v2 listener polls memory.events for POLLPRI. A plain file never
  reports POLLPRI, so against a mock directory the listener falls back to
  re-reading memory.events on every watch_timeout, and that is what these
  tests drive. A real cgroup cannot be used here: memory.events is read only,
  so moving its counters would take an actual out of memory situation.
*/
class OOMListenerV2Test : public ::testing::Test {
private:
  char cgroup[PATH_MAX];
public:
  OOMListenerV2Test() { cgroup[0] = '\0'; }

  virtual ~OOMListenerV2Test() = default;
  virtual const char* GetCGroup() { return cgroup; }

  virtual void SetUp() {
    struct stat cgroup_stat = {};
    timespec timespec1 = {};

    // Note for the reader chasing a production issue: this is a mock, not
    // /sys/fs/cgroup. Whether the host runs a unified hierarchy makes no
    // difference to these tests.
    mkdir(TEST_ROOT, 0700);
    ASSERT_EQ(0, stat(TEST_ROOT, &cgroup_stat))
                  << "Cannot create " << TEST_ROOT;

    ASSERT_EQ(0, clock_gettime(CLOCK_MONOTONIC, &timespec1))
                  << "clock_gettime failed";
    ASSERT_LT(0, snprintf(cgroup, sizeof(cgroup), "%s%lx-v2/",
                          TEST_ROOT, timespec1.tv_nsec));
    ASSERT_EQ(0, mkdir(cgroup, 0700)) << "Cannot create " << cgroup;
  }

  virtual void TearDown() {
    if (cgroup[0] != '\0') {
      unlink(EventsFile().c_str());
      rmdir(cgroup);
    }
    rmdir(TEST_ROOT);
  }

  std::string EventsFile() {
    return std::string(GetCGroup()).append(CGROUP_MEMORY_EVENTS);
  }

  void WriteEvents(uint64_t high, uint64_t max,
                   uint64_t oom, uint64_t oom_kill) {
    std::ofstream events;
    events.open(EventsFile().c_str(),
                std::ofstream::out | std::ofstream::trunc);
    events << "low 0" << std::endl;
    events << "high " << high << std::endl;
    events << "max " << max << std::endl;
    events << "oom " << oom << std::endl;
    events << "oom_kill " << oom_kill << std::endl;
    events.close();
  }

  // Run the listener in a child process, forwarding its events to
  // events_pipe[1] and, if error_fd is not -1, its standard error there.
  pid_t ForkListener(int *events_pipe, int error_fd) {
    pid_t listener = fork();
    if (listener != 0) {
      return listener;
    }
    if (error_fd != -1) {
      dup2(error_fd, STDERR_FILENO);
    }
    _oom_listener_v2_descriptors descriptors = {
        "oom-listener",
        -1,
        {0},
        0,
        0,
        0,
        0,
        100
    };
    int ret = oom_listener_v2(&descriptors, GetCGroup(), events_pipe[1]);
    cleanup_v2(&descriptors);
    close(events_pipe[0]);
    close(events_pipe[1]);
    exit(ret);
  }

  // Deleting the cgroup is what makes the listener exit, and it has to exit
  // successfully.
  void ExpectCleanExit(pid_t listener) {
    int listener_status = 0;
    ASSERT_EQ(0, unlink(EventsFile().c_str()));
    ASSERT_EQ(0, rmdir(GetCGroup()))
                  << "Could not delete cgroup " << GetCGroup();
    ASSERT_EQ(listener, waitpid(listener, &listener_status, 0))
                  << "Wrong process exited";
    ASSERT_TRUE(WIFEXITED(listener_status))
                  << "The listener did not exit normally";
    ASSERT_EQ(EXIT_SUCCESS, WEXITSTATUS(listener_status))
                  << "The listener exited with a failure";
    cgroup[0] = '\0';
  }
};

/*
  An increment of memory.events' high counter produces exactly one event,
  whatever the size of the increment and however many counters moved.
*/
TEST_F(OOMListenerV2Test, test_high_event_is_coalesced) {
  WriteEvents(0, 0, 0, 0);

  int test_pipe[2];
  ASSERT_EQ(0, pipe(test_pipe));

  pid_t listener = ForkListener(test_pipe, -1);
  ASSERT_GE(listener, 1) << "Fork failed " << errno;

  // Let the listener take its baseline before the counters move.
  usleep(V2_SETTLE_US);
  WriteEvents(3, 1, 0, 0);

  uint64_t event_id = 0;
  ASSERT_EQ((ssize_t) sizeof(event_id),
            read(test_pipe[0], &event_id, sizeof(event_id)))
                << "The event has not arrived";
  ASSERT_EQ(1u, event_id);

  // Two counters moved at once and that was a single event. Nothing has
  // moved since, so there is nothing more to read.
  ASSERT_EQ(0, fcntl(test_pipe[0], F_SETFL, O_NONBLOCK));
  usleep(V2_SETTLE_US);
  ASSERT_EQ(-1, read(test_pipe[0], &event_id, sizeof(event_id)))
                << "One wake up has to produce one event only";
  ASSERT_EQ(EAGAIN, errno);

  ExpectCleanExit(listener);
  close(test_pipe[0]);
  close(test_pipe[1]);
}

/*
  An increment of oom_kill means the kernel OOM killer got there before the
  node manager could choose a victim. It is reported on standard error, which
  the java side reads and counts.
*/
TEST_F(OOMListenerV2Test, test_oom_kill_is_reported) {
  WriteEvents(0, 0, 0, 0);

  int test_pipe[2];
  int error_pipe[2];
  ASSERT_EQ(0, pipe(test_pipe));
  ASSERT_EQ(0, pipe(error_pipe));

  pid_t listener = ForkListener(test_pipe, error_pipe[1]);
  ASSERT_GE(listener, 1) << "Fork failed " << errno;
  close(error_pipe[1]);

  usleep(V2_SETTLE_US);
  WriteEvents(1, 0, 1, 2);

  uint64_t event_id = 0;
  ASSERT_EQ((ssize_t) sizeof(event_id),
            read(test_pipe[0], &event_id, sizeof(event_id)))
                << "The event has not arrived";

  char errors[512] = {0};
  ssize_t length = read(error_pipe[0], errors, sizeof(errors) - 1);
  ASSERT_GT(length, 0) << "Nothing was reported on standard error";
  ASSERT_NE((const char *) NULL, strstr(errors, "oom_kill increased by 2"))
                << "Standard error did not name the kernel OOM kills: "
                << errors;

  ExpectCleanExit(listener);
  close(test_pipe[0]);
  close(test_pipe[1]);
  close(error_pipe[0]);
}

#else
/*
This tool covers Linux specific functionality,
so it is not available for other operating systems
*/
int main() {
  return 1;
}
#endif
