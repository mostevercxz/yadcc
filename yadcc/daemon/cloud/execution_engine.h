// Copyright (C) 2020 THL A29 Limited, a Tencent company. All rights reserved.
//
// Licensed under the BSD 3-Clause License (the "License"); you may not use this
// file except in compliance with the License. You may obtain a copy of the
// License at
//
// https://opensource.org/licenses/BSD-3-Clause
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

#ifndef YADCC_DAEMON_CLOUD_EXECUTION_ENGINE_H_
#define YADCC_DAEMON_CLOUD_EXECUTION_ENGINE_H_

#include <sys/types.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "thirdparty/googletest/gtest/gtest_prod.h"
#include "thirdparty/jsoncpp/json.h"

#include "flare/base/buffer.h"
#include "flare/base/expected.h"
#include "flare/base/exposed_var.h"
#include "flare/base/ref_ptr.h"
#include "flare/base/thread/semaphore.h"
#include "flare/fiber/latch.h"

#include "yadcc/daemon/cloud/temporary_file.h"

namespace yadcc::daemon::cloud {

enum class ExecutionStatus {
  Failed,  // Failed to start command.
  Running,
  NotFound  // Expired?
};

enum class NotAcceptingTaskReason {
  Unknown,
  UserInstructed,  // Not used, really.
  PoorMachine,
  CGroupsPresent
};

// Compilation jobs are run inside here.
class ExecutionEngine {
 public:
  struct Input {
    // Unfortunately `stdin` / `stdout` / `stderr` is defined as macro and
    // therefore it's unsafe to use them as variable name.
    TemporaryFile standard_input;  // Read-only.

    // All files below are read automatically by this engine upon command
    // completion and returned to the caller via ` Output`.
    TemporaryFile standard_output, standard_error;  // Write-only.

    // Opaque to the engine. This context is passed back to the caller as-is,
    // via `Output::context`.
    //
    // FIXME: We don't need shared ownership here. The only reason why we use it
    // is because `std::unique_ptr<void>` is not defined (by design). Perhaps we
    // should roll our own move-only type-erased container.
    std::shared_ptr<void> context;
  };

  struct Output {
    int exit_code;

    // See `Input` for where does these bytes come from.
    flare::NoncontiguousBuffer standard_output;
    flare::NoncontiguousBuffer standard_error;

    // From `Input::context`.
    std::shared_ptr<void> context;
  };

  static ExecutionEngine* Instance();

  ExecutionEngine();

  // Get capacity of the execution engine.
  //
  // In case the daemon is not ready for accepting tasks, a reason is returned.
  flare::Expected<std::size_t, NotAcceptingTaskReason> GetMaximumTasks() const;

  // Queue a command to be executed. If there's already too many jobs to
  // execute, a failure is returned.
  std::optional<std::uint64_t> TryQueueCommandForExecution(
      std::uint64_t grant_id, const std::string& command, Input input);

  // Wait for job to complete.
  //
  // `ExecutionStatus::Failed` is returned if we failed to run the command.
  //
  // `ExecutionStatus::Running` is returned if the job is still running after
  // `timeout` has elapsed. In this case the caller should retry this method at
  // a later time.
  //
  // `ExecutionStatus::NotFound` is returned if the task ID is not known to us.
  flare::Expected<Output, ExecutionStatus> WaitForCompletion(
      std::uint64_t task_id, std::chrono::nanoseconds timeout);

  // Forget about the given task. All resources allocated to it is freed.
  void FreeTask(std::uint64_t task_id);

  // Returns a list of grant of running tasks.
  std::vector<std::uint64_t> EnumerateGrantOfRunningTask();

  // Forcibly kill all tasks whose grant ID is not in the given list.
  //
  // For tasks just started within time of `tolerance`, they're exempted. This
  // is necessary to compensate possible network delay.
  void KillExpiredTasks(
      const std::unordered_set<std::uint64_t>& expired_grant_ids);

  // TODO(luobogao): Automatically kill children that have been running for
  // way too long.

  void Stop();
  void Join();

 private:
  FRIEND_TEST(ExecutionEngine, All);
  FRIEND_TEST(ExecutionEngine, Stability);
  FRIEND_TEST(ExecutionEngine, RejectOnMemoryFull);

  // Forcibly kill a running task.
  //
  // Note that this method does NOT remove the task's context. Task's context is
  // either freed by a later call to `FreeTask`, or our cleanup timer.
  void KillTask(std::uint64_t task_id);

  void OnCleanupTimer();
  void OnProcessExitCallback(pid_t pid, int exit_code);  // In Fiber env.
  void ProcessWaiterProc();

  Json::Value DumpTasks();

 private:
  struct TaskDesc : flare::RefCounted<TaskDesc> {
    std::uint64_t grant_id;

    // Set if the task is still running. Cleared by `OnProcessExitCallback`.
    std::atomic<bool> is_running{true};

    // Timestamp this task was started.
    std::chrono::steady_clock::time_point started_at;

    // If the task has completed for a while and no one ever read (and removed)
    // it, we'd like to remove it by our cleanup timer.
    std::chrono::steady_clock::time_point completed_at;

    // Process ID of this task. This is also the process group ID of the
    // process. We start the program in its own process group so that we can
    // kill it and all its
    pid_t process_id;

    // Command being executed.
    std::string command;

    // Input to this task. Automatically cleared on command completion to free
    // resources ASAP.
    std::optional<Input> input;

    // If the task has completed, the result is stored here.
    Output output;

    // Always signaled by `OnProcessExitCallback`, even if the task is killed by
    // `KillTask`.
    flare::fiber::Latch completion_latch{1};
  };

  std::atomic<bool> exiting_{false};

  std::size_t task_concurrency_limit_;
  std::size_t min_memory_for_starting_new_task_;
  NotAcceptingTaskReason not_accepting_task_reason_{
      NotAcceptingTaskReason::Unknown};

  std::uint64_t cleanup_timer_;  // Frees completed tasks that no one cares.

  std::atomic<std::size_t> tasks_run_ever_;  // Number of tasks we've ever run.
  std::atomic<std::size_t> running_tasks_;

  // Task IDs are allocated by it.
  std::atomic<std::uint64_t> next_task_id_{1};
  flare::fiber::Mutex tasks_lock_;
  // Task referenced by this map is either running or waiting to be read by
  // remote daemon.
  std::unordered_map<std::uint64_t, flare::RefPtr<TaskDesc>> tasks_;

  std::thread waitpid_worker_;
  // Released each timer a new subprocess is started.
  flare::CountingSemaphore<> waitpid_semaphore_{0};

  flare::ExposedVarDynamic<Json::Value> exposed_job_internals_;
};

}  // namespace yadcc::daemon::cloud

#endif  // YADCC_DAEMON_CLOUD_EXECUTION_ENGINE_H_
