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

#include <chrono>
#include <cstdio>
#include <string_view>
#include <thread>

#include "fmt/format.h"

#include "yadcc/client/common/command.h"
#include "yadcc/client/common/env_options.h"
#include "yadcc/client/common/io.h"
#include "yadcc/client/common/logging.h"
#include "yadcc/client/common/task_quota.h"
#include "yadcc/client/common/utility.h"
#include "yadcc/client/cxx/compilation_saas.h"
#include "yadcc/client/cxx/compiler_args.h"
#include "yadcc/client/cxx/rewrite_file.h"

using namespace std::literals;

namespace yadcc::client {

// Tests if the argument can be distributed to remote servers.
bool IsCompilerInvocationDistributable(const CompilerArgs& args) {
  if (!args.TryGet("-c")) {  // Not called for compilation.
    LOG_TRACE("Not called for compilation, bailing out.");
    return false;
  }
  if (auto opt = args.TryGet("-x");
      opt && (opt->front() != "c"sv && opt->front() != "c++"sv)) {
    LOG_TRACE("Not called for compiling C/C++ code, bailing out.");
    return false;
  }
  if (args.TryGet("-")) {
    LOG_TRACE("Reading source for stdin is not supported yet. Bailing out.");
    return false;
  }
  if (args.GetFilenames().size() != 1) {
    LOG_TRACE(
        "Multiple filename are present in command line. Confused. Bailing "
        "out.");
    return false;
  }
  if (EndsWith(args.GetFilenames()[0], ".s") ||
      EndsWith(args.GetFilenames()[0], ".S")) {
    LOG_TRACE("Called for assembling, bailing out.");
    return false;
  }
  // TODO(luobogao): Handling environment-dependent arguments, e.g.,
  // `-march=native`.
  return true;
}

// Test (using our heuristics) if `args` is a lightweight task.
bool IsLightweightTask(const CompilerArgs& args) {
  static constexpr std::string_view kLightweightArgs[] = {"-dumpversion",
                                                          "-dumpmachine", "-E"};

  for (auto&& e : kLightweightArgs) {
    if (args.TryGet(e)) {
      return true;
    }
  }
  if (GetOptionTreatSourceFromStdinAsLightweight()) {
    return args.TryGet("-");
  }
  return false;
}

// Run the compilation natively, using the pre-acquired task quota.
//
// If anything else does not work out as intended, this is our last resort.
int RunCompilationNativelyUsingQuota(
    const std::string& program, const char** argv,
    [[maybe_unused]] std::shared_ptr<void> quota) {
  return PassthroughToProgram(program.c_str(), argv);
}

void WriteCompilationResults(
    const std::string& destination,
    const std::vector<std::pair<std::string, std::string>>& output_files) {
  std::string destination_prefix = destination;
  // TODO(jacksonyhe): Possibly we should deal with the case that extention name
  // format looks like '.xxx' other than '.o'.
  if (destination_prefix.size() >= 2 &&
      destination_prefix.substr(destination_prefix.size() - 2) == ".o") {
    destination_prefix =
        destination_prefix.substr(0, destination_prefix.size() - 2);
  }
  for (auto&& [suffix, file] : output_files) {
    LOG_TRACE("Got file with extension [{}]: [{}] bytes.", suffix, file.size());
    if (suffix == ".o") {
      WriteAll(destination, file);
    } else {
      std::string full_name(destination_prefix);
      full_name.append(suffix);
      WriteAll(full_name, file);
    }
  }
}

void PrintLoad() {
  std::string req_body = "";
  auto&& [status, body] = DaemonCall(
      "http://username:password@10.10.111.47:8336/inspect/vars/yadcc", {},
      req_body, 15s /* Must be greater than `milliseconds_to_wait` */);
  if (status != 200) {
    return;
  }

  Json::Value jsv;
  if (!Json::Reader().parse(body, jsv)) {
    return;
  }

  std::string output = "集群总负载:\n";
  auto&& task_dispatcher = jsv["task_dispatcher"];
  output += task_dispatcher["capacity_available"].asString();
  output += "/";
  output += task_dispatcher["capacity"].asString();
  output += "\n";

  // Print load info
  output += "各机器负载:\n";
  for (int i = 0; i != task_dispatcher["servants"].size(); ++i) {
    auto&& one_servant = task_dispatcher["servants"][i];
    auto splitted = flare::Split(one_servant["location"].asString(), ":");
    output += splitted[0];
    output += ",";
    output += "负载/最大贡献核数:";
    output += one_servant["current_load"].asString();
    output += "/";
    output += one_servant["max_tasks"].asString();
    output += ",\n";
  }

  LOG_ERROR("{}", output);
}

// TODO(luobogao): If a sufficiently small number of files to be compiled, we
// can just run them locally (if they miss the cache). Compiling files locally
// (if possible) leads to more consistent time usage.
int Entry(int argc, const char** argv) {
  auto not_using_symlink = EndsWith(argv[0], "yadcc-cxx");
  auto bias = not_using_symlink ? 2 : 1;
  CompilerArgs args(argc - bias, argv + bias);

  if (not_using_symlink && argv[1][0] == '/') {
    // Respect user's choice if absolute path is specified.
    args.SetCompiler(argv[1]);
  } else {
    // Otherwise we need to determine absolute path of the compiler ourselves.
    args.SetCompiler(FindExecutableInPath(
        GetBaseName(argv[bias - 1]), [](auto&& canonical_path) {
          // We need the *real* compiler, not some wrapper. Otherwise the
          // compiler's digest will be wrong, and no matching compiler will be
          // found in the cluster.
          //
          // Besides, I don't expect them to be beneficial (if not harmful) to
          // us.
          return !EndsWith(canonical_path, "ccache") &&
                 !EndsWith(canonical_path, "distcc") &&
                 !EndsWith(canonical_path, "icecc");
        }));
  }
  LOG_TRACE("Using compiler: {}", args.GetCompiler());

  // Run compilation locally, possibly with a pre-acquired task quota.
  auto passthrough = [&](auto&& quota) {
    return RunCompilationNativelyUsingQuota(args.GetCompiler(), argv + bias,
                                            quota);
  };
  auto passthrough_acquiring_quota = [&] {
    return passthrough(AcquireTaskQuota(IsLightweightTask(args)));
  };

  // Bail out quickly if the task can't be done on the cloud.
  if (!IsCompilerInvocationDistributable(args)) {
    if (GetOptionWarnOnNonDistributable()) {
      LOG_ERROR("该编译选项不能进行联合编译，本地编译: {}", args.Rebuild());
    }
    return passthrough_acquiring_quota();
  }

  // Preprocess the source file then.
  auto rewritten = RewriteFile(args);
  if (!rewritten) {
    LOG_ERROR("预处理cpp文件失败，在本机排队等CPU编译{}", args.Rebuild());
    return passthrough_acquiring_quota();
  }

  // User allowed compilation cache but the source file disagrees. Print a
  // warning if requested.
  if ((rewritten->cache_control == CacheControl::Disallow &&
       GetOptionCacheControl() != CacheControl::Disallow) &&
      GetOptionWarnOnNoncacheble()) {
    LOG_WARN("Found non-cacheable translation unit. Invoked with: {}",
             args.Rebuild());
  }

  if (rewritten->zstd_rewritten.size() <
      GetOptionCompileOnCloudSizeThreshold()) {
    LOG_TRACE(
        "Preprocessed file is so small that compiling it locally is likely to "
        "be faster.");
    return passthrough_acquiring_quota();
  }

  // Now submit the task to the cloud.
  //
  // Due to network instability, compilation on cloud fails frequently. Here we
  // mitigate this by retring for some time before failing back to local
  // compilation.
  int retries_left = 5;
  while (true) {
    auto&& [ec, output, err, output_files] =
        // Preprocessed source is **moved** to `CompileOnCloud` so that it can
        // be freed there as soon as it's sent to our delegate daemon. This is
        // necessary to reduce our memory footprint. The preprocessed source
        // code is likely to be large (several megabytes, even after
        // compression.)
        CompileOnCloud(args, std::move(*rewritten));

    // Most likely an error related to our compilation cloud, instead of a
    // "real" compilation error.
    if (ec < 0 || ec == 127 /* Failed to start compiler at remote side. */) {
      if (auto quota = TryAcquireTaskQuota(false, 20s)) {
        LOG_ERROR(
            "20s内,向 localhost:8334 要不到用来编译的CPU核心,本地编译,ret={}",
            ec);
        // Local machine is free, failback to local compilation.
        PrintLoad();
        return passthrough(quota);
      }

      // Local machine is busy then, if we haven't run out of retry budget yet,
      // try submitting this task to the compilation cloud again.
      if (retries_left--) {
        LOG_TRACE("{} 拿不到核用来编译,继续等待中...剩余重试次数:{}", ec,
                  retries_left);
        PrintLoad();

        // `rewritten` was `move`d away when we call `CompileOnCloud`,
        // regenerate it.
        rewritten = RewriteFile(args);
        if (rewritten) {
          // Retry then.
          continue;
        }  // On-disk file changed? Rewrite failed this time. Fall-through then.
      }
    }
    if (ec != 0) {
      LOG_DEBUG("Failed on the cloud with (stdout): {}", output);
      LOG_DEBUG("Failed on the cloud with (stderr): {}", err);
      if (ec == 1) {  // Most likely an error raised by GCC (source code itself
                      // is broken?). We don't print an error in this case.
        LOG_TRACE("云端编译失败,错误={}, 本地重试下面命令: {}", ec,
                  args.Rebuild());
      } else {
        LOG_WARN("云端编译错误码={}. 本地编译", ec);
      }
      return passthrough_acquiring_quota();
    }

    // Compiled successfully on the cloud then.
    LOG_TRACE("Got [{}] files from cloud.", output_files.size());
    fprintf(stdout, "%s", output.c_str());
    fprintf(stderr, "%s", err.c_str());
    WriteCompilationResults(args.GetOutputFile(), output_files);
    break;
  }

  return 0;
}

}  // namespace yadcc::client

int main(int argc, const char** argv) {
  // Keep diagnostics language consistent across the compilation cluster.
  setenv("LC_ALL", "en_US.utf8", true);

  // Initialize logging first.
  yadcc::client::min_log_level = yadcc::client::GetOptionLogLevel();

  // Why do you call us then?
  if (argc == 1) {
    LOG_INFO("No compilation is requested. Leaving.");
    return 0;
  }

  // Let's rock.
  LOG_TRACE("Started");
  auto rc = yadcc::client::Entry(argc, argv);
  LOG_TRACE("Exited.");
  return rc;
}
