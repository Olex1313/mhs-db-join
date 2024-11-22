#pragma once

#include <fstream>
#include <map>
#include <string>

namespace join {

const uintmax_t kMaxRuntimeMemory = 1024 * 1024 * 1024 * 4ull;

enum class JoinKind { Left = 0, Right, Inner, Outer };

static const std::unordered_map<std::string, JoinKind> kindMap = {
    {"left", JoinKind::Left},
    {"right", JoinKind::Right},
    {"inner", JoinKind::Inner},
    {"outer", JoinKind::Outer},
};

struct Args {
  std::string_view leftFilename;
  std::size_t leftFieldIdx;

  std::string_view rightFilename;
  std::size_t rigthFieldIdx;

  JoinKind kind;
};

struct FileMetadata {
  std::string_view filename;
  std::size_t joinFieldIdx;
  std::uintmax_t fileSize;
  std::uintmax_t rowsCount; // is it needed?
};

class JoinExecutor {
public:
  inline JoinExecutor(const Args &args) : args_(args) {}

  virtual void execute() = 0;

  virtual ~JoinExecutor() {}

protected:
  void passRow(std::vector<std::string> &&row);

  void matchRow(const std::vector<std::string> &row);

  void flushMatches();

private:
  bool predicate(const std::vector<std::string> &row) const;

protected:
  const Args args_;
  bool matched_;
  std::vector<std::string> currentRow_;
};

class NestedLoopExecutor : public JoinExecutor {
public:
  NestedLoopExecutor(const Args &args);

  void execute() override;

  ~NestedLoopExecutor() = default;

private:
  std::ifstream leftFile_;
};

class SortMergeExecutor : public JoinExecutor {
public:
  SortMergeExecutor(const Args &args);

  void execute() override;
};

class HashJoinExecutor : public JoinExecutor {
public:
  HashJoinExecutor(const Args &args, const FileMetadata &filesMeta);

  void execute() override;

private:
  std::ifstream leftFile_;
  std::ifstream rightFile_;
  std::map<std::string, std::string> hashTable_;
};

std::unique_ptr<JoinExecutor> decideOnExecutor(const FileMetadata &first,
                                               const FileMetadata &second,
                                               const Args &args);

void performJoin(const Args &args);

} // namespace join