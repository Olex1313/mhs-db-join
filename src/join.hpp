#pragma once

#include <fstream>
#include <map>
#include <string>
#include <vector>

namespace join {

const uintmax_t kKilobyte = 1024;
const uintmax_t kMegabyte = kKilobyte * 1024;
const uintmax_t kGigabyte = kMegabyte * 1024;
const uintmax_t kMaxRuntimeMemory = kGigabyte * 4ull;

using Row = std::vector<std::string>;
using Table = std::vector<Row>;
// pair in value acts as a marker that row matched during join
// FIXME there may be many rows for same key
using HashedTable =
    std::unordered_map<std::string, std::pair<std::vector<Row>, bool>>;

using RowComparator = std::function<bool(const Row &, const Row &)>;

enum class JoinKind { Left = 0, Right, Inner, Outer };

static const std::unordered_map<std::string, JoinKind> kindMap = {
    {"left", JoinKind::Left},
    {"right", JoinKind::Right},
    {"inner", JoinKind::Inner},
    {"outer", JoinKind::Outer},
};

enum class JoinAlgo { Nested = 0, Hash, SortMerge };

static const std::unordered_map<std::string, JoinAlgo> algoMap = {
    {"nested", JoinAlgo::Nested},
    {"hash", JoinAlgo::Hash},
    {"sort-merge", JoinAlgo::SortMerge},
};

struct Args {
  std::string_view leftFilename;
  std::size_t leftFieldIdx;

  std::string_view rightFilename;
  std::size_t rightFieldIdx;

  JoinKind kind;
  std::optional<JoinAlgo> algorithm;
};

struct FileMetadata {
  std::string_view filename;
  std::size_t joinFieldIdx;
  std::uintmax_t fileSize;
};

class JoinExecutor {
public:
  inline JoinExecutor(const Args &args) : args_(args) {}

  virtual void execute() = 0;

  virtual ~JoinExecutor() {}

protected:
  int predicate(const Row &leftRow, const Row &rightRow) const;

protected:
  const Args args_;
  bool matched_;
  Row currentRow_;
};

class NestedLoopExecutor : public JoinExecutor {
public:
  NestedLoopExecutor(const Args &args);

  void execute() override;

  ~NestedLoopExecutor() = default;
};

class SortMergeExecutor : public JoinExecutor {
public:
  SortMergeExecutor(const Args &args,
                    const std::pair<FileMetadata, FileMetadata> &filesMeta);

  void execute() override;

  ~SortMergeExecutor();

private:
  const std::pair<FileMetadata, FileMetadata> filesMeta_;
  std::string sortedLeftFilename;
  std::string sortedRightFilename;
};

class HashJoinExecutor : public JoinExecutor {
public:
  HashJoinExecutor(const Args &args,
                   const std::pair<FileMetadata, FileMetadata> &filesMeta);

  void execute() override;

private:
  bool leftFileHashed() const;

  bool rightFileHashed() const;

  bool shouldAddUnmatchedToResults() const;

  bool shouldCheckUnmatched() const;

private:
  std::size_t leftTableColumnsCount;
  std::size_t rightTableColumnsCount;
  const std::pair<FileMetadata, FileMetadata> filesMeta_;
  HashedTable hashTable_;
};

JoinAlgo decideOnExecutor(const FileMetadata &first, const FileMetadata &second,
                          const Args &args);

void performJoin(const Args &args);

} // namespace join