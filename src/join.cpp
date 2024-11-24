#include "join.hpp"

#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string_view>

namespace join {

void printRows(const Row &leftRow, const Row &rightRow) {
  for (const auto &cell : leftRow) {
    std::cout << cell << ",";
  }
  for (std::size_t i = 0; i < rightRow.size() - 1; ++i) {
    std::cout << rightRow[i] << ",";
  }
  std::cout << rightRow[rightRow.size() - 1] << "\n";
}

void printLeftRow(const Row &leftRow, std::size_t rightRowColumnsCount) {
  printRows(leftRow, Row(rightRowColumnsCount, ""));
}

void printRightRow(const Row &rightRow, std::size_t leftRowColumnsCount) {

  printRows(Row(leftRowColumnsCount, ""), rightRow);
}

Table readFile(std::string_view filename) {
  std::ifstream file(filename);
  Table result;
  std::string line;
  while (std::getline(file, line)) {
    std::stringstream ss(line);
    std::string cell;
    Row row;
    while (std::getline(ss, cell, ',')) {
      row.push_back(cell);
    }
    if (line.empty() || line.back() == ',') {
      row.push_back("");
    }
    result.push_back(row);
  }
  return result;
}

HashedTable readFileToTable(std::string_view filename, std::size_t keyIdx) {
  std::ifstream file(filename);
  HashedTable result;
  std::string line;
  while (std::getline(file, line)) {
    std::stringstream ss(line);
    std::string cell;
    Row row;
    while (std::getline(ss, cell, ',')) {
      row.push_back(cell);
    }
    if (line.empty() || line.back() == ',') {
      row.push_back("");
    }
    result.emplace(std::make_pair(row[keyIdx], std::make_pair(row, false)));
  }
  return result;
}

bool JoinExecutor::predicate(const Row &leftRow, const Row &rightRow) const {
  return leftRow[args_.leftFieldIdx] == rightRow[args_.rigthFieldIdx];
}

NestedLoopExecutor::NestedLoopExecutor(const Args &args) : JoinExecutor(args) {}

void NestedLoopExecutor::execute() {
  // NRVO here, so it won't cause large copying
  auto leftTable = readFile(args_.leftFilename);
  auto rightTable = readFile(args_.rightFilename);

  for (const auto &leftRow : leftTable) {
    bool matched = false;
    for (const auto &rightRow : rightTable) {
      if (predicate(leftRow, rightRow)) {
        matched = true;
        printRows(leftRow, rightRow);
      }
    }
    if (!matched) {
      if (args_.kind == JoinKind::Left || args_.kind == JoinKind::Outer) {
        printLeftRow(leftRow, rightTable[0].size());
      }
    }
  }

  for (const auto &rightRow : rightTable) {
    bool matched = false;
    for (const auto &leftRow : leftTable) {
      if (predicate(leftRow, rightRow)) {
        matched = true;
        break;
      }
    }
    if (!matched) {
      if (args_.kind == JoinKind::Right || args_.kind == JoinKind::Outer) {
        printRightRow(rightRow, leftTable[0].size());
      }
    }
  }
}

HashJoinExecutor::HashJoinExecutor(
    const Args &args, const std::pair<FileMetadata, FileMetadata> &filesMeta)
    : JoinExecutor(args), filesMeta_(filesMeta), hashTable_(leftFileHashed()) {

  if (leftFileHashed()) {
    hashTable_ = readFileToTable(args.leftFilename, args.leftFieldIdx);
    leftTableColumnsCount = hashTable_.begin()->second.first[0].size();
  } else {
    hashTable_ = readFileToTable(args.rightFilename, args.rigthFieldIdx);
    rightTableColumnsCount = hashTable_.begin()->second.first[0].size();
  }
}

bool HashJoinExecutor::leftFileHashed() const {
  return filesMeta_.first.fileSize <= filesMeta_.second.fileSize;
}

bool HashJoinExecutor::rightFileHashed() const { return !leftFileHashed(); }

bool HashJoinExecutor::shouldAddUnmatchedToResults() const {
  return args_.kind == JoinKind::Outer ||
         (args_.kind == JoinKind::Right && leftFileHashed()) ||
         (args_.kind == JoinKind::Left && rightFileHashed());
}

void HashJoinExecutor::execute() {
  auto fileTable = leftFileHashed() ? std::ifstream(args_.rightFilename)
                                    : std::ifstream(args_.leftFilename);

  std::string line;
  while (std::getline(fileTable, line)) {
    std::stringstream ss(line);
    std::string cell;
    Row row;
    while (std::getline(ss, cell, ',')) {
      row.push_back(cell);
    }
    if (line.empty() || line.back() == ',') {
      row.push_back("");
    }

    if (leftFileHashed() && rightTableColumnsCount == 0) {
      rightTableColumnsCount = row.size();
    }
    if (rightFileHashed() && leftTableColumnsCount == 0) {
      leftTableColumnsCount = row.size();
    }

    std::size_t keyIdx =
        leftFileHashed() ? args_.rigthFieldIdx : args_.leftFieldIdx;
    auto matchingRow = hashTable_.find(row[keyIdx]);

    if (matchingRow != hashTable_.end()) {
      const Row &leftRow = leftFileHashed() ? matchingRow->second.first : row;
      const Row &rightRow = rightFileHashed() ? matchingRow->second.first : row;
      matchingRow->second.second = true;
      printRows(leftRow, rightRow);
    } else if (shouldAddUnmatchedToResults()) {
      const Row &leftRow =
          leftFileHashed() ? Row(leftTableColumnsCount, "") : row;
      const Row &rightRow =
          rightFileHashed() ? Row(rightTableColumnsCount, "") : row;
      printRows(leftRow, rightRow);
    }
  }

  // add leftovers
  if (args_.kind != JoinKind::Inner) {
    for (const auto &entry : hashTable_) {
      if (!entry.second.second) { // didn't match row on first step
        const Row &leftRow = leftFileHashed() && args_.kind == JoinKind::Left
                                 ? entry.second.first
                                 : Row(leftTableColumnsCount, "");
        const Row &rightRow = rightFileHashed() && args_.kind == JoinKind::Right
                                  ? entry.second.first
                                  : Row(rightTableColumnsCount, "");
        printRows(leftRow, rightRow);
      }
    }
  }
}

void performJoin(const Args &args) {
  auto leftFileMetadata =
      FileMetadata{.filename = args.leftFilename,
                   .joinFieldIdx = args.leftFieldIdx,
                   .fileSize = std::filesystem::file_size(args.leftFilename)};

  auto rightFileMetadata =
      FileMetadata{.filename = args.rightFilename,
                   .joinFieldIdx = args.rigthFieldIdx,
                   .fileSize = std::filesystem::file_size(args.rightFilename)};

  std::unique_ptr<JoinExecutor> executor;
  JoinAlgo joinAlgorithm = args.algorithm.value_or(
      decideOnExecutor(leftFileMetadata, rightFileMetadata, args));

  switch (joinAlgorithm) {
  case JoinAlgo::Nested:
    executor = std::make_unique<NestedLoopExecutor>(args);
    break;
  case JoinAlgo::Hash:
    executor = std::make_unique<HashJoinExecutor>(
        args, std::make_pair(leftFileMetadata, rightFileMetadata));
    break;
  case JoinAlgo::SortMerge:
    throw std::runtime_error("unimplemented");
  }
  executor->execute();
}

JoinAlgo decideOnExecutor(const FileMetadata &first, const FileMetadata &second,
                          const Args &args) {

  return JoinAlgo::Nested;
}
} // namespace join