#include "join.hpp"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <functional>
#include <ios>
#include <iostream>
#include <memory>
#include <queue>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>

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

Row parseCSVRow(const std::string &line) {
  Row row;
  std::stringstream ss(line);
  std::string cell;

  while (std::getline(ss, cell, ',')) {
    row.push_back(cell);
  }

  if (!line.empty() && line.back() == ',') {
    row.push_back("");
  }

  return row;
}

void writeCSVRow(std::ofstream &file, const Row &row) {
  for (size_t i = 0; i < row.size() - 1; ++i) {
    file << row[i] << ",";
  }

  file << row[row.size() - 1] << "\n";
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
    auto rowsWithKey = result.find(row[keyIdx]);

    if (rowsWithKey == result.end()) {
      result.emplace(std::make_pair(
          row[keyIdx], std::make_pair(std::vector<Row>{row}, false)));
    } else {
      rowsWithKey->second.first.push_back(row);
    }
  }
  return result;
}

// Splits a CSV file into sorted chunks
std::vector<std::string> splitAndSortChunks(std::string_view filename,
                                            std::size_t chunkSize,
                                            RowComparator comp) {
  std::ifstream file(filename);
  std::string line;
  std::vector<std::string> tempFiles;
  std::size_t chunkIndex = 0;

  while (file) {
    Table chunk;
    std::size_t currentSize = 0;

    while (currentSize < chunkSize && std::getline(file, line)) {
      chunk.push_back(parseCSVRow(line));
      currentSize++;
    }

    if (!chunk.empty()) {
      std::sort(chunk.begin(), chunk.end(), comp);
      std::string tempFile = "chunk_" + std::to_string(chunkIndex++) + ".csv";
      std::ofstream outFile(tempFile);
      for (const auto &row : chunk) {
        writeCSVRow(outFile, row);
      }
      outFile.close();
      tempFiles.push_back(tempFile);
    }
  }

  file.close(); // obviously RAII does not require that, but, nice to have
  return tempFiles;
}

void mergeSortedChunks(const std::vector<std::string> &chunkFiles,
                       std::string_view outputFile, RowComparator comp) {
  struct Entry {
    Row row;
    std::size_t fileIndex;
  };

  auto compareEntries = [&comp](const Entry &a, const Entry &b) {
    return comp(b.row, a.row);
  };

  std::priority_queue<Entry, std::vector<Entry>, decltype(compareEntries)>
      minHeap(compareEntries);

  { // NOTE separate scope for auto-closing openFiles ifstream objects
    std::vector<std::ifstream> openFiles(chunkFiles.size());

    for (std::size_t i = 0; i < chunkFiles.size(); ++i) {
      openFiles[i].open(chunkFiles[i]);
      std::string line;
      if (std::getline(openFiles[i], line)) {
        minHeap.push({parseCSVRow(line), i});
      }
    }

    std::ofstream outFile(outputFile);
    while (!minHeap.empty()) {
      Entry smallest = minHeap.top();
      minHeap.pop();
      writeCSVRow(outFile, smallest.row);

      std::string line;
      if (std::getline(openFiles[smallest.fileIndex], line)) {
        minHeap.push({parseCSVRow(line), smallest.fileIndex});
      }
    }
  }

  for (const auto &tempFile : chunkFiles) {
    std::filesystem::remove(tempFile);
  }
}

void externalMemoryMergeSort(std::string_view inputFile,
                             std::string_view outputFile, std::size_t chunkSize,
                             RowComparator comp) {
  auto chunkFiles = splitAndSortChunks(inputFile, chunkSize, comp);
  mergeSortedChunks(chunkFiles, outputFile, comp);
}

int JoinExecutor::predicate(const Row &leftRow, const Row &rightRow) const {
  if (args_.leftFieldIdx >= leftRow.size() ||
      args_.rightFieldIdx >= rightRow.size()) {
    return -1;
  }
  if (leftRow[args_.leftFieldIdx] < rightRow[args_.rightFieldIdx]) {
    return -1;
  }
  if (leftRow[args_.leftFieldIdx] > rightRow[args_.rightFieldIdx]) {
    return 1;
  }
  return 0;
}

NestedLoopExecutor::NestedLoopExecutor(const Args &args) : JoinExecutor(args) {}

void NestedLoopExecutor::execute() {
  // NRVO here, so it won't cause large copying
  auto leftTable = readFile(args_.leftFilename);
  auto rightTable = readFile(args_.rightFilename);

  for (const auto &leftRow : leftTable) {
    bool matched = false;
    for (const auto &rightRow : rightTable) {
      if (predicate(leftRow, rightRow) == 0) {
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
      if (predicate(leftRow, rightRow) == 0) {
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
    : JoinExecutor(args), leftTableColumnsCount(0), rightTableColumnsCount(0),
      filesMeta_(filesMeta), hashTable_(leftFileHashed()) {

  if (leftFileHashed()) {
    hashTable_ = readFileToTable(args.leftFilename, args.leftFieldIdx);
    leftTableColumnsCount = hashTable_.begin()->second.first.cbegin()->size();
  } else {
    hashTable_ = readFileToTable(args.rightFilename, args.rightFieldIdx);
    rightTableColumnsCount = hashTable_.begin()->second.first.cbegin()->size();
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

bool HashJoinExecutor::shouldCheckUnmatched() const {
  return args_.kind == JoinKind::Outer ||
         (args_.kind == JoinKind::Left && leftFileHashed()) ||
         (args_.kind == JoinKind::Right && rightFileHashed());
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
        leftFileHashed() ? args_.rightFieldIdx : args_.leftFieldIdx;
    auto rowsWithKey = hashTable_.find(row[keyIdx]);

    if (rowsWithKey != hashTable_.end()) {
      for (const auto &matchedRow : rowsWithKey->second.first) {
        const Row &leftRow = leftFileHashed() ? matchedRow : row;
        const Row &rightRow = rightFileHashed() ? matchedRow : row;
        rowsWithKey->second.second = true;
        printRows(leftRow, rightRow);
      }
    } else if (shouldAddUnmatchedToResults()) {
      const Row &leftRow =
          leftFileHashed() ? Row(leftTableColumnsCount, "") : row;
      const Row &rightRow =
          rightFileHashed() ? Row(rightTableColumnsCount, "") : row;
      printRows(leftRow, rightRow);
    }
  }

  // add leftovers
  if (shouldCheckUnmatched()) {
    for (const auto &entry : hashTable_) {
      bool rowsAreUnmatched = !entry.second.second;
      if (rowsAreUnmatched) {
        for (const auto &unmatchedRow : entry.second.first) {
          const Row &leftRow =
              leftFileHashed() ? unmatchedRow : Row(leftTableColumnsCount, "");
          const Row &rightRow = rightFileHashed()
                                    ? unmatchedRow
                                    : Row(rightTableColumnsCount, "");
          printRows(leftRow, rightRow);
        }
      }
    }
  }
}

SortMergeExecutor::SortMergeExecutor(
    const Args &args, const std::pair<FileMetadata, FileMetadata> &filesMeta)
    : JoinExecutor(args), filesMeta_(filesMeta),
      sortedLeftFilename(std::string(args.leftFilename) + ".sorted"),
      sortedRightFilename(std::string(args.rightFilename) + ".sorted") {

  externalMemoryMergeSort(args.leftFilename, sortedLeftFilename, 10ull,
                          [&](const Row &left, const Row &right) {
                            return left[args_.leftFieldIdx] <
                                   right[args_.leftFieldIdx];
                          });
  externalMemoryMergeSort(args.rightFilename, sortedRightFilename, 10ull,
                          [&](const Row &left, const Row &right) {
                            return left[args_.rightFieldIdx] <
                                   right[args_.rightFieldIdx];
                          });
}

void SortMergeExecutor::execute() {
  std::ifstream leftFile(args_.leftFilename);
  std::ifstream rightFile(args_.rightFilename);

  std::string leftLine, rightLine;
  Row leftRow, rightRow;
  std::size_t leftColumnsCount = 0, rightColumnsCount = 0;

  bool hasLeft = static_cast<bool>(std::getline(leftFile, leftLine));
  leftRow = parseCSVRow(leftLine);
  leftColumnsCount = leftRow.size();

  bool hasRight = static_cast<bool>(std::getline(rightFile, rightLine));
  rightRow = parseCSVRow(rightLine);
  rightColumnsCount = rightRow.size();

  std::streampos rightGroupMark;

  while (hasLeft || hasRight) {
    std::string leftKey = hasLeft ? leftRow[args_.leftFieldIdx] : "";
    std::string rightKey = hasRight ? rightRow[args_.rightFieldIdx] : "";

    if (!hasRight || (hasLeft && leftKey < rightKey)) {
      if (args_.kind == JoinKind::Left || args_.kind == JoinKind::Outer) {
        printLeftRow(leftRow, rightColumnsCount);
      }

      hasLeft = static_cast<bool>(std::getline(leftFile, leftLine));
      if (hasLeft) {
        leftRow = parseCSVRow(leftLine);
      }
    } else if (!hasLeft || (hasRight && leftKey > rightKey)) {
      if (args_.kind == JoinKind::Right || args_.kind == JoinKind::Outer) {
        printRightRow(rightRow, leftColumnsCount);
      }

      hasRight = static_cast<bool>(std::getline(rightFile, rightLine));
      if (hasRight) {
        rightRow = parseCSVRow(rightLine);
      }
    } else {
      rightGroupMark = rightFile.tellg();
      std::string currentKey = leftKey;

      while (hasLeft && leftRow[args_.leftFieldIdx] == currentKey) {
        do {
          printRows(leftRow, rightRow);

          hasRight = static_cast<bool>(std::getline(rightFile, rightLine));
          if (hasRight) {
            rightRow = parseCSVRow(rightLine);
          }
        } while (hasRight && rightRow[args_.rightFieldIdx] == currentKey);

        rightFile.clear();
        rightFile.seekg(rightGroupMark);
        hasRight = static_cast<bool>(std::getline(rightFile, rightLine));
        if (hasRight) {
          rightRow = parseCSVRow(rightLine);
        }

        hasLeft = static_cast<bool>(std::getline(leftFile, leftLine));
        if (hasLeft) {
          leftRow = parseCSVRow(leftLine);
        }
      }

      while (hasRight && rightRow[args_.rightFieldIdx] == currentKey) {
        hasRight = static_cast<bool>(std::getline(rightFile, rightLine));
        if (hasRight) {
          rightRow = parseCSVRow(rightLine);
        }
      }
      while (hasLeft && leftRow[args_.leftFieldIdx] == currentKey) {
        hasLeft = static_cast<bool>(std::getline(leftFile, leftLine));
        if (hasLeft) {
          leftRow = parseCSVRow(leftLine);
        }
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
                   .joinFieldIdx = args.rightFieldIdx,
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
    executor = std::make_unique<SortMergeExecutor>(
        args, std::make_pair(leftFileMetadata, rightFileMetadata));
  }
  executor->execute();
}

JoinAlgo decideOnExecutor(const FileMetadata &first, const FileMetadata &second,
                          const Args &args) {

  return JoinAlgo::Nested;
}
} // namespace join