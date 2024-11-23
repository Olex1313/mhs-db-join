#include "join.hpp"

#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string_view>

namespace join {

void printRows(const std::vector<std::string> &leftRow,
               const std::vector<std::string> &rightRow) {
  for (const auto &cell : leftRow) {
    std::cout << cell << ",";
  }
  for (std::size_t i = 0; i < rightRow.size() - 1; ++i) {
    std::cout << rightRow[i] << ",";
  }
  std::cout << rightRow[rightRow.size() - 1] << "\n";
}

void printLeftRow(const std::vector<std::string> &leftRow,
                  std::size_t rightRowColumnsCount) {
  printRows(leftRow, std::vector<std::string>(rightRowColumnsCount, ""));
}

void printRightRow(const std::vector<std::string> &rightRow,
                   std::size_t leftRowColumnsCount) {

  printRows(std::vector<std::string>(leftRowColumnsCount, ""), rightRow);
}

std::vector<std::vector<std::string>> readFile(std::string_view filename) {
  std::ifstream file(filename);
  std::vector<std::vector<std::string>> result;
  std::string line;
  while (std::getline(file, line)) {
    std::stringstream ss(line);
    std::string cell;
    std::vector<std::string> row;
    while (std::getline(ss, cell, ',')) {
      row.push_back(cell);
    }
    result.push_back(row);
  }
  return result;
}

bool JoinExecutor::predicate(const std::vector<std::string> &leftRow,
                             const std::vector<std::string> &rightRow) const {
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

void performJoin(const Args &args) {
  auto leftFileMetadata =
      FileMetadata{.filename = args.leftFilename,
                   .joinFieldIdx = args.leftFieldIdx,
                   .fileSize = std::filesystem::file_size(args.leftFilename)};

  auto rightFileMetadata =
      FileMetadata{.filename = args.rightFilename,
                   .joinFieldIdx = args.rigthFieldIdx,
                   .fileSize = std::filesystem::file_size(args.rightFilename)};

  auto executor = decideOnExecutor(leftFileMetadata, rightFileMetadata, args);
  executor->execute();
}

std::unique_ptr<JoinExecutor> decideOnExecutor(const FileMetadata &first,
                                               const FileMetadata &second,
                                               const Args &args) {
  return std::make_unique<NestedLoopExecutor>(args);
}
} // namespace join