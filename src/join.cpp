#include "join.hpp"

#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>

namespace join {

void JoinExecutor::passRow(std::vector<std::string> &&row) {
  currentRow_ = std::move(row);
}

bool JoinExecutor::predicate(const std::vector<std::string> &row) const {
  return currentRow_[args_.leftFieldIdx] == row[args_.rigthFieldIdx];
}

void JoinExecutor::matchRow(const std::vector<std::string> &row) {
  bool joinPredicate = predicate(row);
  matchedRows_.push_back(std::move(row));
}

void JoinExecutor::joinRows(const std::vector<std::string> &leftLine,
                            const std::vector<std::string> &rightLine) const {
  if (leftLine[args_.leftFieldIdx] == rightLine[args_.rigthFieldIdx]) {
    if (args_.kind == JoinKind::Outer) {
      return;
    }

    for (std::size_t i = 0; i < leftLine.size(); ++i) {
      std::cout << leftLine[i] << ",";
    }
    for (std::size_t i = 0; i < rightLine.size() - 1; ++i) {
      std::cout << rightLine[i] << ",";
    }
    std::cout << rightLine[rightLine.size() - 1] << "\n";
  }

  // rows not equal

  if (args_.kind == JoinKind::Inner) {
    return;
  }

  if (args_.kind == JoinKind::Left) {
    std::vector<std::string> result(leftLine);
    for (std::size_t i = 0; i < leftLine.size(); ++i) {
      std::cout << leftLine[i] << ",";
    }
    for (std::size_t i = 0; i < rightLine.size() - 1; ++i) {
      std::cout << ",";
    }
    std::cout << "\n";
    return;
  }

  if (args_.kind == JoinKind::Right) {
    std::vector<std::string> result;
    result.reserve(leftLine.size() + rightLine.size());
    for (std::size_t i = 0; i < leftLine.size(); ++i) {
      result.push_back("");
    }
    result.insert(result.end(), rightLine.begin(), rightLine.end());
    return result;
  }

  if (args_.kind == JoinKind::Outer) {
    std::vector<std::string> result;
    result.reserve(leftLine.size() + rightLine.size());
    result.insert(result.end(), leftLine.begin(), leftLine.end());
    result.insert(result.end(), rightLine.begin(), rightLine.end());
    return result;
  }

  throw std::runtime_error("Unknown join kind");
}

NestedLoopExecutor::NestedLoopExecutor(const Args &args)
    : JoinExecutor(args), leftFile_(std::ifstream(args.leftFilename)) {}

// scan first file
// for each line, scan second file
// check join condition and print if match
void NestedLoopExecutor::execute() {
  std::string leftLine;
  while (std::getline(leftFile_, leftLine)) {
    std::stringstream leftRowStream(leftLine);
    std::string leftRowValue;
    std::vector<std::string> leftRowValues;
    while (std::getline(leftRowStream, leftRowValue, ',')) {
      leftRowValues.push_back(leftRowValue);
    }

    std::ifstream rightFile_(args_.rightFilename);

    std::string rightLine;
    while (std::getline(rightFile_, rightLine)) {
      std::stringstream rightRowStream(rightLine);
      std::string rightRowValue;
      std::vector<std::string> rightRowValues;
      while (std::getline(rightRowStream, rightRowValue, ',')) {
        rightRowValues.push_back(rightRowValue);
      }

      auto joinedRow = joinRows(leftRowValues, rightRowValues);
      if (joinedRow.empty()) {
        break;
      }
      printRow(joinedRow);
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