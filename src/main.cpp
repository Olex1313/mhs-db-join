
#include <exception>
#include <iostream>

#include "join.hpp"

int main(int argc, char *argv[]) {
  try {
    if (argc != 6) {
      throw std::invalid_argument("Invalid number of arguments: " +
                                  std::to_string(argc));
    }

    auto joinArgs = join::Args{
        .leftFilename = argv[1],
        .leftFieldIdx = std::stoul(argv[2]) - 1,
        .rightFilename = argv[3],
        .rigthFieldIdx = std::stoul(argv[4]) - 1,
        .kind = join::kindMap.at(argv[5]),
    };

    join::performJoin(joinArgs);

  } catch (std::exception &e) {
    std::cout << e.what();
  }
  return 0;
}