cmake_minimum_required(VERSION 2.8.3)
project(ur_driver)

find_package(catkin REQUIRED COMPONENTS
  roscpp
  std_msgs
)

catkin_package()

# Copy lua files
install(DIRECTORY ./
   DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION}
   FILES_MATCHING PATTERN "*.lua"
   PATTERN "launch" EXCLUDE
)
