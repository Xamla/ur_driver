cmake_minimum_required(VERSION 2.8.3)
project(ur5_driver)

find_package(catkin REQUIRED COMPONENTS
  roscpp
  std_msgs
)

catkin_package(
#  INCLUDE_DIRS include
#  LIBRARIES wurst
#  CATKIN_DEPENDS other_catkin_pkg
#  DEPENDS system_lib
)

#############
## Install ##
#############

## Mark executable scripts (Python etc.) for installation
## in contrast to setup.py, you can choose the destination
#install(PROGRAMS
#   *.lua
#   DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION}
#)

# Copy lua files
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
   DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION}
   FILES_MATCHING PATTERN "*.lua"
)

## Mark executables and/or libraries for installation
# install(TARGETS ur5_driver ur5_driver_node
#   ARCHIVE DESTINATION ${CATKIN_PACKAGE_LIB_DESTINATION}
#   LIBRARY DESTINATION ${CATKIN_PACKAGE_LIB_DESTINATION}
#   RUNTIME DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION}
# )

## Mark other files for installation (e.g. launch and bag files, etc.)
# install(FILES
#   file1
#   DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION}
# )
