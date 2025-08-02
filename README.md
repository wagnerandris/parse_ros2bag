# UNTESTED DEVELOPMENT FORK!
## TODO:
- fix misc topics
	- Right now the misc topics are not exported, so the .kml conversion also fails.
- bag.zip's content is really weird, contains the full absolute path to the bagfile. Fix this!
- pointcloud.zip should contain only the pointclouds, no subfolder, also rename to pointclouds.zip
## Notes:
- While running it throws a LOT of errors and warnings, but works :D
# parse_ros2bag
A python script to parse a ROS2 bag's content into separate image, video, pointcloud and csv files.

# Prerequisites/Dependencies
- The video generation needs FFMPEG to be installed.
	- You probabaly already have it installed, but if not, the install it by running: `sudo apt install ffmpeg`
- You must have ROS2 humble installed and configured on your system, with the default underlay sourced.
	- Either run `source /opt/ros/humble/setup.bash` or add the same to the end of your `.bashrc` file.
- ROS2's ament and cmake system must be installed first, system-wide.
	-run `sudo apt install ros-humble-ament-cmake*`
- The package uses ros2-humble with packages included as submodules under `src` and other submodules in the main directory, as well as python package dependencies listed in `requirements.txt`.

# Installation

## Step-by-step installation instructions
1. Clone this repo, with the submodules `git clone --recurse-submodules`
2. `cd parse_ros2bag`
3. Source your main ROS2 underlay with `source /opt/ros/humble/setup.bash`
	- _this step is rendundant, but whatewer_
4. Build the required and included ROS2 packages with colcon. Because colcon tries to build the packages in alphabetical order, ignoring possible conflicts, you need to manually build some packages.
	- Before any other package, build just rosbag2\_tools. Run `colcon build --symlink-install --packages-select rosbag2_tools`
	- Run `colcon build --symlink-install`
	- ~~_colcon build will probably fail on the package_ `test_msgs` _which is included in_ `rcl_interfaces`_. You can skip this package, as long as all the others are built._~~
	- `rcl_interfaces` _was pinned to version 1.2.1, but since it was removed from the submodules_
5. Create a python virtual environment to handle pip dependencies (optional, but highly recommended)
	- `python -m venv venv`
6. Source the built ROS2 packages and python virtual environment by using the provided source script. Run `source source_deps.bash`
7. Use pip to install the dependencies
	- run `pip install -r requirements.txt`
	- or run `pip install -r req_nover.txt` <- this is a versionless, stripped-down requirement list.
8. **ALL DONE** Use the parser with the options provided in: `python parse_ros2bags.py -h`

# Usage
If the python venv is created and ros packages are built in the package's root, the ros workspace and venv can be activated with `source source_deps.bash`.

With the workspace and environment activated the script can be run with possible options described by `python parse_ros2bag -h`.
