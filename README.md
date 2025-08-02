# parse_ros2bag
A python script to parse a ROS2 bag's content into separate image, video, pointcloud and csv files.

# Prerequisites/Dependencies
- You must have ROS2 humble installed and configured on your system.
- ROS2's ament and cmake system must be installed first, system-wide.
	-run `sudo apt install ros-humble-ament-cmake*`
- The package uses ros2-humble with packages included as submodules under `src` and other submodules in the main directory, as well as python package dependencies listed in `requirements.txt`.

# Installation

## Step-by-step installation instructions
1. Clone this repo, with the submodules `git clone --recurse-submodules`
2. `cd parse_ros2bag`
3. Source your main ROS2 underlay with `source /opt/ros/humble/setup.bash`
4. Build the required and included ROS2 packages with colcon
	- Run `colcon build --symlink install`
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
