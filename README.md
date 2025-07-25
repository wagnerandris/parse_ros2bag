# parse_ros2bag
A python script to parse a ROS2 bag's content into separate image, video, pointcloud and csv files.

# Dependencies
The package uses ros2-humble with packages included as submodules under [`src`](https://github.com/wagnerandris/parse_ros2bag/tree/main/src) and other submodules in the main directory, as well as python package dependencies listed in `requirements.txt`.

# Installation
## ros packages
With a ros2-humble workspace sourced, the packages can be build with `colcon build`. Note that gcc13 is required for this.

## python packages
Python packages can be installed via `pip install -r requirements.txt`. Using a venv is advised.

# Usage
If the python venv is created and ros packages are built in the package's root, the ros workspace and venv can be activated with `source source_deps.bash`.

With the workspace and environment activated the script can be run with possible options described by `python parse_ros2bag -h`.
