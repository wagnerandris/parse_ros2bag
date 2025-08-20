# parse_ros2bag
A python script to parse a ROS2 bags content into separate image, video, pointcloud and csv files.

# Prerequisites/Dependencies
- The video generation needs FFMPEG to be installed.
	- You probabaly already have it installed, but if not, the install it by running:
   ```
   sudo apt install ffmpeg
   ```
- You must have ROS2 humble installed and configured on your system, with the default underlay sourced.
	- ROS2 humble must be installed with either one of the `ros-humble-desktop` or `ros-humble-ros-base` packages, and the compilers must be installed with
   ```
   sudo apt install ros-dev-tools
   ```
	- To activate and use the underlay, either run
   ```
   source /opt/ros/humble/setup.bash
   ```
   or add the same to the end of your `.bashrc` file.
- ROS2s ament and cmake system must be installed system-wide.
	-run
  	```
  	sudo apt install ros-humble-ament-cmake
   	```
- ros2bag-convert runs into permission issues, if not installed system-wide, so run:
  ```
  sudo pip install git+https://github.com/fishros/ros2bag_convert.git
  ```
- python on Ubuntu does not install the required packages for virtual environments automatically, so you will need to install it with
  ```
  sudo apt install python3.10-venv
  ```
  if not already installed.

# Installation

## Step-by-step installation instructions
0. Have a working ROS2 Humble install, described in prerequisites.
1. Clone this repo, with the submodules
   ```
   git clone --recurse-submodules
   ```
5. Source your main ROS2 underlay with
   ```
   source /opt/ros/humble/setup.bash
   ```
   and switch to the new directory  
   ```
   cd parse_ros2bag
   ```
7. Build the required and included ROS2 packages with colcon. Because colcon tries to build the packages in alphabetical order, ignoring possible conflicts, you need to manually pre-build some packages.
	- Before any other package, build just `rosbag2\_tools`. Run
	```
	colcon build --symlink-install --packages-select rosbag2_tools
 	```
	- After `rosbag2_tools` was successfully built, you can build all the rest. Run
	```
	colcon build --symlink-install
 	```
8. Create a python virtual environment to handle pip dependencies (optional, but highly recommended)
   	- On Ubuntu, ros2bag-convert needs to be installed system-wide, not in the venv as shown in prerequisites
   	- Create venv
    ```
   	python3 -m venv venv
    ```
	- _if the creation of virtual environment fails, you probably skipped a step in prerequisites_
10. Source the built ROS2 packages and python virtual environment by using the provided source script. Run
    ```
    source source_deps.bash
    ```
12. Use pip to install the dependencies
	- run
	```
	pip install -r requirements.txt
 	```
13. **ALL DONE** Use the parser with the options provided in:
    ```
    python3 parse_ros2bag.py -h
    ```

# Usage
If the python venv is created and ros packages are built in the packages root, the ros workspace and venv can be activated with
```
source source_deps.bash
```

With the workspace and environment activated the script can be run with possible options described by
```
python3 parse_ros2bag -h
```

The following options can be specified in a yaml config file (given in the option `-c`):
```
output_dir: str,
blur: bool,
keep_intermediary: bool,
zip: bool,
sync: bool,
sync_slop: int,
sync_topics: list,
topic_blacklist: list,
preview_topics: list,
preview_cols: int,
preview_rows: int,
preview_image_width: int,
preview_image_height: int,
preview_config: str,
ffmpeg_options: str,
ffmpeg_input_options: str,
ffmpeg_output_options: str,
logfile: str,
```
