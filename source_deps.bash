SCRIPT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source /opt/ros/humble/setup.bash # Source the system-wide RO2 just to be sure it is sourced
source "${SCRIPT_PATH}/install/setup.bash" # ROS2 packages
source "${SCRIPT_PATH}/venv/bin/activate" # python venv
