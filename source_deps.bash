SCRIPT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_PATH}/install/setup.bash" # ROS2 packages
source "${SCRIPT_PATH}/venv/bin/activate" # python venv
export PYTHONPATH="${PYTHONPATH}:${SCRIPT_PATH}/create_preview" # for create_preview.py
