#!/usr/bin/env python3

import subprocess
import argparse

def parse_ros2bag(bag, output, camera_num, blur):
    print(bag, output, camera_num, blur)


if __name__ == '__main__':
# Parse command-line arguments
    parser = argparse.ArgumentParser(
            description='Automatically parse ROS2 bags into separate files and folders per device.')
    parser.add_argument('input',
                        type=str,
                        help='Path to input ROS2 bag')
    parser.add_argument('-o', '--output_dir',
                        type=str, required=False, default='.',
                        help='Path to the output folder')
    parser.add_argument('-c', '--camera_num',
                        type=str, required=False, default=None,
                        help='Number of cameras to consider')
    parser.add_argument('-b', '--blur',
                        action='store_true',
                        help='Blur faces and license plates')

    args = parser.parse_args()
    parse_ros2bag(args.input, args.output_dir, args.camera_num, args.blur)
