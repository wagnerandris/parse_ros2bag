#!/usr/bin/env python3

import argparse
import rosbag2_py
# TODO wtf
import rosbag2_tools
import ros2bag_tools


def get_topics(bag):
    info = rosbag2_py.Info()
    metadata = info.read_metadata(bag, 'sqlite3')

    return [t.topic_metadata.name for t in metadata.topics_with_message_count]


def parse_ros2bag(bag, output, camera_num, blur):
    topics = get_topics(bag)
    # TODO check topics against camera_num
    print(topics)

    # TODO export images

    # TODO sync
    # TODO export synced images

    # TODO blur images (synced and not synced)

    # TODO create video

    # TODO extract misc into new bag
    # TODO export to csv
    # TODO convert to kml

    # TODO file structure


if __name__ == '__main__':
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Automatically parse ROS2 bags into separate files and folders per device.')
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
