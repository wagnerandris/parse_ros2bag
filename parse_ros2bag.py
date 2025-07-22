#!/usr/bin/env python3

import argparse
import rosbag2_py
import subprocess


def get_topics(bag):

    image_topics = []
    return image_topics


def parse_ros2bag(bag, output, camera_num, blur):
    # get topics
    info = rosbag2_py.Info()
    metadata = info.read_metadata(bag, 'sqlite3')

    # image topics
    image_topic_names = [t.topic_metadata.name
                        for t in metadata.topics_with_message_count
                        if t.topic_metadata.type == 'sensor_msgs/msg/Image']

    image_export_processes = []
    for t in image_topic_names:
        image_export_processes.append(subprocess.Popen([
            "ros2", "bag", "export",
            "--in", bag,
            "-t", t, "image",
            "--dir", '.' + t
            ]))

    for p in image_export_processes:
        p.wait()

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
