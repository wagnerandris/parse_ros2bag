#!/usr/bin/env python3

import argparse
import rosbag2_py
import subprocess


def parse_ros2bag(bag, output, camera_num, blur):
    print(bag)

    image_topic_names = []
    pointcloud_topic_names = []
    misc_topic_names = []

    topic_types = {
            'std_msgs/msg/Int32': misc_topic_names,

            'sensor_msgs/msg/Image': image_topic_names,
            'sensor_msgs/msg/PointCloud2': pointcloud_topic_names,
            'sensor_msgs/msg/NatSatFix': misc_topic_names,
            'sensor_msgs/msg/TimeReference': misc_topic_names,

            'geometry_msgs/msg/TwistStamped': misc_topic_names,

            'tf2_msgs/msg/TFMessage': misc_topic_names,
            }
    # TODO some of these are not just misc

    # get topics
    info = rosbag2_py.Info()
    metadata = info.read_metadata(bag, 'sqlite3')

    # separate topic types we care about into lists
    for t in metadata.topics_with_message_count:
        print(t.topic_metadata.name, '\t', t.topic_metadata.type, '\n')
        topic_names = topic_types.get(t.topic_metadata.type)
        if topic_names is not None:
            topic_names.append(t.topic_metadata.name)

    # export pointclouds
    for t in pointcloud_topic_names:
        subprocess.Popen([
            'ros2', 'bag', 'export',
            '--in', bag,
            '-t', t, 'pcd',
            '--dir', output + t,
            ])

    # export images
    image_export_processes = []
    for t in image_topic_names:
        image_export_processes.append(subprocess.Popen([
            'ros2', 'bag', 'export',
            '--in', bag,
            '-t', t, 'image',
            '--dir', output + t,
            ]))

    # extract misc topics
    misc_extract_process = subprocess.Popen([
        'ros2', 'bag', 'extract',
        bag,
        '-t', *misc_topic_names,
        '-o', output + '/misc_topics'
        ])

    # wait for image exports to finish
    for p in image_export_processes:
        p.wait()

    # wait for misc extract to finish
    misc_extract_process.wait()

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
