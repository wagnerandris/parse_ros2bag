#!/usr/bin/env python3

import argparse
import rosbag2_py
import subprocess
import os
import shutil


def parse_ros2bag(bag, output, camera_num, blur):
    scriptpath = os.path.dirname(os.path.realpath(__file__))
    image_topic_names = []
    pointcloud_topic_names = []
    misc_topic_names = []

    topic_types = {
            'sensor_msgs/msg/Image': image_topic_names,
            'sensor_msgs/msg/PointCloud2': pointcloud_topic_names,
            'sensor_msgs/msg/NavSatFix': misc_topic_names,
            'sensor_msgs/msg/TimeReference': misc_topic_names,
            'geometry_msgs/msg/TwistStamped': misc_topic_names,
            'tf2_msgs/msg/TFMessage': misc_topic_names,
            'rcl_interfaces/msg/Log': misc_topic_names,
            'std_msgs/msg/Int32': misc_topic_names,
            }

    # get topics
    info = rosbag2_py.Info()
    metadata = info.read_metadata(bag, 'sqlite3')

    # separate topic types we care about into lists
    for t in metadata.topics_with_message_count:
        topic_names = topic_types.get(t.topic_metadata.type)
        if topic_names is not None:
            topic_names.append(t.topic_metadata.name)

    # export pointclouds
    pointcloud_export_processes = []
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

    # wait for misc extract to finish
    misc_extract_process.wait()

    conversion_process = subprocess.Popen(['ros2bag-convert', output + '/misc_topics/misc_topics_0.db3'])

    # wait for conversion to csv to finish
    conversion_process.wait()

    subprocess.Popen([
        'python3', scriptpath + '/ros2-csv-kml_converter/csv-to-kml.py',
        'misc_topics'],
        cwd=output)

    sync_process = subprocess.Popen([
        'ros2', 'bag', 'sync',
        bag,
        '-t', *image_topic_names,
        '-o', output + '/synced_topics',
        ])

    # wait for syncronization to finish
    sync_process.wait()

    # export synced images (with the same topic names as before)
    synced_image_export_processes = []
    for t in image_topic_names:
        synced_image_export_processes.append(subprocess.Popen([
            'ros2', 'bag', 'export',
            '--in', output + '/synced_topics/synced_topics_0.db3',
            '-t', t, 'image',
            '--dir', output + '/synced_topics' + t,
            ]))

    if blur:
        # wait for image exports to finish and start blurring
        for p, t in zip(image_export_processes, image_topic_names):
            p.wait()
            subprocess.run([
                'python3', 'licenseplate_test.py',
                '-i', output + t,
                '-o', output + '/blurred_images' + t,
                ],
                cwd=scriptpath + '/person_and_licenceplate_blurring')

        # wait for synced image exports to finish
        for p, t in zip(synced_image_export_processes, image_topic_names):
            p.wait()

            # go through synced images' folder and copy their blurred version in it
            for f in os.listdir(output + '/synced_topics' + t):
                shutil.copy(output + '/blurred_images' + t +
                            '/' + os.path.splitext(f)[0] + '.jpg',
                            output + '/synced_topics' + t)

    # TODO create video

    # wait for pointcloud exports to finish
    for p in pointcloud_export_processes:
        p.wait()

    # TODO file structure

    # TODO separate pipelines to separate subprocesses


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
    # TODO verbose/silent
    # TODO keep intermediary files
    # TODO checks

    args = parser.parse_args()
    parse_ros2bag(args.input,
                  os.path.realpath(args.output_dir),
                  args.camera_num,
                  args.blur)
