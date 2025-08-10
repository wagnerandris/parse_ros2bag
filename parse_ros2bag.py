#!/usr/bin/env python3

import rosbag2_py

import yaml
import argparse

import subprocess
import threading
import multiprocessing

import os
import shutil
import zipfile


class ROS2BagParser:
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

    def __init__(self,
                 bag,
                 output_path,
                 blur,
                 keep,
                 preview_topics, preview_cols, preview_rows,
                 ffmpeg_options, ffmpeg_input_options, ffmpeg_output_options,
                 logfile):
        self.script_path = os.path.dirname(os.path.realpath(__file__))
        self.bag = bag

        self.output_path = output_path
        self.image_path = os.path.join(output_path, 'images')
        self.synced_path = os.path.join(output_path, 'synced_topics')
        self.blurred_path = os.path.join(output_path, 'blurred_images') if blur else None
        self.preview_path = os.path.join(output_path, 'previews')
        self.pointcloud_path = os.path.join(output_path, 'pointclouds')
        self.misc_path = os.path.join(output_path, 'misc_topics')

        self.keep = keep

        self.preview_topics = preview_topics
        self.preview_cols = preview_cols
        self.preview_rows = preview_rows

        self.ffmpeg_options = ffmpeg_options
        self.ffmpeg_input_options = ffmpeg_input_options
        self.ffmpeg_output_options = ffmpeg_output_options

        print([
            "ffmpeg",
            *self.ffmpeg_options.split(),
            *self.ffmpeg_input_options.split(),
            "-i", "frame_%04d.jpg",
            *self.ffmpeg_output_options.split(),
            f"{self.output_path}/preview.mp4"])
        exit()

    def parse_pointclouds(self):
        # export pointclouds
        pointcloud_export_processes = []
        for t in self.pointcloud_topic_names:
            pointcloud_export_processes.append(subprocess.Popen([
                'ros2', 'bag', 'export',
                '--in', self.bag,
                '-t', t, 'pcd',
                '--dir', self.pointcloud_path + t,
                ]))

        # wait for pointcloud exports to finish
        for p in pointcloud_export_processes:
            p.wait()

        # zip pointclouds
        shutil.make_archive(self.output_path + '/pointcloud', 'zip', self.pointcloud_path)

        if not self.keep:
            # cleanup
            shutil.rmtree(self.pointcloud_path)

    def export_images(self, image_topic_name):
        # export images
        subprocess.run([
            'ros2', 'bag', 'export',
            '--in', self.bag,
            '-t', image_topic_name, 'image',
            '--dir', self.image_path + image_topic_name,
            ])

        if self.blurred_path:
            # only one thread can run the blurring script at a time
            with self.blurring_lock:
                subprocess.run([
                    'python3', 'licenseplate_test.py',
                    '-i', self.image_path + image_topic_name,
                    '-o', self.blurred_path + image_topic_name,
                    ],
                    cwd=self.script_path + '/person_and_licenceplate_blurring')

    def copy_blurred(self, thread, proc, topic):
        # wait for blur thread
        thread.join()

        # wait for synced export process
        proc.wait()

        # go through synced images' folder and copy their blurred version in it
        for f in os.listdir(self.synced_path + topic):
            # remove original to avoid duplicates if there are .pngs (may be unneccessary)
            os.remove(os.path.join(self.synced_path + topic, f))

            # copy blurred version (always .jpg)
            shutil.copy(self.blurred_path + topic +
                        '/' + os.path.splitext(f)[0] + '.jpg',
                        self.synced_path + topic)

    def create_preview(self):
        # unite images
        # TODO
        subprocess.run([
            'python3', self.script_path + '/create_preview/create_preview.py',
            '-o', self.preview_path,
            '-c', str(self.preview_cols),
            '-r', str(self.preview_rows),
            '-t', *self.preview_topics,
            ], cwd=self.synced_path)

        # create video
        subprocess.run([
            "ffmpeg",
            *self.ffmpeg_options.split(),
            *self.ffmpeg_input_options.split(),
            "-i", "frame_%04.jpg",
            *self.ffmpeg_output_options.split(),
            f"{self.output_path}/preview.mp4"
            ], cwd=self.preview_path)

    def parse_images(self):
        self.blurring_lock = threading.Lock()

        # export (and blur) images
        image_export_threads = []
        for t in self.image_topic_names:
            image_export_threads.append(threading.Thread(target=self.export_images, args=(t,)))
            image_export_threads[-1].start()

        # syncronize
        subprocess.run([
            'ros2', 'bag', 'sync',
            self.bag,
            '-t', *self.image_topic_names,
            '-o', self.synced_path
            ])

        # export synced images
        synced_image_export_processes = []
        for t in self.image_topic_names:
            synced_image_export_processes.append(subprocess.Popen([
                'ros2', 'bag', 'export',
                '--in', self.synced_path + '/synced_topics_0.db3',
                '-t', t, 'image',
                '--dir', self.synced_path + t,
                ]))

        # if blurring is needed
        if self.blurred_path:
            copying_threads = []
            # for each topic
            for thread, proc, topic in zip(image_export_threads,
                                           synced_image_export_processes,
                                           self.image_topic_names):
                # on a new thread wait for the corresponding synced and blurred images,
                # then replace the synced ones with the corresponding blurred ones
                copying_threads.append(threading.Thread(target=self.copy_blurred, args=(thread, proc, topic)))
                copying_threads[-1].start()

            # wait for image exports to finish
            for t in image_export_threads:
                t.join()

            # zip images in a separate process
            zipping_process = multiprocessing.Process(
                    target=shutil.make_archive,
                    args=(self.output_path + '/pictures', 'zip', self.blurred_path)
                    )
            zipping_process.start()

            # wait for copying to finish (image exports always precede)
            for t in copying_threads:
                t.join()
            # create preview video
            self.create_preview()

        else:
            # wait for image exports to finish
            for t in image_export_threads:
                t.join()
            # zip images in a separate process
            zipping_process = multiprocessing.Process(
                    target=shutil.make_archive,
                    args=(self.output_path + '/pictures', 'zip', self.image_path)
                    )
            zipping_process.start()

            # wait for all synced exports then preview then video (probably preceded by image exports)
            for p in synced_image_export_processes:
                p.wait()

            # create preview video
            self.create_preview()

        if not self.keep:
            zipping_process.join()
            # cleanup
            shutil.rmtree(self.image_path)
            shutil.rmtree(self.synced_path)
            shutil.rmtree(self.preview_path)
            if self.blurred_path:
                shutil.rmtree(self.blurred_path)

    def parse_misc(self):
        # extract misc topics
        subprocess.run([
            'ros2', 'bag', 'extract',
            self.bag,
            '-t', *self.misc_topic_names,
            '-o', self.misc_path
            ])

        # convert to csv
        subprocess.run(['ros2bag-convert', self.misc_path + '/misc_topics_0.db3'])

        # convert to kml
        subprocess.run([
            'python3', self.script_path + '/ros2-csv-kml_converter/csv-to-kml.py',
            self.misc_path
            ])

        if not self.keep:
            os.remove(self.misc_path + '/fix.csv')

    def zip_bag(self):
        # zip original bag
        with zipfile.ZipFile(self.output_path + '/bag.zip', 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(self.bag, arcname=os.path.basename(self.bag))

    def parse_ros2bag(self):
        # make output dir
        if os.path.isdir(self.output_path) and os.listdir(self.output_path):
            print('Error: output path is a non-empty folder.')
            exit()
        os.makedirs(self.output_path, exist_ok=True)

        # start zipping original bag
        multiprocessing.Process(target=self.zip_bag).start()

        # get topics
        info = rosbag2_py.Info()
        metadata = info.read_metadata(self.bag, 'sqlite3')

        # separate topic types we care about into lists
        for t in metadata.topics_with_message_count:
            topic_names = self.topic_types.get(t.topic_metadata.type)
            if topic_names is not None:
                topic_names.append(t.topic_metadata.name)

        # start independent parsing pipelines
        threading.Thread(target=self.parse_pointclouds).start()
        threading.Thread(target=self.parse_misc).start()
        self.parse_images()


def load_config_file(config_path):
    VALID_CONFIG_OPTIONS = {
            'output_dir': str,
            'blur': bool,
            'keep_intermediary': bool,
            'preview_topics': list,
            'preview_cols': int,
            'preview_rows': int,
            'preview_config': str,
            'ffmpeg_options': str,
            'ffmpeg_input_options': str,
            'ffmpeg_output_options': str,
            'logfile': str,
    }

    with open(config_path, 'r') as f:
        config = yaml.safe_load(f) or {}

    # validate keys
    invalid_keys = set(config) - VALID_CONFIG_OPTIONS.keys()
    if invalid_keys:
        raise ValueError(f"Invalid config keys: {', '.join(invalid_keys)}. "
                         f"Valid keys are: {', '.join(sorted(VALID_CONFIG_OPTIONS.keys()))}")

    # validate types
    for key, expected_type in VALID_CONFIG_OPTIONS.items():
        if key in config and not isinstance(config[key], expected_type):
            raise TypeError(f"Invalid type for '{key}': expected {expected_type.__name__}, got {type(config[key]).__name__}")

    return config


if __name__ == '__main__':
    # parse command-line arguments
    parser = argparse.ArgumentParser(description='Automatically parse ROS2 bags into separate files and folders per device.')

    # parse config file
    parser.add_argument('-c', '--config',
                        type=str, default='./config.yaml',
                        help='Path to config file')
    args_config, remaining_argv = parser.parse_known_args()

    # load config file if it exists
    config_options = {}
    if os.path.exists(args_config.config):
        config_options = load_config_file(args_config.config)

    # parse remaining arguments
    parser.add_argument('input',
                        type=str,
                        help='Path to input ROS2 bag')
    parser.add_argument('-o', '--output_dir',
                        type=str, required=False, default='./convert',
                        help='Path to the output folder')
    parser.add_argument('-b', '--blur',
                        action='store_true',
                        help='Blur faces and license plates')
    parser.add_argument('-k', '--keep-intermediary',
                        action='store_true',
                        help='Keep intermediary files')
    parser.add_argument('-l', '--logfile',
                        type=str,
                        help='Path to log file')

    # override defaults with config file options
    parser.set_defaults(**config_options)

    # TODO verbose/silent

    args = parser.parse_args()

    print(args)

    bag_parser = ROS2BagParser(args.input,
                               os.path.realpath(args.output_dir),
                               args.blur,
                               args.keep_intermediary,
                               getattr(args, 'preview_topics', None),
                               getattr(args, 'preview_cols', None),
                               getattr(args, 'preview_rows', None),
                               getattr(args, 'ffmpeg_options', ''),
                               getattr(args, 'ffmpeg_input_options', ''),
                               getattr(args, 'ffmpeg_output_options', ''),
                               args.logfile
                               )
    bag_parser.parse_ros2bag()
