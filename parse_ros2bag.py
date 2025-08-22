#!/usr/bin/env python3

import rosbag2_py

import yaml
import argparse

import subprocess
import threading
import multiprocessing

import torch

import sys
import os
import shutil
import zipfile

import logging


def log_stream(stream, prefix, logger):
    for line in iter(stream.readline, ''):
        # Skip spinner/progress-style lines (heuristic)
        if line.strip() == '' or line.endswith('\r') or len(line.strip()) < 3:
            continue
        logger.info(f"[{prefix}] {line.strip()}")
    stream.close()


def log_and_print(message, logger=None):
    print(message)
    if logger:
        logger.info(message)


def run_logged_subprocess(cmd, cwd='.', logger=None):
    if not logger:
        subprocess.run(cmd, cwd=cwd)

    else:
        process = subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        # Start threads to capture stdout and stderr
        threading.Thread(target=log_stream, args=(process.stdout, "stdout", logger), daemon=True).start()
        threading.Thread(target=log_stream, args=(process.stderr, "stderr", logger), daemon=True).start()

        process.wait()


def Popen_logged_subprocess(cmd, cwd='.', logger=None):
    if not logger:
        return subprocess.Popen(cmd, cwd=cwd)

    else:
        process = subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        # Start threads to capture stdout and stderr
        threading.Thread(target=log_stream, args=(process.stdout, "stdout", logger), daemon=True).start()
        threading.Thread(target=log_stream, args=(process.stderr, "stderr", logger), daemon=True).start()

        return process


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
                 zip_,
                 sync,
                 sync_slop,
                 sync_topics,
                 topic_blacklist,
                 preview_config, preview_topics, preview_cols, preview_rows, preview_image_width, preview_image_height,
                 ffmpeg_options, ffmpeg_input_options, ffmpeg_output_options,
                 logger):
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
        self.zip = zip_

        self.sync = sync
        self.sync_slop = sync_slop
        self.sync_topics = ['/' + t for t in sync_topics] # topics begint with / for some reason
        self.topic_blacklist = ['/' + t for t in topic_blacklist] # topics begint with / for some reason

        self.preview_config = preview_config
        self.preview_topics = ['/' + t for t in preview_topics] # topics begint with / for some reason
        self.preview_cols = preview_cols
        self.preview_rows = preview_rows
        self.preview_image_width = preview_image_width
        self.preview_image_height = preview_image_height

        self.ffmpeg_options = ffmpeg_options
        self.ffmpeg_input_options = ffmpeg_input_options
        self.ffmpeg_output_options = ffmpeg_output_options

        self.logger = logger

    def parse_pointclouds(self):
        if not self.pointcloud_topic_names:
            return

        log_and_print('Pointcloud parsing started', self.logger)
        log_and_print('Exporting pointcoulds', self.logger)
        # export pointclouds
        for t in self.pointcloud_topic_names:
                run_logged_subprocess([
                'ros2', 'bag', 'export',
                '--in', self.bag,
                '-t', t, 'pcd',
                '--dir', self.pointcloud_path + t,
                ], logger=self.logger)

        if self.zip:
            log_and_print('Zipping pointclouds', self.logger)
            shutil.make_archive(self.output_path + '/pointcloud', 'zip', self.pointcloud_path)

        if not self.keep and self.zip:
            # cleanup
            shutil.rmtree(self.pointcloud_path)

        log_and_print('Pointcould parsing finished', self.logger)

    def export_images(self, image_topic_name):
        # export images
        run_logged_subprocess([
            'ros2', 'bag', 'export',
            '--in', self.bag,
            '-t', image_topic_name, 'image',
            '--dir', self.image_path + image_topic_name,
            ], logger=self.logger)

    def export_and_blur_images(self, image_topic_name):
        self.export_images(image_topic_name)

        run_logged_subprocess([
            'python3', 'licenseplate_test.py',
            '-i', self.image_path + image_topic_name,
            '-o', self.blurred_path + image_topic_name,
            ],
            cwd=self.script_path + '/person_and_licenceplate_blurring',
            logger=self.logger)

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

    def create_preview(self, image_path):
        if not self.preview_topics:
            log_and_print('No preview topics, skipping step', self.logger)
            if self.logger:
                self.logger.info('No preview topics, skipping step')
            return
        else:
            log_and_print('Creating preview images', self.logger)

        # unite images
        cmd = ['python3', self.script_path + '/create_preview/create_preview.py', '-o', self.preview_path]
        if self.preview_config:
            cmd.append(f'--config {self.preview_config}')
        if self.preview_cols:
            cmd.append(f'-c {str(self.preview_cols)}')
        if self.preview_rows:
            cmd.append(f'-r {str(self.preview_rows)}')
        if self.preview_image_width:
            cmd.append(f'-iw {str(self.preview_image_width)}')
        if self.preview_image_height:
            cmd.append(f'-ih {str(self.preview_image_height)}')
        cmd.append('-t')
        cmd += [t[1:] for t in self.preview_topics]
        run_logged_subprocess(cmd, cwd=image_path, logger=self.logger)

        # create video
        if self.logger:
            self.logger.info(f'Creating video with {cmd}')
        cmd = ["ffmpeg"]
        if self.logger:
            cmd.append("-nostats")
        cmd += [
            *self.ffmpeg_options.split(),
            *self.ffmpeg_input_options.split(),
            "-i", "frame_%04d.jpg",
            *self.ffmpeg_output_options.split(),
            f"{self.output_path}/preview.mp4"
            ]
        run_logged_subprocess(cmd, cwd=self.preview_path, logger=self.logger)

        # cleanup
        if not self.keep:
            shutil.rmtree(self.preview_path)

    def sync_and_export_images(self):
        log_and_print('Synchronizing topics', self.logger)
        # create config file
        sync_config = f'extract -t {" ".join(self.sync_topics)}\nsync -t {" ".join(self.sync_topics)}'
        if self.sync_slop:
            sync_config += f' --slop {str(self.sync_slop)}'
        with open(self.output_path + '/sync.config', 'w') as file: # sync needs an empty folder
            file.write(sync_config)

        # syncronize and extract
        cmd = ['ros2', 'bag', 'process',
               self.bag,
               '-c', self.output_path + '/sync.config',
               '-o', self.synced_path]
        run_logged_subprocess(cmd, logger=self.logger)

        # cleanup
        if self.keep:
            os.rename(self.output_path + '/sync.config', self.synced_path + '/sync.config')
        else:
            os.remove(self.output_path + '/sync.config')

        # export synced preview images
        synced_image_export_processes = []
        for t in self.preview_topics:
            synced_image_export_processes.append(Popen_logged_subprocess([
                'ros2', 'bag', 'export',
                '--in', self.synced_path + '/synced_topics_0.db3',
                '-t', t, 'image',
                '--dir', self.synced_path + t,
                ], logger=self.logger))

        # return processes so they can be waited on
        return synced_image_export_processes

    def zip_images(self, image_export_threads, image_path):
        # wait for image exports to finish
        for t in image_export_threads:
            t.join()

        log_and_print('Zipping images', self.logger)
        # zip images in a separate process
        zipping_process = multiprocessing.Process(
                target=shutil.make_archive,
                args=(self.output_path + '/pictures', 'zip', image_path)
                )
        zipping_process.start()
        zipping_process.join()

    def parse_images(self):
        if not self.image_topic_names:
            return

        log_and_print('Image parsing started', self.logger)
        # with blurring
        if self.blurred_path:
            # get blurring model
            torch.hub.load('ultralytics/yolov5', 'yolov5s', pretrained=True, force_reload=True)

            log_and_print('Exporting and blurring images', self.logger)
            # export and blur images
            image_export_threads = []
            for t in self.image_topic_names:
                image_export_threads.append(threading.Thread(target=self.export_and_blur_images, args=(t,)))
                image_export_threads[-1].start()

            if self.zip:
                # zip images
                zipping_thread = threading.Thread(
                        target=self.zip_images, args=(image_export_threads, self.blurred_path))
                zipping_thread.start()

            # sync if needed
            if self.sync:
                synced_image_export_processes = self.sync_and_export_images()

                # copy blurred versions into synced images
                copying_threads = []
                # for each topic we'll have to wait for all previous exports
                for thread, proc, topic in zip(image_export_threads,
                                               synced_image_export_processes,
                                               self.image_topic_names):
                    copying_threads.append(threading.Thread(
                        target=self.copy_blurred, args=(thread, proc, topic)))
                    copying_threads[-1].start()

                # create preview video
                # wait for copying to finish
                for t in copying_threads:
                    t.join()
                self.create_preview(self.synced_path)

            else:
                # wait for export and blur to finish
                for t in image_export_threads:
                    t.join()
                # create preview video
                self.create_preview(self.blurred_path)

        # without blurring
        else:
            log_and_print('Exporting images', self.logger)
            # export images
            image_export_threads = []
            for t in self.image_topic_names:
                image_export_threads.append(threading.Thread(target=self.export_images, args=(t,)))
                image_export_threads[-1].start()

            # zip images
            if self.zip:
                zipping_thread = threading.Thread(
                        target=self.zip_images, args=(image_export_threads, self.image_path))
                zipping_thread.start()

            if self.sync:
                # sync
                synced_image_export_processes = self.sync_and_export_images()

                # create preview video
                # wait for all synced_exports
                for p in synced_image_export_processes:
                    p.wait()
                self.create_preview(self.synced_path)

            else:
                # wait for exports to finish
                for t in image_export_threads:
                    t.join()
                # create preview video
                self.create_preview(self.image_path)

        # cleanup
        if not self.keep:
            if self.zip:
                zipping_thread.join()
                shutil.rmtree(self.image_path)
                if self.blurred_path:
                    shutil.rmtree(self.blurred_path)
            else:
                if self.blurred_path:
                    shutil.rmtree(self.image_path)

            if self.sync:
                shutil.rmtree(self.synced_path)

        log_and_print('Image parsing finished', self.logger)

    def parse_misc(self):
        if not self.misc_topic_names:
            return

        log_and_print('Misc parsing started', self.logger)
        log_and_print('Extracting misc topics', self.logger)
        # extract misc topics
        run_logged_subprocess([
            'ros2', 'bag', 'extract',
            self.bag,
            '-t', *self.misc_topic_names,
            '-o', self.misc_path
            ], logger=self.logger)

        log_and_print('Converting misc topics', self.logger)
        # convert to csv
        run_logged_subprocess(['ros2bag-convert', self.misc_path + '/misc_topics_0.db3'], logger=self.logger)

        if '/fix' in self.misc_topic_names:
            # convert to kml
            run_logged_subprocess([
                'python3', self.script_path + '/ros2-csv-kml_converter/csv-to-kml.py',
                self.misc_path
                ], logger=self.logger)

        if not self.keep:
            os.remove(self.misc_path + '/fix.csv')
            os.remove(self.misc_path + '/metadata.yaml')
            os.remove(self.misc_path + '/misc_topics_0.db3')

        log_and_print('Misc parsing finished', self.logger)

    def zip_bag(self):
        log_and_print('Zipping ros2 bag', self.logger)
        # zip original bag
        with zipfile.ZipFile(self.output_path + '/bag.zip', 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(self.bag, arcname=os.path.basename(self.bag))

    def sort_topics(self):
        log_and_print('Sorting topics', self.logger)
        # get topics
        info = rosbag2_py.Info()
        metadata = info.read_metadata(self.bag, 'sqlite3')

        # separate topic types we care about into lists
        if self.sync:
            new_sync_topics = []
            for t in metadata.topics_with_message_count:
                if t.topic_metadata.name in self.topic_blacklist:
                    continue

                if t.topic_metadata.name in self.sync_topics:
                    new_sync_topics.append(t.topic_metadata.name)

                topic_names = self.topic_types.get(t.topic_metadata.type)
                if topic_names is not None:
                    topic_names.append(t.topic_metadata.name)

            if new_sync_topics:
                self.sync_topics = new_sync_topics
            else:
                self.sync = False
                log_and_print('Sync topics not found, step will be skipped', self.logger)
        else:
            for t in metadata.topics_with_message_count:
                if t.topic_metadata.name in self.topic_blacklist:
                    continue

                topic_names = self.topic_types.get(t.topic_metadata.type)
                if topic_names is not None:
                    topic_names.append(t.topic_metadata.name)

        if self.preview_topics:
            new_preview_topics = []
            for t in self.preview_topics:
                if self.sync and t not in self.sync_topics:
                    continue
                if t in self.image_topic_names:
                    new_preview_topics.append(t)

            if new_preview_topics:
                self.preview_topics = new_preview_topics
            else:
                log_and_print('Preview topics not found, step will be skipped', self.logger)

    def parse_ros2bag(self):
        # make output dir
        if os.path.isdir(self.output_path) and os.listdir(self.output_path):
            log_and_print('Error: output path is a non-empty folder.', self.logger)
            exit()
        os.makedirs(self.output_path, exist_ok=True)

        # start zipping original bag
        if self.zip:
            bag_zipping_process = multiprocessing.Process(target=self.zip_bag)
            bag_zipping_process.start()

        # put topics into different lists based on types and options
        self.sort_topics()

        # start independent parsing pipelines
        pointcloud_parser_thread = threading.Thread(target=self.parse_pointclouds)
        pointcloud_parser_thread.start()
        misc_parser_thread = threading.Thread(target=self.parse_misc)
        misc_parser_thread.start()
        self.parse_images()

        if self.zip:
            bag_zipping_process.join()
        pointcloud_parser_thread.join()
        misc_parser_thread.join()
        log_and_print('Finished', self.logger)


def load_config_file(config_path):
    VALID_CONFIG_OPTIONS = {
            'output_dir': str,
            'blur': bool,
            'keep_intermediary': bool,
            'zip': bool,
            'sync': bool,
            'sync_slop': float,
            'sync_topics': list,
            'topic_blacklist': list,
            'preview_topics': list,
            'preview_cols': int,
            'preview_rows': int,
            'preview_image_width': int,
            'preview_image_height': int,
            'preview_config': str,
            'ffmpeg_options': str,
            'ffmpeg_input_options': str,
            'ffmpeg_output_options': str,
            'logfile': str,
            'verbose': bool
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

    if config == {}:
        print(f'\033[1;33mWarning:\033[0m No options could be loaded from {config_path}', file=sys.stderr)

    return config


if __name__ == '__main__':
    # parse command-line arguments
    parser = argparse.ArgumentParser(description='Automatically parse ROS2 bags into separate files and folders per device.')

    # parse config file
    parser.add_argument('-c', '--config',
                        type=str, default='./config.yaml',
                        help='Path to config file')
    parser.add_argument('input',
                        type=str,
                        help='Path to input ROS2 bag')
    parser.add_argument('-o', '--output_dir',
                        type=str, required=False, default='./convert',
                        help='Path to the output folder')
    parser.add_argument('-tb', '--topic_blacklist',
                        nargs='*',
                        help='Topics not to parse')
    parser.add_argument('-b', '--blur',
                        action='store_true',
                        help='Blur faces and license plates')
    parser.add_argument('-nb', '--no_blur',
                        dest='blur',
                        action='store_false',
                        help='Do not blur faces and license plates')
    parser.add_argument('-k', '--keep_intermediary',
                        action='store_true',
                        help='Keep intermediary files')
    parser.add_argument('-nk', '--no_keep_intermediary',
                        dest='keep_intermediary',
                        action='store_false',
                        help='Do not keep intermediary files')
    parser.add_argument('-z', '--zip',
                        action='store_true', default=True,
                        help='Zip results')
    parser.add_argument('-nz', '--no_zip',
                        dest='zip',
                        action='store_false',
                        help='Do not zip results')
    parser.add_argument('-s', '--sync',
                        action='store_true', default=True,
                        help='Sync topics')
    parser.add_argument('-ns', '--no_sync',
                        dest='sync',
                        action='store_false',
                        help='Do not sync topics')
    parser.add_argument('-ss', '--sync_slop',
                        type=float,
                        help='Synchronization slope/error')
    parser.add_argument('-st', '--sync_topics',
                        nargs='*',
                        help='Topics to synchronize for preview creation')
    parser.add_argument('-pc', '--preview_config',
                        type=str,
                        help='Path to config file for create_preview.py')
    parser.add_argument('-pt', '--preview_topics',
                        nargs='*',
                        help='Topics to use in preview creation')
    parser.add_argument('-l', '--logfile',
                        type=str,
                        help='Path to log file')
    parser.add_argument('--verbose',
                        action='store_true',
                        help='Print every log message to the terminal')
    args_config, remaining_argv = parser.parse_known_args()

    # load config file if it exists
    config_options = {}
    if os.path.isfile(args_config.config):
        config_options = load_config_file(args_config.config)
    else:
        print(f'\033[1;33mWarning:\033[0m Cannot read config from {args_config.config}: no such file', file=sys.stderr)

    # parse remaining arguments

    # override defaults with config file options
    parser.set_defaults(**config_options)

    args = parser.parse_args()

    print(f'Parsed args: {args}')

    handlers = []

    if args.logfile:
        handlers.append(logging.FileHandler(args.logfile))
        print(f'Starting parser... find more detailed logs in {args.logfile}')
    if args.verbose:
        handlers.append(logging.StreamHandler())
        print('Starting parser...')

    if handlers:
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s',
            handlers=handlers
        )

        logger = logging.getLogger(__name__)
        logger.info(f'Parsed args: {args}')
    else:
        logger = None
        print('Starting parser...')

    bag_parser = ROS2BagParser(os.path.realpath(args.input),
                               os.path.realpath(args.output_dir),
                               args.blur,
                               args.keep_intermediary,
                               args.zip,
                               args.sync,
                               args.sync_slop,
                               getattr(args, 'sync_topics', []),
                               getattr(args, 'topic_blacklist', []),
                               getattr(args, 'preview_config', None),
                               getattr(args, 'preview_topics', None),
                               getattr(args, 'preview_cols', None),
                               getattr(args, 'preview_rows', None),
                               getattr(args, 'preview_image_width', None),
                               getattr(args, 'preview_image_height', None),
                               getattr(args, 'ffmpeg_options', ''),
                               getattr(args, 'ffmpeg_input_options', ''),
                               getattr(args, 'ffmpeg_output_options', ''),
                               logger
                               )
    bag_parser.parse_ros2bag()
