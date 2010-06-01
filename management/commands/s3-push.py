"""
Sync Media to S3
================

Django command that scans all files in your settings.MEDIA_ROOT folder and
uploads them to S3 using the same directory structure. This command does
not mirror the files, just pushing them if the local copy is newer than 
the remote copy.

Note: This script requires the Python boto library and a valid Amazon Web
Services API key.  It will skip .svn directories.

Requires Python 2.6 or newer.

Required settings.py variables:
AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''
AWS_BUCKET_NAME = ''

This command optionally:
* gzip any CSS/Javascript files it finds and adds the appropriate
  'Content-Encoding' header.
* sets an 'Expires' header for 2 years from today.

Command options are:
  -p PREFIX, --prefix=PREFIX
                        The prefix to prepend to the path on S3.
  --gzip                Enables gzipping of javascript/css files.
  --expires             Enables expires header.
  --force               Skip the file mtime check to force upload of all
                        files.
  --dryrun              Only show actions instead of uploading files
  --workers             Specify the number of worker processes to use for uploading files.
  --verbose             Prints the basic output
  --debug               Prints the maximum output


Copyright (c) 2010 Bryan Pieper, http://www.thepiepers.net/

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

"""

import optparse
import os
import re
import sys

if sys.version_info < (2, 6):
    raise "Python 2.6+ required"

from django.core.management.base import BaseCommand, CommandError
from django.conf import settings

try:
    import boto
    import boto.exception
except ImportError:
    raise ImportError, "The boto library is not installed."

from multiprocessing import Process
from . import S3UploadWorker, S3File, DEFAULT_OPTIONS, get_queue


# svn directory filter
svn_re = re.compile(r'/\.svn/?')


class Command(BaseCommand):

    # Extra variables to avoid passing these around
    FILTER_LIST = ['.DS_Store', '.svn', '.project', '.pydevproject',]

    option_list = BaseCommand.option_list + DEFAULT_OPTIONS + (
        optparse.make_option('-p', '--prefix',
            dest='prefix', default='',
            help="The prefix to prepend to the path on S3."),
        optparse.make_option('--force',
            action='store_true', dest='force', 
            help="Skip the file mtime check to force upload of all files."),
    )

    help = "Pushes the complete MEDIA_ROOT structure and files to the given S3 bucket."

    def handle(self, *args, **options):

        # Check for AWS keys in settings
        if not hasattr(settings, 'AWS_ACCESS_KEY_ID') or \
           not hasattr(settings, 'AWS_SECRET_ACCESS_KEY'):
           raise CommandError("Missing AWS keys from settings file.  Please " \
                     "supply both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.")

        if not hasattr(settings, 'AWS_BUCKET_NAME'):
            raise CommandError("Missing bucket name from settings file. Please " \
                " add the AWS_BUCKET_NAME to your settings file.")
        else:
            if not settings.AWS_BUCKET_NAME:
                raise CommandError("AWS_BUCKET_NAME cannot be empty.")

        if not hasattr(settings, 'MEDIA_ROOT'):
            raise CommandError("MEDIA_ROOT must be set in your settings.")
        else:
            if not settings.MEDIA_ROOT:
                raise CommandError("settings.MEDIA_ROOT must have a value")

        self.prefix = options.get('prefix')
        
        processes_count = int(options.get('processes'))
        if processes_count < 1:
            processes_count = 1
        
        self.verbosity = 0
        if options.get('verbose'):
            self.verbosity = 1
        if options.get('debug'):
            self.verbosity = 2
        
        # arg list for the worker processes
        process_args = (
            settings.AWS_BUCKET_NAME,                        
            settings.AWS_ACCESS_KEY_ID,
            settings.AWS_SECRET_ACCESS_KEY,
            self.verbosity,
            options.get('prefix'),
            options.get('gzip'),
            options.get('expires'),
            options.get('force'),
            options.get('dryrun'),
        )

        file_filter = lambda f: not f in self.FILTER_LIST
        
        media_root = settings.MEDIA_ROOT
        if not media_root.endswith('/'):
            media_root += '/'
        
        for root, dirs, files in os.walk(media_root):
            files = filter(file_filter, files)
            if not files:
                continue
            if bool(svn_re.search(root)):
                continue

            for file in files:
                file_key = os.path.join(root[len(media_root):], file)
                if self.prefix:
                    file_key = self.prefix + file_key

                filename = os.path.join(root, file)
                
                # queue the file object for S3
                s3_file = S3File(settings.AWS_BUCKET_NAME, file_key, filename)
                get_queue().put(s3_file)

        process_workers = []
        for num in xrange(processes_count):
            process_workers.append(S3UploadWorker(num, *process_args))

        for process_worker in process_workers:
            process = Process(target=process_worker)
            process.start()
