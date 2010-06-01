"""
Sync Media from SVN to S3
================

Django command for synchronizing the S3 bucket with the local subversion MEDIA_ROOT repository.
Performs the difference (svn diff) between the MEDIA_ROOT and the S3 bucket. Only changes and
deletions are synchronized.   

Note: This script requires the Python boto library and valid Amazon Web
Services API keys.  This svn integration is handled by the pysvn library. The security
credentials pulled from your local svn client config or via command line prompt (svn+ssh).

Requires Python 2.6 or newer.

Required Django settings:
AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''
AWS_BUCKET_NAME = ''

The local MEDIA_ROOT needs to a valid SVN repository.

This command optionally:
* gzip any CSS/Javascript files it finds and adds the appropriate
  'Content-Encoding' header.
* sets an 'Expires' header for 2 years from today.

Command options are:
  -p PREFIX, --prefix=PREFIX
                        The prefix to prepend to the path on S3.
  --gzip                Enables gzipping of javascript/css files.
  --expires             Enables expires header
  --dryrun              Only show actions instead of uploading files
  --workers             Specify the number of worker processes to use for uploading files.
  --verbose             Prints the basic output
  --debug               Prints the maximum output
  --ignore-url          Ignores the stored SVN url. This is needed if you access the 
                        repository via different endpoints. For example file:///svnhost/repo and 
                        ssh+svn://svnhost/repo


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
import time
from datetime import datetime

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


try:
    import pysvn
except ImportError:
    raise ImportError, "The pysvn library is not installed."

try:
    import yaml
except ImportError:
    raise ImportError, "The yaml library is not installed."

INITIAL_REVISION = -1  # if svn config not present 


class Command(BaseCommand):

    # Extra variables to avoid passing these around
    FILTER_LIST = ['.DS_Store']
    
    # the config file for the svn revision
    SVN_REVISION_CONF = 'svn_revision.yaml'

    option_list = BaseCommand.option_list + DEFAULT_OPTIONS + (
        optparse.make_option('--ignore-url',
            dest='ignore_url', action='store_true',
            help="Ignores the remote svn repository url"),
    )

    help = "Synchronizes the MEDIA_ROOT Subversion (svn) changes to Amazon S3"

    def handle(self, *args, **options):
        
        # Check for AWS keys in settings
        if not hasattr(settings, 'AWS_ACCESS_KEY_ID') or \
                not hasattr(settings, 'AWS_SECRET_ACCESS_KEY'):
            raise CommandError("Missing AWS keys from settings file.  Please " \
                     "supply both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.")

        if not hasattr(settings, 'AWS_BUCKET_NAME') or not settings.AWS_BUCKET_NAME:
            raise CommandError("AWS_BUCKET_NAME must be set in your settings.")
        
        if not hasattr(settings, 'MEDIA_ROOT') or not settings.MEDIA_ROOT:
            raise CommandError("MEDIA_ROOT must be set in your settings.")
        
        processes_count = int(options.get('processes'))
        if processes_count < 1:
            processes_count = 1
       
        self.dryrun = options.get('dryrun')

        self.verbosity = 0
        if options.get('verbose'):
            self.verbosity = 1
        if options.get('debug'):
            self.verbosity = 2
        
        # skip svn configured url in S3 config
        self.ignore_svn_url = options.get('ignore_url')

        # Note: If you are using the svn+ssh protocol, pysvn may prompt for credentials multiple times 
        #       depending on your ssh client configuration. 

        client = pysvn.Client()
        client.set_interactive(True)
            
        # grab s3 config file data
        s3_svn_revision = self.get_s3_svn_revision()

        if s3_svn_revision['revision'] == INITIAL_REVISION:
            # look up initial revision for the given repo
            # basically, push the repo from the first log entry revision
            if self.verbosity > 0:
                print "Pulling svn logs for local repo"
            history = client.log(settings.MEDIA_ROOT)
            first_entry = history[-1]
            s3_svn_revision['revision'] = first_entry.revision.number
            s3_svn_revision['initial_revision'] = s3_svn_revision['revision']
            if self.verbosity > 0:
                print "Using revision %s for first upload" % s3_svn_revision['revision']

        # local svn repo information
        local_repo_info = client.info(settings.MEDIA_ROOT)
        if 'url' in s3_svn_revision and s3_svn_revision['url']:
            if s3_svn_revision['url'] != local_repo_info.url:
                if not self.ignore_svn_url :
                    raise CommandError("The S3 repo %s does not match the local repo %s" % \
                                       (s3_svn_revision['url'], local_repo_info.url))
           
        # set the url location to remote s3 conf container for update later
        if not self.ignore_svn_url:
            s3_svn_revision['url'] = str(local_repo_info.url)
        
        s3_svn_revision['uuid'] = str(local_repo_info.uuid)

        if self.verbosity > 0:
            print "Local SVN revision: %s" % local_repo_info.revision.number
            print "S3 SVN revision: %s" % s3_svn_revision['revision']
            print "Running svn diff"
        
        # calculates the changes from the local svn revision vs the s3 svn revision
        changes = client.diff_summarize(url_or_path1=settings.MEDIA_ROOT, 
                        revision1=pysvn.Revision(pysvn.opt_revision_kind.number, s3_svn_revision['revision']),
                        url_or_path2=local_repo_info.url,
                        revision2=local_repo_info.revision,
                        recurse=True)

        # the list of changes from the local to s3 repository
        changed_files = []
        
        for change in changes:
            if change.node_kind in (pysvn.node_kind.file, pysvn.node_kind.dir,) and \
                    change.summarize_kind != pysvn.diff_summarize_kind.normal:
                s3_file = S3File(settings.AWS_BUCKET_NAME, change.path, os.path.join(settings.MEDIA_ROOT, change.path))
                
                if change.summarize_kind == pysvn.diff_summarize_kind.delete:
                    s3_file.delete = True

                if change.node_kind == pysvn.node_kind.dir and \
                        change.summarize_kind != pysvn.diff_summarize_kind.delete:
                    # don't upload individual directories, only delete
                    continue

                changed_files.append(s3_file)
        
        if self.verbosity > 0:
            print "Found %d changes" % len(changed_files)
        
        # the list of files that are out-of-sync in the local repository. 
        # if one or more files is out-of-sync, the upload is halted to prevent
        # uncommitted changes from being uploaded.
        outofsync_files = []
  
        for i, file in enumerate(changed_files):
            status_list = client.status(file.filename)
            if status_list:
                file_status = status_list[0]
                if file_status.text_status not in (pysvn.wc_status_kind.normal,):
                    outofsync_files.append((file, file_status,))
                    
        if outofsync_files:
            print "Unable to upload changes to S3. The local repository is out-of-sync. " \
                "Please checkin the following changes:"
            
            for file, file_status in outofsync_files:
                print "\t%s (status: %s)" % (file.filename, file_status.text_status)
            sys.exit(1)
        
        if not changed_files:
            sys.exit(0)
        
        # load the queue
        [ get_queue().put(s3_file) for s3_file in changed_files ]
      
        # build the args for the process workers
        process_args = (
            settings.AWS_BUCKET_NAME,                        
            settings.AWS_ACCESS_KEY_ID,
            settings.AWS_SECRET_ACCESS_KEY,
            self.verbosity,
            options.get('prefix'),
            options.get('gzip'),
            options.get('expires'),
            True, # run with force 
            self.dryrun,
        )        
        
        processes = [ Process(target=S3UploadWorker(num, *process_args)) \
                                for num in xrange(processes_count) ]
        
        # fire off workers
        for s3_worker in processes:
            s3_worker.start()

        # poll the workers until they are all finished
        while True:
            some_alive = any([ worker.is_alive() for worker in processes ])

            if some_alive:
                time.sleep(0.1)
            else:
                process_result = sum([ abs(worker.exitcode) for worker in processes ])    
                if not process_result:
                    # all completed successfully
                    if self.verbosity > 0:
                        print "Updating S3 revision number to %d" % local_repo_info.revision.number
                        
                    # store the current repo number
                    s3_svn_revision['revision'] = local_repo_info.revision.number
                    s3_svn_revision['last_update'] = datetime.now().ctime()
                    self.set_s3_revision(s3_svn_revision)
                        
                    if self.verbosity > 0:
                        print "Finished (exit code: %d)" % process_result
                    break
                
                else:
                    sys.exit(process_result)

    
    def get_s3_svn_bucket(self):
        """
        Looks up the svn s3 configuration bucket instance.  If the bucket
        does not exist, it will be created.
        """
        if self.verbosity > 1:
            print "Connecting to s3"
            
        # open s3 connection to retrieve the current revision 
        conn = boto.connect_s3(settings.AWS_ACCESS_KEY_ID, settings.AWS_SECRET_ACCESS_KEY)
        
        # the s3 svn configuration bucket
        svn_bucket_name = '.'.join([settings.AWS_BUCKET_NAME, 'svn'])
        
        try:
            svn_bucket = conn.get_bucket(svn_bucket_name)
        except boto.exception.S3ResponseError:
            svn_bucket = conn.create_bucket(svn_bucket_name)
        
        return svn_bucket
      
    def get_s3_svn_revision(self):
        """ 
        Retrieves the SVN revision number for the S3 conf bucket.
        """
        # the s3 revision conf
        s3_revision = dict(url='', revision=INITIAL_REVISION, last_update=None, uuid=None)
        svn_bucket = self.get_s3_svn_bucket()
        svn_key = svn_bucket.get_key(self.SVN_REVISION_CONF)
        
        if not svn_key:
            self.set_s3_revision(s3_revision, svn_bucket)
        else:
            s3_data = svn_key.read()
            s3_revision_yaml = yaml.load(s3_data)
            s3_revision.update(s3_revision_yaml)
                
        return s3_revision
    
    def set_s3_revision(self, s3_revision, bucket_instance=None):
        """
        Stores the svn revision number in the S3 conf bucket
        """
        svn_bucket = bucket_instance or self.get_s3_svn_bucket() 
        svn_key = svn_bucket.get_key(self.SVN_REVISION_CONF)
        
        if not svn_key:
            svn_key = boto.s3.key.Key(svn_bucket)
            svn_key.name = self.SVN_REVISION_CONF
            if self.verbosity > 0:
                print "Creating S3 SVN config file %s in bucket %s" % (svn_key.name, svn_bucket.name)

        yaml_data = yaml.dump(s3_revision, default_flow_style=False)
        if not self.dryrun:
            svn_key.set_contents_from_string(yaml_data)
        if self.verbosity > 1:
            print "Stored S3 SVN revision %s" % s3_revision['revision']
