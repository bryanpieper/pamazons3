"""
Amazon S3 Django command support. 

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

import os
import datetime, time
import gzip
import email
import mimetypes
import hashlib
import optparse

from cStringIO import StringIO
from multiprocessing import Queue

# amazon s3 boto library
import boto

# default optparse list
DEFAULT_OPTIONS = (
    optparse.make_option('--gzip',
        action='store_true', dest='gzip', 
        help="Enables gzipping CSS and Javascript files."),
    optparse.make_option('--expires',
        action='store_true', dest='expires', 
        help="Enables setting a far future expires header."),
    optparse.make_option('--dryrun', '--dry-run',
        action='store_true', dest='dryrun', 
        help="Only print out actions instead of uploading files."),
    optparse.make_option('--workers',
        dest='processes', default=4, type='int',
        help="Specifies the number of worker processes to use to upload the files."),
    optparse.make_option('--verbose', '--v', dest='verbose',
        help="Specifies the basic output.", action="store_true"),
    optparse.make_option('--debug', '--V', '--d',
        dest='debug', action='store_true',
        help="Specifies the maximum debug output."),
)

# global FIFO queue 
s3_queue = Queue()

def get_queue():
    """
    The S3 file queue used by the worker processes.
    """
    global s3_queue
    return s3_queue


class S3File(object):
    """
    Contains the file information and S3 file key and the action.
    """
    def __init__(self, bucket_name, file_key, filename, delete=False):
        self.bucket_name = bucket_name
        self.file_key = file_key
        self.filename = filename
        self.delete = delete

    def do_delete(self):
        return self.delete
    
    def do_upload(self):
        return not self.delete
    
    def __str__(self):
        delete_str = self.delete and 'Delete' or '' 
        return '<S3File%s %s => %s>' % (delete_str, self.file_key, self.filename)



class S3UploadWorker(object):
    """
    Creates a process worker for the AWS S3 connection.
    """
    GZIP_CONTENT_TYPES = (
        'text/css',
        'application/javascript',
        'application/x-javascript',
    )    

    def __init__(self,
                 num,
                 aws_bucket, 
                 aws_access_key_id, 
                 aws_secret_key,
                 verbosity=0,
                 prefix='',
                 do_gzip=False,
                 do_expires=False,
                 do_force=False,
                 dry_run=False):
        self.num = num
        self.aws_bucket = aws_bucket
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_key = aws_secret_key
        self.verbosity = verbosity
        self.do_gzip = do_gzip
        self.do_expires = do_expires
        self.do_force = do_force
        self.dry_run = dry_run
        
        if self.verbosity > 1:
            print "S3ProcessWorker init (worker: %d)" % self.num
        
        # open s3 connection
        conn = boto.connect_s3(self.aws_access_key_id, self.aws_secret_key)
        try:
            self.bucket = conn.get_bucket(self.aws_bucket)
        except boto.exception.S3ResponseError:
            self.bucket = conn.create_bucket(self.aws_bucket)
            
        self.key = boto.s3.key.Key(self.bucket)
        self.upload_count = 0
        self.skip_count = 0
        self.delete_count = 0
        
        if self.verbosity > 0:
            print "Connected to s3 (worker: %d)" % self.num

    def compress_string(self, s):
        """
        Gzips a given string.
        """
        zbuf = StringIO()
        zfile = gzip.GzipFile(mode='wb', compresslevel=6, fileobj=zbuf)
        zfile.write(s)
        zfile.close()
        return zbuf.getvalue()

    def delete_s3(self, s3_file):
        """
        Handles the s3 delete processing of the given file
        """
        if self.verbosity > 0:
            print "Deleting file key %s (worker: %d)" % (s3_file.file_key, self.num)
        if not self.dry_run:
            self.bucket.delete_key(s3_file.file_key)
            
            # delete the gzipped file also if present
            gz_filename, gz_ext = os.path.splitext(s3_file.file_key)
            gz_file_key = ''.join([gz_filename, '.gz', gz_ext])
            if self.bucket.get_key(gz_file_key):
                if self.verbosity > 0:
                    print "Deleting gzip file key %s (worker: %d)" % (gz_file_key, self.num)
                self.bucket.delete_key(gz_file_key)
        self.delete_count += 1

    def upload_s3(self, s3_file):
        """
        Handles the s3 upload processing of the given file 
        """
        file_key = s3_file.file_key
        filename = s3_file.filename
        headers = {}
      
        content_type = mimetypes.guess_type(filename)[0]
        if content_type:
            headers['Content-Type'] = content_type
        
        # Check if file on S3 is older than local file, if so, upload
        if not self.do_force:
            s3_key = self.bucket.get_key(file_key)
            if s3_key:
                s3_datetime = datetime.datetime(*time.strptime(
                    s3_key.last_modified, "%a, %d %b %Y %H:%M:%S %Z")[0:6])
                local_datetime = datetime.datetime.utcfromtimestamp(
                    os.stat(filename).st_mtime)
                if local_datetime < s3_datetime:
                    self.skip_count += 1
                    if self.verbosity > 1:
                        print "File %s hasn't changed since last uploade" % file_key
                    return

        if self.verbosity > 0:
            print "Uploading %s (worker: %d)" % (file_key, self.num)
        
        file_obj = open(filename, 'rb')
        filedata = file_obj.read()
        file_size = os.fstat(file_obj.fileno()).st_size
                                
        if self.do_expires:
            # HTTP/1.0
            headers['Expires'] = "%s GMT" % (email.Utils.formatdate(
                time.mktime((datetime.datetime.now() +
                datetime.timedelta(days=365*2)).timetuple())))
            
            # HTTP/1.1
            headers['Cache-Control'] = "max-age %d" % (3600 * 24 * 365 * 2)
            if self.verbosity > 1:
                print "\texpires: %s" % (headers['Expires'])
                print "\tcache-control: %s" % (headers['Cache-Control'])

        try:
            if not self.dry_run:
                self.key.name = file_key
                self.key.set_contents_from_string(filedata, headers, replace=True)
                self.key.make_public()
                
            if self.do_gzip and not self.dry_run:
                
                # Gzipping only if file is large enough (>1K recommended) 
                if file_size > 1024 and content_type in self.GZIP_CONTENT_TYPES:
                    headers['Content-Encoding'] = 'gzip'
                    gzip_filedata = self.compress_string(filedata)
                    gz_filename, gz_ext = os.path.splitext(file_key)
                    
                    self.key.name = ''.join([gz_filename, '.gz', gz_ext])
                    self.key.set_contents_from_string(gzip_filedata, headers, replace=True)
                    self.key.make_public()
                    if self.verbosity > 1:
                        print "\tgzipped: %dk to %dk" % (file_size / 1024, len(gzip_filedata) / 1024)
                        
                elif self.verbosity > 0 and file_size < 1024 and content_type in self.GZIP_CONTENT_TYPES:
                    print "Skipping gzip on %s, less than 1k" % file_key
                
        except boto.s3.connection.S3CreateError, e:
            print "Failed: %s" % e
        except Exception, e:
            print e
            raise
        else:
            self.upload_count += 1
        
        if file_obj:
            file_obj.close()
    
    def run(self):
        """ 
        Runs the worker process. Uses the queue to find the next
        file to work on.
        """
        while True:
            if get_queue().empty():
                break
            try:
                s3_file = get_queue().get()
            except:
                break
            else:
                if s3_file.do_delete():
                    self.delete_s3(s3_file)
                else:
                    self.upload_s3(s3_file)
            
        if self.verbosity > 0:
            print "Finished processing files (worker: %d)" % self.num
            
        if self.verbosity > 1:
            if self.upload_count:
                print "\tUploaded %d files" % self.upload_count
            if self.skip_count:
                print "\tSkipped %d files" % self.skip_count
            if self.delete_count:
                print "\tDeleted %d files" % self.delete_count


# the worker process hook
S3UploadWorker.__call__ = S3UploadWorker.run
        
