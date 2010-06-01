Django Amazon S3 Management Commands
====================================

- S3 Push: for pushing MEDIA_ROOT content to S3
- S3 SVN Sync: for synchronzing an S3 Bucket with your SVN repository 

How to install it
-----------------

### Dependancies

- boto
- pysvn
- yaml
- Python 2.6+
- Django 1.1

### Setup

Put the `pamazons3` folder in your python path or Django project folder. Then add to your settings file:

    # in settings.py
    INSTALLED_APPS = (
        ...
        'pamazons3',
        ...
    )

Also, add the following new items to settings.py:
     
    AWS_ACCESS_KEY_ID = 'YourS3AccessID'
    AWS_SECRET_ACCESS_KEY = 'YourS3SecretKey'
    AWS_BUCKET_NAME = 'YourAmazonS3BucketName'

How to use it
-------------

pamazons3 will allow you to push content (and push changes based on mod time) and/or synchronize your media
with your subversion repository.

The available commands are:

- s3-push
- s3-svnsync

### s3-push

The command options are:

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

### s3-svnsync

The command options are:

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


