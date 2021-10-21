''' Upload files to, download objects from, or remove objects from Amazon AWS S3
'''

import mimetypes
import os
from pathlib import Path
import sys
from time import sleep, time
from typing import Sequence, Optional, Tuple, List
import boto3
import click
import colorlog
from dask import bag
from dask.diagnostics import ProgressBar
import s3fs

__version__ = '1.2.0'
# pylint: disable=broad-except, too-many-arguments, too-many-locals

TEMPLATE = "An exception of type {0} occurred. Arguments:\n{1!r}"


def check_parms(source_paths, order, bucket, download, delete, cloud, basedir,
                version_tag, stage_tag, developer_tag, project_tag,
                description_tag, logger):
    ''' Check input parameters
        Keyword arguments:
          source_paths: local source path
          order_file: order file
          bucket: S3 bucket
          download: download flag
          delete: delete flag
          cloud: cloud flag
          basedir: local base directory
          version_tag: S3 tag for project version
          stage_tag: S3 tag for project stage
          developer_tag: S3 tag for project developer
          project_tag: S3 tag for project name
          description_tag: S3 tag for project description
          logger: logging instance
        Returns:
          None
    '''
    msg = ''
    if not source_paths and not order:
        msg = "You must specify an order file or source path(s)"
    if download:
        if not basedir:
            msg = "You must specify a local base directory"
        if version_tag or stage_tag or developer_tag or project_tag or description_tag:
            msg = "Tags may only be used when uploading files"
    elif delete and not order:
        msg = "You must specify an order file with --delete"
    elif cloud and not order:
        msg = "You must specify an order file with --cloud"
    else:
        if not order and not bucket:
            msg = "You must specify an order file or a source path/bucket"
    if msg:
        logger.error(msg)
        sys.exit(-1)


def humansize(num: int, suffix='B') -> str:
    ''' Return a human-readable storage size
        Keyword arguments:
          num: size
          suffix: default suffix
        Returns:
          string
    '''
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'P', suffix)


def download_single_file(sfs, source, dest):
    ''' Download a single file
        Keyword arguments:
          sfs: S3FS connection
          source: S3 bucket/key
          dest: destination path
        Returns:
          None
    '''
    try:
        sfs.download(source, dest, True)
    except Exception as err:
        print(TEMPLATE.format(type(err).__name__, err.args))
        print("Could not get %s" % source)
        sys.exit(-1)


def remove_single_file(sfs, source):
    ''' Remove a single file
        Keyword arguments:
          sfs: S3FS connection
          source: S3 bucket/key
        Returns:
          None
    '''
    try:
        sfs.rm(source, False)
    except Exception as err:
        print(TEMPLATE.format(type(err).__name__, err.args))
        print("Could not remove %s" % source)
        sys.exit(-1)


def upload_single_file(sfs, source, dest, tag=None):
    ''' Upload a single file, set MIME type, and apply optional tags
        Keyword arguments:
          sfs: S3FS connection
          source: source file
          dest: S3 bucket/key
          tag: tags [optional]
        Returns:
          None
    '''
    try:
        sfs.put(source, dest)
    except Exception as err:
        print(TEMPLATE.format(type(err).__name__, err.args))
        print("Could not put %s" % dest)
        sys.exit(-1)
    added = False
    tries = 8
    mimetype = mimetypes.guess_type(source)[0]
    if not mimetype:
        mimetype = 'binary/octet-stream'
    while added is False and tries:
        try:
            sfs.setxattr(dest, copy_kwargs={'ContentType': mimetype})
            added = True
        except Exception as err:
            tries -= 1
            sleep(4)
    if not added:
        if mimetype != 'binary/octet-stream':
            print("Could not setxattr to %s for %s" % (mimetype, dest))
        return
    if not tag:
        return
    try:
        sfs.put_tags(dest, tag)
    except Exception as err:
        print(TEMPLATE.format(type(err).__name__, err.args))
        print("Could not put_tags %s" % dest)
        sys.exit(-1)


def copy_single_file(s3r, source, dest):
    ''' Copy a single file
        Keyword arguments:
          s3r: S3 resource
          source: source file
          dest: S3 bucket/key
        Returns:
          None
    '''
    bucket, from_key = source.split("/", 1)
    copy_source = {'Bucket': bucket,
                   'Key': from_key}
    bucket, to_key = dest.split("/", 1)
    try:
        bucket = s3r.Bucket(bucket)
        bucket.copy(copy_source, to_key)
    except Exception as err:
        print(TEMPLATE.format(type(err).__name__, err.args))
        print("Could not put %s" % dest)
        sys.exit(-1)


def connect_s3(profile):
    ''' Connect an S3 filesystem
        Keyword arguments:
          profile: user profile [optional]
        Returns:
          S3FS connection
    '''
    try:
        if profile:
            sfs = s3fs.S3FileSystem(anon=False, profile=profile)
        else:
            sfs = s3fs.S3FileSystem(anon=False)
    except Exception as err:
        print(TEMPLATE.format(type(err).__name__, err.args))
        sys.exit(-1)
    return sfs


def connect_s3_boto(profile):
    ''' Connect an S3 filesystem
        Keyword arguments:
          profile: user profile [optional]
        Returns:
          S3 resource
    '''
    try:
        if profile:
            session = boto3.session.Session(profile_name=profile)
        else:
            session = boto3.session.Session()
        s3r = session.resource('s3')
    except Exception as err:
        print(TEMPLATE.format(type(err).__name__, err.args))
        sys.exit(-1)
    return s3r


def iterget(sources: Sequence[str], dests: Sequence[str], profile):
    ''' Given a sequence of sources and dests, download each source to dest
        Keyword arguments:
          sources: sources
          dests: destinations
          profile: user profile [optional]
        Returns:
          True
    '''
    sfs = connect_s3(profile)
    for source, dest in zip(sources, dests):
        download_single_file(sfs, source, dest)
    return True


def iterrm(sources: Sequence[str], profile):
    ''' Given a sequence of sources, remove each source object
        Keyword arguments:
          sources: sources
          profile: user profile [optional]
        Returns:
          True
    '''
    sfs = connect_s3(profile)
    for source in sources:
        remove_single_file(sfs, source)
    return True


def iterput(sources: Sequence[str], dests: Sequence[str], tags: Sequence[Optional[dict]], profile):
    ''' Given a sequence of sources, dests, and tags, save each source to dest with a tag
        Keyword arguments:
          sources: sources
          dests: destinations
          tags: S3 tags [optional]
          profile: user profile [optional]
        Returns:
          True
    '''
    sfs = connect_s3(profile)
    if tags:
        for source, dest, tag in zip(sources, dests, tags):
            upload_single_file(sfs, source, dest, tag)
    else:
        for source, dest in zip(sources, dests):
            upload_single_file(sfs, source, dest)
    return True


def itercloud(sources: Sequence[str], dests: Sequence[str], profile):
    ''' Given a sequence of sources, dests, and tags, save each source to dest with a tag
        Keyword arguments:
          sources: sources
          dests: destinations
          profile: user profile [optional]
        Returns:
          True
    '''
    s3r = connect_s3_boto(profile)
    for source, dest in zip(sources, dests):
        copy_single_file(s3r, source, dest)
    return True


def walk_s3_path(s3_path, profile):
    ''' Recursively walk a path in S3 to return objects
        Keyword arguments:
          s3_path: path on S3 to walk
          profile: user profile [optional]
        Returns:
          source_objs: list of objects
          osize: total size of objects
    '''
    source_objs = []
    sfs = connect_s3(profile)
    objs = sfs.find(s3_path, detail=True)
    osize = 0
    for obj in objs:
        source_objs.append(obj)
        osize = objs[obj]['size']
    return source_objs, osize


def check_columns(line, require):
    ''' Check a line for the required number of columns
        Keyword arguments:
          line: line from file
          require: required number of columns
        Returns:
          True or False
    '''
    columns = len(line.split("\t"))
    if columns != require:
        print("Order file requires %d column(s), but has %d" % (require, columns))
        return False
    return True


def s3get(source_paths: str, order_file: str, basedir: str, dryrun: bool,
          profile: Optional[str] = None, **kwargs):
    ''' Find objects to download from S3
        Keyword arguments:
          source_paths: local source path
          order_file: order file
          basedir: local base directory
          dryrun: do not upload
          profile: user profile [optional]
        Returns:
          Dask bag
    '''
    sources = []
    dests = []
    total_size = 0
    if order_file:
        order = open(order_file, "r")
        checked = False
        for src in order.readlines():
            src = src.rstrip()
            if not checked:
                if not check_columns(src, 1):
                    sys.exit(-1)
                checked = True
            sources.append(src)
            dests.append(src.replace(src.split('/')[0], basedir))
        order.close()
    else:
        for source_path in source_paths:
            objects, osize = walk_s3_path(source_path, profile)
            for obj in objects:
                sources.append(obj)
                dests.append(obj.replace(obj.split('/')[0], basedir))
                total_size += osize
    print("Files selected: %d" % (len(sources)))
    if not order_file:
        print("Size: %s" % humansize(total_size))
    if dryrun:
        return None, len(sources), total_size
    source_bag = bag.from_sequence(sources)
    dest_bag = bag.from_sequence(dests)
    return bag.map_partitions(iterget, source_bag, dest_bag, profile), \
                              len(sources), total_size


def s3rm(order_file: str, dryrun: bool,
         profile: Optional[str] = None, **kwargs):
    ''' Find objects to remove from S3
        Keyword arguments:
          order_file: order file
          dryrun: do not upload
          profile: user profile [optional]
        Returns:
          Dask bag
    '''
    sources = []
    total_size = 0
    checked = False
    order = open(order_file, "r")
    for src in order.readlines():
        src = src.rstrip()
        if not checked:
            if not check_columns(src, 1):
                sys.exit(-1)
            checked = True
        sources.append(src)
    order.close()
    print("Files selected: %d" % (len(sources)))
    if dryrun:
        return None, len(sources), total_size
    source_bag = bag.from_sequence(sources)
    return bag.map_partitions(iterrm, source_bag, profile), \
                              len(sources), total_size


def fwalk(source: str, endswith='') -> Tuple[List[str], int, int]:
    ''' Use os.walk to recursively parse a directory tree, returning
        a list containing the full paths to all files with filenames
        ending with `endswith`.
        Keyword arguments:
          source: source path
          endswith: ending string [optional]
        Returns:
          results: list of source paths
          considered: number of files walked
          total_size: number of files in list
    '''
    results = []
    considered = total_size = 0
    for wpath, _, files in os.walk(source):
        for file in files:
            considered += 1
            if file.endswith(endswith):
                fpath = os.path.join(wpath, file)
                total_size += os.stat(fpath).st_size
                results.append(fpath)
    return results, considered, total_size


def s3put(dest_root: str, source_path: str, dryrun: bool, profile: Optional[str] = None,
          endswith: Optional[str] = '', tags: Optional[dict] = None, **kwargs):
    ''' Find local files to upload to S3 by walking source paths
        Keyword arguments:
          dest_root: S3 destination root
          source_path: local source path
          dryrun: do not upload
          profile: user profile [optional]
          endswith: ending string [optional]
          tags: S3 tags [optional]
        Returns:
          Dask bag
    '''
    sources, considered, total_size = fwalk(source_path, endswith)
    if dryrun:
        for fpath in sources:
            print(fpath)
    print("Files selected: %d/%d" % (len(sources), considered))
    print("Size: %s" % humansize(total_size))
    if dryrun:
        return None, len(sources), total_size
    dests = tuple(dest_root / Path(f).relative_to(source_path) for f in sources)
    source_bag = bag.from_sequence(sources)
    dest_bag = bag.from_sequence(dests)
    tag_bag = bag.from_sequence((tags,) * len(sources)) if tags else None
    return bag.map_partitions(iterput, source_bag, dest_bag, tag_bag, profile), \
                              len(sources), total_size


def s3put_order(order_file: str, dryrun: bool, cloud: bool, profile: Optional[str] = None,
                tags: Optional[dict] = None, **kwargs):
    ''' Find local files to upload to S3 from an order file
        Keyword arguments:
          order_file: order file
          basedir: local base directory
          dryrun: do not upload
          profile: user profile [optional]
          tags: S3 tags [optional]
        Returns:
          Dask bag
    '''
    sources = []
    dests = []
    checked = False
    order = open(order_file, "r")
    total_size = 0
    for line in order.readlines():
        line = line.rstrip()
        if not checked:
            if not check_columns(line, 2):
                sys.exit(-1)
            checked = True
        src, dst = line.split("\t")
        if not cloud:
            total_size += os.stat(src).st_size
        sources.append(src)
        dests.append(dst)
    order.close()
    print("Files selected: %d" % (len(sources)))
    if not cloud:
        print("Size: %s" % humansize(total_size))
    if dryrun:
        return None, len(sources), total_size
    source_bag = bag.from_sequence(sources)
    dest_bag = bag.from_sequence(dests)
    tag_bag = bag.from_sequence((tags,) * len(sources)) if tags else None
    if cloud:
        return bag.map_partitions(itercloud, source_bag, dest_bag, profile), \
                                  len(sources), total_size
    return bag.map_partitions(iterput, source_bag, dest_bag, tag_bag, profile), \
                              len(sources), total_size


def execute_transfers(result, source_count, source_size, workers, source_paths, order):
    ''' Interface with AWS S3 to upload/download files
        Keyword arguments:
          result: Dask bag
          source_count: number of source objects
          source_size: size of source objects
          workers: number of workers
          source_paths: local source path
          order: order file
        Returns:
          None
    '''

    total = {'count': source_count, 'size': source_size, 'time': 0}
    if result:
        start_time = time()
        result.compute(scheduler='processes', num_workers=workers)
        total['time'] = time() - start_time
    # Transfers complete
    if len(source_paths) >= 1 or order:
        print_stats(total)


def print_stats(total):
    ''' Print statistics
        Keyword arguments:
          total: totals dictionary
        Returns:
          None
    '''
    print("Total files: %d" % total['count'])
    if total['size']:
        print("Total size: %s" % humansize(total['size']))
        print("Data transfer rate: %.2f MB/sec"
              % (total['size'] / total['time'] / (1024 * 1024)))


@click.command()
@click.argument('source_paths', required=False, nargs=-1)
@click.option('-of', '--order', type=str)
@click.option('-b', '--bucket', required=False, type=str)
@click.option('-w', '--workers', default=12, type=int)
@click.option('-dl', '--download', default=False, is_flag=True)
@click.option('-delete', '--delete', default=False, is_flag=True)
@click.option('-cl', '--cloud', default=False, is_flag=True)
@click.option('-base', '--basedir', type=str)
@click.option('-ew', '--endswith', default='', type=str)
@click.option('-vt', '--version-tag', default=None, type=str)
@click.option('-st', '--stage-tag', default=None, type=click.Choice(['dev', 'prod', 'val']))
@click.option('-dvt', '--developer-tag', default=None, type=str)
@click.option('-pt', '--project-tag', default=None, type=str)
@click.option('-dt', '--description-tag', default=None, type=str)
@click.option('-pr', '--profile', default=None, type=str)
@click.option('-dr', '--dryrun', default=False, is_flag=True)
@click.option('-vb', '--verbose', default=False, is_flag=True)
@click.option('-db', '--debug', default=False, is_flag=True)


def s3_cli(source_paths, order, bucket, workers, download, delete, cloud, basedir, endswith,
           version_tag, stage_tag, developer_tag, project_tag, description_tag,
           profile, dryrun, verbose, debug):
    ''' Interface with AWS S3 to upload/download files
        Keyword arguments:
          source_paths: local source path
          order: order file
          bucket: S3 bucket
          workers: number of workers [12]
          download: download flag
          delete: delete flag
          cloud: cloud-to-cloud transfer
          basedir: local base directory
          endswith: ending string [optional]
          version_tag: S3 tag for project version
          stage_tag: S3 tag for project stage
          developer_tag: S3 tag for project developer
          project_tag: S3 tag for project name
          description_tag: S3 tag for project description
          profile: user profile [optional]
          dryrun: do not upload
          verbose: verbose mode (program is chatty)
          debug: debug mode (program is very chatty)
        Returns:
          None
    '''
    mimetypes.init()
    logger = colorlog.getLogger()
    if debug:
        logger.setLevel(colorlog.colorlog.logging.DEBUG)
        verbose = True
        os.environ['S3FS_LOGGING_LEVEL'] = "DEBUG"
    elif verbose:
        logger.setLevel(colorlog.colorlog.logging.INFO)
    else:
        logger.setLevel(colorlog.colorlog.logging.WARNING)
    handler = colorlog.StreamHandler()
    handler.setFormatter(colorlog.ColoredFormatter())
    logger.addHandler(handler)
    # Parameter checking
    check_parms(source_paths, order, bucket, download, delete, cloud, basedir,
                version_tag, stage_tag, developer_tag, project_tag, description_tag, logger)
    tags = dict()
    for tag in ['description', 'developer', 'project', 'stage', 'version']:
        if locals()[tag + '_tag']:
            tags[tag + '_tag'] = locals()[tag + '_tag']
    if download:
        result, source_count, source_size = s3get(source_paths, order, basedir=basedir,
                                                  profile=profile, dryrun=dryrun)
    elif delete:
        result, source_count, source_size = s3rm(order, profile=profile, dryrun=dryrun)
    elif (not download) and source_paths:
        for source_path in source_paths:
            if len(source_paths) > 1:
                print("Source " + source_path)
            dest_root = Path(bucket) / Path(source_path).stem
            result, source_count, source_size = s3put(dest_root, source_path, endswith=endswith,
                                                      tags=tags, profile=profile, dryrun=dryrun)
    else:
        result, source_count, source_size = s3put_order(order, tags=tags, profile=profile,
                                                        dryrun=dryrun, cloud=cloud)
    # Run the transfers
    execute_transfers(result, source_count, source_size, workers, source_paths, order)


if __name__ == '__main__':
    if not (sys.version_info[0] == 3 and sys.version_info[1] >= 6):
        print("This program requires at least Python 3.6")
        sys.exit(-1)
    PBAR = ProgressBar()
    PBAR.register()
    s3_cli() # pylint: disable=no-value-for-parameter
