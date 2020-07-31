import os
from pathlib import Path
import sys
from time import time
from typing import Sequence, Optional, Tuple, List
import click
import colorlog
from dask import bag
from dask.diagnostics import ProgressBar
import s3fs



STAGES = ('dev', 'prod', 'val')
TEMPLATE = "An exception of type {0} occurred. Arguments:\n{1!r}"

def fwalk(source: str, endswith='') -> Tuple[List[str], int, int]:
    """
    Use os.walk to recursively parse a directory tree, returning a list containing the full paths
    to all files with filenames ending with `endswith`.
    """
    results = []
    considered = total_size = 0
    for wpath, wdir, files in os.walk(source):
        for file in files:
            considered += 1
            if file.endswith(endswith):
                fpath = os.path.join(wpath, file)
                total_size += os.stat(fpath).st_size
                results.append(fpath)
    return results, considered, total_size


def humansize(num: int, suffix='B') -> str:
    ''' Convert a storage number to a human-readable string
    '''
    for unit in ['', 'K', 'M', 'G', 'T']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'P', suffix)


def iterput(sources: Sequence[str], dests: Sequence[str], tags: Sequence[Optional[dict]], profile):
    """
    Given a sequence of sources, dests, and tags, save each source to dest with a tag.
    """
    try:
        if profile:
            fs = s3fs.S3FileSystem(anon=False, profile=profile)
        else:
            fs = s3fs.S3FileSystem(anon=False)
    except Exception as err:
        print(TEMPLATE.format(type(err).__name__, err.args))
        sys.exit(-1)
    if tags:
        for source, dest, tag in zip(sources, dests, tags):
            try:
                fs.put(source, dest)
                fs.put_tags(dest, tag)
            except Exception as err:
                print(TEMPLATE.format(type(err).__name__, err.args))
                print("Could not process %s" % dest)
                sys.exit(-1)
    else:
        for source, dest in zip(sources, dests):
            try:
                fs.put(source, dest)
            except Exception as err:
                print(TEMPLATE.format(type(err).__name__, err.args))
                print("Could not process %s" % dest)
                sys.exit(-1)
    return True


def s3put(dest_root: str, source_path: str, dryrun: bool, profile: Optional[str] = None,
          endswith: Optional[str] = '', tags: Optional[dict] = None, **kwargs):
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


@click.command()
@click.argument('source_paths', required=True, nargs=-1)
@click.option('-b', '--bucket', required=True, type=str)
@click.option('-w', '--workers', default=8, type=int)
@click.option('-ew', '--endswith', default='', type=str)
@click.option('-vt', '--version-tag', default=None, type=str)
@click.option('-st', '--stage-tag', default=None, type=str)
@click.option('-dvt', '--developer-tag', default=None, type=str)
@click.option('-pt', '--project-tag', default=None, type=str)
@click.option('-dt', '--description-tag', default=None, type=str)
@click.option('-pr', '--profile', default=None, type=str)
@click.option('-dr', '--dryrun', default=False, is_flag=True)
@click.option('-vb', '--verbose', default=False, is_flag=True)
@click.option('-db', '--debug', default=False, is_flag=True)


def s3put_cli(source_paths, bucket, workers, endswith, version_tag, stage_tag,
              developer_tag, project_tag, description_tag, profile, dryrun, verbose, debug):
    LOGGER = colorlog.getLogger()
    if debug:
        LOGGER.setLevel(colorlog.colorlog.logging.DEBUG)
        verbose = True
        os.environ['S3FS_LOGGING_LEVEL'] = "DEBUG"
    elif verbose:
        LOGGER.setLevel(colorlog.colorlog.logging.INFO)
    else:
        LOGGER.setLevel(colorlog.colorlog.logging.WARNING)
    HANDLER = colorlog.StreamHandler()
    HANDLER.setFormatter(colorlog.ColoredFormatter())
    LOGGER.addHandler(HANDLER)

    total = {'count': 0, 'size': 0, 'time': 0}
    tags = dict()
    if stage_tag:
        assert stage_tag in STAGES
    for tag in ['description', 'developer', 'project', 'stage', 'version']:
        if locals()[tag + '_tag']:
            tags[tag + '_tag'] = locals()[tag + '_tag']
    for source_path in source_paths:
        if len(source_paths) > 1:
            print("Source " + source_path)
        dest_root = Path(bucket) / Path(source_path).stem
        result, source_count, source_size = s3put(dest_root, source_path, endswith=endswith,
                                                  tags=tags, profile=profile, dryrun=dryrun)
        total['count'] += source_count
        total['size'] += source_size
        if result:
            start_time = time()
            result.compute(scheduler='processes', num_workers=workers)
            elapsed_time = time() - start_time
            total['time'] += elapsed_time
    if len(source_paths) >= 1:
        print("Total files: %d" % total['count'])
        print("Total size: %s" % humansize(total['size']))
        if not dryrun:
            print("Data transfer rate: %.2f MB/sec"
                  % (total['size'] / total['time'] / (1024 * 1024)))

if __name__ == '__main__':
    if not (sys.version_info[0] == 3 and sys.version_info[1] >= 6):
        print("This program requires at least Python 3.6")
        sys.exit(-1)
    PBAR = ProgressBar()
    PBAR.register()
    s3put_cli()
