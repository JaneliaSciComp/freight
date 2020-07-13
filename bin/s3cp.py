import s3fs
from dask import bag
import click 
from pathlib import Path
from dask.diagnostics import ProgressBar
from typing import Sequence, Optional, Tuple, List
import os 
from time import time

STAGES = ('dev', 'prod', 'val')

def fwalk(source: str, endswith='') -> Tuple[List[str], int, int]:
    """
    Use os.walk to recursively parse a directory tree, returning a list containing the full paths
    to all files with filenames ending with `endswith`.
    """
    results = []
    considered = total_size = 0
    for p, d, f in os.walk(source):
        for file in f:
            considered += 1
            if file.endswith(endswith):
                fpath = os.path.join(p, file)
                total_size += os.stat(fpath).st_size
                results.append(fpath)
    return results, considered, total_size

def humansize(num: int, suffix='B') -> str:
    for unit in ['','K','M','G','T']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'P', suffix)

def iterput(sources: Sequence[str], dests: Sequence[str], tags: Sequence[Optional[dict]], profile: Optional[str]=None):
    """
    Given a sequence of sources, dests, and tags, save each source to dest with a tag.    
    """           
    fs = s3fs.S3FileSystem(profile=profile)
    for source, dest, tag in zip(sources, dests, tags):
        fs.put(source, dest)
        if tag is not None:
            fs.put_tags(dest, tag)
    return True        

def s3put(dest_root: str, source_path: str, dryrun: bool, endswith: Optional[str]='', profile=None, tags: Optional[dict]=None, **kwargs):
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
    tag_bag = bag.from_sequence((tags,) * len(sources))

    return bag.map_partitions(iterput, source_bag, dest_bag, tag_bag), len(sources), total_size

@click.command()
@click.argument('source_paths', required=True, nargs=-1)
@click.option('-b', '--bucket', required=True, type=str)
@click.option('-w', '--workers', default=8, type=int)
@click.option('-ew', '--endswith', default='', type=str)
@click.option('-vt', '--version-tag', required=True, type=str)
@click.option('-st', '--stage-tag', required=True, type=str)
@click.option('-dvt', '--developer-tag', required=True, type=str)
@click.option('-pt', '--project-tag', required=True, type=str)
@click.option('-dt', '--description-tag', required=True, type=str)
@click.option('-dr', '--dryrun', default=False, is_flag=True)
def s3put_cli(source_paths, bucket, workers, endswith, version_tag, stage_tag, developer_tag, project_tag, description_tag, dryrun):
    total = {'count': 0, 'size': 0, 'time': 0}
    for source_path in source_paths:
        if len(source_paths) > 1:
            print("Source " + source_path)
        dest_root = Path(bucket) / Path(source_path).stem 
        assert stage_tag in STAGES
        tags = {'VERSION': version_tag, 
                'DEVELOPER': developer_tag, 
                'STAGE': stage_tag, 
                'PROJECT': project_tag, 
                'DESCRIPTION': description_tag}
        result, source_count, source_size = s3put(dest_root, source_path, endswith=endswith, tags=tags, dryrun=dryrun)
        total['count'] += source_count
        total['size'] += source_size
        if result:
            start_time = time()
            result.compute(scheduler='processes', num_workers=workers)
            elapsed_time = time() - start_time
            total['time'] += elapsed_time
    if len(source_paths) > 1:
        print("Total files: %d" % total['count'])
        print("Total size: %s" % humansize(total['size']))
        if not dryrun:
            print("Data transfer rate: %.2f MB/sec" % (total['size'] / total['time'] / (1024 * 1024)))

if __name__ == '__main__':
    pbar = ProgressBar()
    pbar.register()
    s3put_cli()
