''' This program will allow listing of "paths" (buckets with added key) on AWS S3.
'''
__version__ = '1.0.0'

import argparse
import s3fs

def humansize(num, suffix='B'):
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


def group_procesing(group, group_key, fsize):
    ''' Group key information
        Keyword arguments:
          group: group dictionary
          group_key: group key
          fsize: object size
        Returns:
          None
    '''
    if group_key not in group:
        group[group_key] = dict()
    if 'count' not in group[group_key]:
        group[group_key]['count'] = 0
    group[group_key]['count'] += 1
    if 'size' not in group[group_key]:
        group[group_key]['size'] = 0
    group[group_key]['size'] += fsize


def list_path():
    ''' List keys
        Keyword arguments:
          None
        Returns:
          None
    '''
    afs = s3fs.S3FileSystem(anon=True)
    to_strip = ARG.PATH
    if to_strip[-1] != '/':
        to_strip = to_strip + '/'
    objs = afs.find(ARG.PATH, detail=True) if ARG.RECURSIVE else \
           afs.ls(ARG.PATH, detail=True)
    count = size = 0
    group = dict()
    for obj in objs:
        count += 1
        if ARG.GROUP:
            group_key = obj.replace(to_strip, '')
            group_key = group_key.split('/')[0]
        if ARG.RECURSIVE:
            key = obj if ARG.FULL else obj.replace(ARG.PATH + '/', '')
            fsize = objs[obj]['size']
            ftype = objs[obj]['type']
            if ARG.GROUP:
                group_procesing(group, group_key, fsize)
        else:
            key = obj['Key'] if ARG.FULL else obj['Key'].replace(ARG.PATH + '/', '')
            fsize = obj['size']
            ftype = obj['type']
        size += fsize
        if not ARG.GROUP:
            if ARG.DETAIL:
                print("\t".join([key, ftype, str(fsize)]))
            else:
                print(key)
    if ARG.GROUP:
        for key in sorted(group, key=str.lower):
            if group[key]['count'] <= 1:
                continue
            print("%s: %d keys, %s" % (key, group[key]['count'], humansize(group[key]['size'])))
    print("Total keys: %d" % count)
    print("Total size: %s" % humansize(size))


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="List paths on AWS S3")
    PARSER.add_argument('--path', dest='PATH', action='store',
                        default='janelia-flylight-imagery', help='bucket/key to list')
    PARSER.add_argument('--full', dest='FULL', action='store_true',
                        default=False, help='List with full key')
    PARSER.add_argument('--recursive', dest='RECURSIVE', action='store_true',
                        default=False, help='List recursively')
    PARSER.add_argument('--group', dest='GROUP', action='store_true',
                        default=False, help='Group keys')
    PARSER.add_argument('--detail', dest='DETAIL', action='store_true',
                        default=False, help='List with details')
    ARG = PARSER.parse_args()
    if ARG.GROUP:
        ARG.RECURSIVE = True
    list_path()
