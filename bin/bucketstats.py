''' Show bucket size and count metrics for a digen region and profile
'''
import argparse
import datetime
import boto3

METRICLIST = [('BucketSizeBytes', 'StandardStorage'),
              ('NumberOfObjects', 'AllStorageTypes'),
             ]

def humansize(num, suffix='B'):
    ''' Return a human-readable storage size
        Keyword arguments:
          num: size
          suffix: default suffix
        Returns:
          string
    '''
    for unit in ['', 'K', 'M', 'G', 'T']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'P', suffix)


def cloudwatch(region, cws=dict()):
    ''' Return a Cloudwatch accessor for a region
        Keyword arguments:
          region: AWS region
          cws: Cloudwatch dict
        Returns:
          Cloudwatch accessor
    '''
    if region not in cws:
        cws[region] = boto3.client('cloudwatch', region_name=region)
    return cws[region]


def bucket_stats(name, date):
    ''' Get the bucket size and object count for a given date
        Keyword arguments:
          name: bucket name
          date: end date/time
        Returns:
          results dict
    '''
    results = {}
    for metric_name, storage_type in METRICLIST:
        metrics = cloudwatch(ARG.REGION).get_metric_statistics(
            Namespace='AWS/S3',
            MetricName=metric_name,
            StartTime=date - datetime.timedelta(days=1),
            EndTime=date,
            Period=86400,
            Statistics=[ARG.METRIC],
            Dimensions=[{'Name': 'BucketName', 'Value': name},
                        {'Name': 'StorageType', 'Value': storage_type}],
        )
        if metrics['Datapoints']:
            results[metric_name] = sorted(metrics['Datapoints'],
                                          key=lambda row: row['Timestamp'])[-1][ARG.METRIC]
            continue
    return results


def process_buckets():
    ''' Display metrics for all buckets
        Keyword arguments:
          None
        Returns:
          None
    '''
    midnight = datetime.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    s3r = boto3.resource('s3')
    total = {"size": 0, "count": 0}
    for bucket in s3r.buckets.all():
        results = bucket_stats(bucket.name, midnight)
        total['size'] += int(results.get('BucketSizeBytes', 0))
        total['count'] += int(results.get('NumberOfObjects', 0))
        print(bucket.name, humansize(int(results.get('BucketSizeBytes', 0))),
              int(results.get('NumberOfObjects', 0)), sep='\t')
    print('TOTAL', humansize(total['size']), total['count'], sep='\t')


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Get bucket stats for a profile")
    PARSER.add_argument('--region', dest='REGION', action='store',
                        default='us-east-1', help='AWS region [us-east-1]')
    PARSER.add_argument('--metric', dest='METRIC', action='store',
                        default='Maximum', help='S3 metric [Maximum]')
    PARSER.add_argument('--profile', dest='PROFILE', action='store',
                        help='Optional profile')
    ARG = PARSER.parse_args()

    print('name', 'bytes', 'objects', sep='\t')
    if ARG.PROFILE:
        boto3.setup_default_session(profile_name=ARG.PROFILE)
    process_buckets()
