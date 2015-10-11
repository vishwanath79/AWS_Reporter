# author vishwanath subramanian
import boto
import boto.ec2
import boto.rds
import boto.emr
import boto.redshift
from collections import Counter
import boto.ec2.cloudwatch
import datetime

ec2 = boto.connect_ec2()
rds = boto.connect_rds2()
s3 = boto.connect_s3()
emr = boto.connect_emr()
rs = boto.connect_redshift()
cw = boto.ec2.cloudwatch.connect_to_region('us-west-1')

all_running_clusters = []
all_ins_names = []
today = datetime.datetime.now()
yesterday = today - datetime.timedelta(days=1)


def getKey(item):
    return item[1]


# Get all the EC2 instances running
def all_instances():
    reservations = ec2.get_all_instances(filters={'instance-state-code': '16'})
    print '\nALL RUNNING EC2 INSTANCES'
    print '-------------------------------------------'
    allins = []
    countins = []
    i = 0
    for a in reservations:
        for b in a.instances:
            allins1 = b.instance_type, b.tags.get("cluster")
            allins.append(allins1)
            i += 1
    print "\nTOTAL NUMBER OF EC2 INSTANCES =  ", i

    for a in reservations:
        for b in a.instances:
            countins1 = b.instance_type
            try:
                names = b.instance_type, b.tags['Name']
                all_ins_names.append(names)
            except:
                break

            countins.append(countins1)
    disins = Counter(countins)
    disins2 = sorted(disins.keys())
    print "\nEC2 INSTANCE TYPE BREAKDOWN  ", i
    for x in disins2:
        print x, disins[x]


# Get all buckets in the cloud
def all_s3_instances():
    buckets = s3.get_all_buckets()
    print '\nALL S3 BUCKETS'
    i = 0
    print '-------------------------------------------'
    for bucket in buckets:
        print "{name}\t{created} ".format(name=bucket.name, created=bucket.creation_date)
        i += 1
    print "\nTotal number  of  Buckets = ", i


def all_db_instances():
    dbreservations = rds.describe_db_instances()
    print '\nALL RUNNING DB INSTANCES'
    print '-------------------------------------------'
    db = []
    dbclass = []
    i = 0
    for a in dbreservations['DescribeDBInstancesResponse']['DescribeDBInstancesResult']['DBInstances']:
        alldb = a['DBInstanceIdentifier'], a['DBInstanceClass']
        alldbclass = a['DBInstanceClass']
        db.append(alldb)
        dbclass.append(alldbclass)
        i += 1

    disdb = Counter(dbclass)
    disdb2 = sorted(disdb.keys())
    print "\nTotal number  of  Instances = ", i
    for x in disdb2:
        print x, disdb[x]


def all_redshift_instances():
    red = rs.describe_clusters()
    print '\nALL RUNNING REDSHIFT INSTANCES'
    print '-------------------------------------------'
    try:
        for _ in rs.describe_clusters():
            print red['DescribeClustersResponse']['DescribeClustersResult']['Clusters'][0]['ClusterIdentifier'], \
                red['DescribeClustersResponse']['DescribeClustersResult']['Clusters'][0]['NodeType'], \
                red['DescribeClustersResponse']['DescribeClustersResult']['Clusters'][0]['NumberOfNodes']
    except:
        print "No instances found"


# Get all EMR cluster
def all_emr():
    print '\nALL RUNNING,WAITING, STARTING, BOOTSTRAPPING,TERMINATED EMR CLUSTERS  IN THE LAST 24 HOURS'
    print '-------------------------------------------'
    states = ['STARTING', 'BOOTSTRAPPING', 'WAITING', 'RUNNING', 'TERMINATED', 'TERMINATED_WITH_ERRORS', 'TERMINATING']
    # TERMINATED','TERMINATED_WITH_ERRORS
    for a in emr.list_clusters(cluster_states=states, created_after=yesterday).clusters:
        try:
            print "\n Name: ", a.name, "Status: ", a.status.state, "Tag: ", a.tags, "Created Time : ", a.status.timeline.creationdatetime, \
                "Ended Time : ", a.status.timeline.enddatetime, "Total Time Up (HH:MM:SS): ", \
                (datetime.datetime.strptime(a.status.timeline.enddatetime,
                                            "%Y-%m-%dT%H:%M:%S.%fZ") - datetime.datetime.strptime(
                    a.status.timeline.creationdatetime, "%Y-%m-%dT%H:%M:%S.%fZ"))
        except:
            print "\n Name: ", a.name, "Status: ", a.status.state, "Tag: ", a.tags, "Created Time : ", a.status.timeline.creationdatetime


def main():
    # full EC2 list
    all_instances()
    # Get all Red Shift instances
    all_redshift_instances()
    # Get all DB instances
    all_db_instances()
    # Get all emr clusters
    all_emr()
    # Get s3 buckets
    all_s3_instances()


if __name__ == '__main__':
    main()
