#!usr/bin/python


import boto.ec2.elb
import boto.ec2.cloudwatch
import boto.ec2.autoscale
from boto.ec2.autoscale import AutoScaleConnection
from boto.ec2.autoscale import LaunchConfiguration
from boto.ec2.autoscale import AutoScalingGroup   
from boto.ec2.autoscale import ScalingPolicy
from boto.ec2.cloudwatch import MetricAlarm
import _mysql
import sys

# sudo su
# 
# dd if=/dev/xvdc of=/dev/null bs=256k
# 
# mkfs.ext4 /dev/xvdc 
# 
# mkdir /storage/mountpoint
# mount /dev/xvdc /storage/mountpoint
# 
# cd /storage/mountpoint
# cp -a /home/mysql_backup/* .
# chown mysql:mysql /storage/mountpoint
# 

def main():
    max_TPS = 138.89
    con = _mysql.connect('localhost', 'u', 'pdb15319root') 
    
    cloudwatch = cloudwatch = boto.ec2.cloudwatch.connect_to_region('us-east-1')
    conn = boto.ec2.cloudwatch.CloudWatchConnection(aws_access_key_id='AKIAIUUSLBP7GFHBNSDA',aws_secret_access_key='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')
    alarm_dimensions = {"AutoScalingGroupName": 'Auto_scale_group'}
    
    while(1):
        
        query_num = con.store_result()
                query_num = (float)(query_num.fetch_row()[0][0])
                con.query("select variable_value from information_schema.global_status where variable_name like 'Uptime'")
                query_time = con.store_result()
                query_time = (float)(query_time.fetch_row()[0][0])
        
                time.sleep(60)

                con.query("select variable_value from information_schema.global_status where variable_name like 'Queries'")
                query_num1 = con.store_result()
                query_num1 = (float)(query_num1.fetch_row()[0][0])
                con.query("select variable_value from information_schema.global_status where variable_name like 'Uptime'")
                query_time1 = con.store_result()
                query_time1 = (float)(query_time1.fetch_row()[0][0])

                TPS_percentage = (query_num1 - query_num - 6)/(query_time1 - query_time)/max_TPS/16
        
  
       
        
        conn.put_metric_data(namespace='Tao_TPS', name='TPSPercentage', value='TPS_percentage', unit='Percent', dimensions=alarm_dimensions)
        
        #     
        # scale_up_alarm = MetricAlarm(
        #             name='scale_up_on_cpu', namespace='AWS/EC2',
        #             metric='CPUUtilization', statistic='Average',
        #             comparison='>', threshold='80',
        #             period='60', evaluation_periods=5,
        #             alarm_actions=[scale_up_policy.policy_arn],
        #             dimensions=alarm_dimensions)
        #         
        # cloudwatch.create_alarm(scale_up_alarm)
        #     
        # scale_down_alarm = MetricAlarm(
        #             name='scale_down_on_cpu', namespace='AWS/EC2',
        #             metric='CPUUtilization', statistic='Average',
        #             comparison='<', threshold='20',
        #             period='60', evaluation_periods=5,
        #             alarm_actions=[scale_down_policy.policy_arn],
        #             dimensions=alarm_dimensions)
        #         
        # cloudwatch.create_alarm(scale_down_alarm)
    
    # except _mysql.Error, e:
    #   
    #     print "Error %d: %s" % (e.args[0], e.args[1])
    #     sys.exit(1)
    # 
    # finally:
    # 
    #     if con:
    #         con.close()

if __name__ == "__main__":
    main()

