#!/usr/bin/python

import sys
import time
import boto.ec2.elb
import boto.ec2.cloudwatch
import boto.ec2.autoscale
from boto.ec2.autoscale import AutoScaleConnection
from boto.ec2.autoscale import LaunchConfiguration
from boto.ec2.autoscale import AutoScalingGroup   
from boto.ec2.autoscale import ScalingPolicy
from boto.ec2.cloudwatch import MetricAlarm

        
def create_elb():
    elb = boto.ec2.elb.connect_to_region('us-east-1')
    hc = boto.ec2.elb.HealthCheck(interval=20, timeout=5,
    unhealthy_threshold=2,healthy_threshold=10,target='HTTP:8080/upload')
    zones = ['us-east-1a']
    ports = [(80,80,'http'),(8080,8080,'http')]
    
    try:
        new_elb = elb.create_load_balancer('elbdemo',zones,ports)
    except:
        print "creating new elb failed"
        sys.exit(1)
    new_elb.configure_health_check(hc)
    return new_elb


def create_autoscalegroup_watch(conn):
    
    lc = LaunchConfiguration(name='my-launch_config', image_id='ami-2b7b2c42',
                                 key_name='key2bench',
                                 security_groups=['default'])
    conn.create_launch_configuration(lc)
     
    scale_up_policy = ScalingPolicy(
                name='scale_up', adjustment_type='ChangeInCapacity',
                as_name='Auto_scale_group', scaling_adjustment=1, cooldown=180)
                
    scale_down_policy = ScalingPolicy(
                name='scale_down', adjustment_type='ChangeInCapacity',
                as_name='Auto_scale_group', scaling_adjustment=-1, cooldown=180)
    
    conn.create_scaling_policy(scale_up_policy)
    conn.create_scaling_policy(scale_down_policy)
    scale_up_policy = conn.get_all_policies(
            as_group='Auto_scale_group', policy_names=['scale_up'])[0]
    scale_down_policy = conn.get_all_policies(
            as_group='Auto_scale_group', policy_names=['scale_down'])[0]
    # ======create auto scale group with configuration ======
    
    # =============  CloudWatch ====================
    cloudwatch = boto.ec2.cloudwatch.connect_to_region('us-east-1')
    
    alarm_dimensions = {"AutoScalingGroupName": 'Auto_scale_group'}
    
    scale_up_alarm = MetricAlarm(
                name='scale_up_on_cpu', namespace='AWS/EC2',
                metric='CPUUtilization', statistic='Average',
                comparison='>', threshold='80',
                period='60', evaluation_periods=5,
                alarm_actions=[scale_up_policy.policy_arn],
                dimensions=alarm_dimensions)
                
    cloudwatch.create_alarm(scale_up_alarm)
    
    scale_down_alarm = MetricAlarm(
                name='scale_down_on_cpu', namespace='AWS/EC2',
                metric='CPUUtilization', statistic='Average',
                comparison='<', threshold='20',
                period='60', evaluation_periods=5,
                alarm_actions=[scale_down_policy.policy_arn],
                dimensions=alarm_dimensions)
                
    cloudwatch.create_alarm(scale_down_alarm)
    
    conn.put_notification_configuration(ag, 'arn:aws:sns:us-east-1:233941742685:Auto_Scale_events',['autoscaling:EC2_INSTANCE_LAUNCH', 'autoscaling:EC2_INSTANCE_TERMINATE'])
    # =============  CloudWatch ====================

def shutdown(conn, ag):
        
    ag.shutdown_instances()
    
    time.sleep(180)
    
    print "Auto_Scale_Group's activities:"
    ag.get_all_activities()
    
    conn.delete_launch_configuration('Auto_scale_group', force_delete=False)
    
    conn.delete_launch_configuration('my-launch_config')
         
def main():
    
    create_elb()
    
    # ======create auto scale group with configuration ======
    conn = boto.ec2.autoscale.connect_to_region('us-east-1')
    
    
    ag = AutoScalingGroup(group_name='Auto_scale_group', load_balancers=['ELBdemo'],
                              availability_zones=['us-east-1a'],
                              launch_config='my-launch_config', min_size=2, max_size=5,
                              connection=conn)
                              
    conn.create_auto_scaling_group(ag)
    
    ag.get_activities()
    
    create_autoscalegroup_watch(conn)
    
    # after creating the auto_scale_group with instances, I use command to run the benchmark mannully in lanuch instance and receive the results from Email
    
    
    # after compeleting the benchmark, I use the shutdown menthod to shutdown the group 
    
    # shutdown(conn, ag)
        

if __name__=='__main__':
    main()
