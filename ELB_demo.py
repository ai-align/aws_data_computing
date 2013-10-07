#!/usr/bin/python

import sys
import boto.ec2.elb

def show_region():
    regions = boto.ec2.elb.regions()
    for region in regions:
        print region

def show_existing_elb():
    elb = boto.ec2.elb.connect_to_region('us-east-1')
    for existed_elb in elb.get_all_load_balancers():
        print existed_elb

def get_installed_elb():
    elb = boto.ec2.elb.connect_to_region('us-east-1')
    new_elb = None
    for existed_elb in elb.get_all_load_balancers():
        if(existed_elb.name=='ELB_demo'):
            new_elb = existed_elb
        if(not new_elb):
            new_elb = create_elb()
    return new_elb;


def add_instace(instance_id):
    new_elb = get_installed_elb()
    new_elb.register_instances([instance_id])
    
def remove_instance(instance_id):
    new_elb = get_installed_elb()
    new_elb.deregister_instances([instance_id])
    
def create_elb():
    elb = boto.ec2.elb.connect_to_region("us-east-1")
    hc = boto.ec2.elb.HealthCheck(interval=20, timeout=5,
    unhealthy_threshold=2,healthy_threshold=10,target='HTTP:8080/upload')
    zones = ['us-east-1a']
    ports = [(80,80,'http'),(8080,8080,'http')]
    
    try:
        new_elb = elb.create_load_balancer('ELB_demo',zones,ports)
    except:
        print "creating new elb failed"
        sys.exit(1)
    new_elb.configure_health_check(hc)
    return new_elb

def elb_demo(arguments):
    if(arguments['--region']):
        show_region()
    elif(arguments['--elb']):
        show_existing_elb()
    elif(arguments['--add']):
        add_instance(arguments['<id>'])
    elif(arguments['--remove']):
        remove_instance(arguments['<id>'])
    elif(arguments['--create']):
        create_elb()
    