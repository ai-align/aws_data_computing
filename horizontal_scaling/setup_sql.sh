#!/bin/bash

sudo su

dd if=/dev/xvdc of=/dev/null bs=256k

mkfs.ext4 /dev/xvdc 

mkdir /storage/mountpoint
mount /dev/xvdc /storage/mountpoint

cd /storage/mountpoint
cp -a /home/mysql_backup/* .
chown mysql:mysql /storage/mountpoint

service mysql stop
mount --bind /storage/mountpoint /var/lib/mysql
service mysql start

