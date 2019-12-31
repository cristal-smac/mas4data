#!/usr/bin/sh
for cpu in /sys/devices/system/cpu/cpu[1-9]*/online; do
    sudo echo 0 >"$cpu"
done
