#!/usr/bin/sh
for cpu in /sys/devices/system/cpu/cpu[1-9]*/online; do
    echo 1 >"$cpu"
done
