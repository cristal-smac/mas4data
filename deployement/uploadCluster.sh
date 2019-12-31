sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-1.local 'sudo mkdir /mnt/Data; chown centos /mnt/Data; mkdir /mnt/Data/MeteoFrance'
sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-2.local 'sudo mkdir /mnt/Data; chown centos /mnt/Data; mkdir /mnt/Data/MeteoFrance'
sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-3.local 'sudo mkdir /mnt/Data; chown centos /mnt/Data; mkdir /mnt/Data/MeteoFrance'
sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-4.local 'sudo mkdir /mnt/Data; chown centos /mnt/Data; mkdir /mnt/Data/MeteoFrance'
sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-5.local 'sudo mkdir /mnt/Data; chown centos /mnt/Data; mkdir /mnt/Data/MeteoFrance'
sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-6.local 'sudo mkdir /mnt/Data; chown centos /mnt/Data; mkdir /mnt/Data/MeteoFrance'
sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-7.local 'sudo mkdir /mnt/Data; chown centos /mnt/Data; mkdir /mnt/Data/MeteoFrance'
sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-8.local 'sudo mkdir /mnt/Data; chown centos /mnt/Data; mkdir /mnt/Data/MeteoFrance'
sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-9.local 'sudo mkdir /mnt/Data; chown centos /mnt/Data; mkdir /mnt/Data/MeteoFrance'
sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-10.local 'sudo mkdir /mnt/Data; chown centos /mnt/Data; mkdir /mnt/Data/MeteoFrance'
sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-11.local 'sudo mkdir /mnt/Data; chown centos /mnt/Data; mkdir /mnt/Data/MeteoFrance'
sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-12.local 'sudo mkdir /mnt/Data; chown centos /mnt/Data; mkdir /mnt/Data/MeteoFrance'
sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-13.local 'sudo mkdir /mnt/Data; chown centos /mnt/Data; mkdir /mnt/Data/MeteoFrance'
sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-14.local 'sudo mkdir /mnt/Data; chown centos /mnt/Data; mkdir /mnt/Data/MeteoFrance'
sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-15.local 'sudo mkdir /mnt/Data; chown centos /mnt/Data; mkdir /mnt/Data/MeteoFrance'
sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-16.local 'sudo mkdir /mnt/Data; chown centos /mnt/Data; mkdir /mnt/Data/MeteoFrance'
sudo scp /mnt/Data/MeteoFrance/synop2019.csv root@morge-smac-cluster-1.local:/mnt/Data/MeteoFrance/mapper1.txt
sudo scp /mnt/Data/MeteoFrance/synop2019.csv root@morge-smac-cluster-2.local:/mnt/Data/MeteoFrance/mapper2.txt
sudo scp /mnt/Data/MeteoFrance/synop2019.csv root@morge-smac-cluster-3.local:/mnt/Data/MeteoFrance/mapper3.txt
sudo scp /mnt/Data/MeteoFrance/synop2019.csv root@morge-smac-cluster-4.local:/mnt/Data/MeteoFrance/mapper4.txt
sudo scp /mnt/Data/MeteoFrance/synop2019.csv root@morge-smac-cluster-5.local:/mnt/Data/MeteoFrance/mapper5.txt
sudo scp /mnt/Data/MeteoFrance/synop2019.csv root@morge-smac-cluster-6.local:/mnt/Data/MeteoFrance/mapper6.txt
sudo scp /mnt/Data/MeteoFrance/synop2019.csv root@morge-smac-cluster-7.local:/mnt/Data/MeteoFrance/mapper7.txt
sudo scp /mnt/Data/MeteoFrance/synop2019.csv root@morge-smac-cluster-8.local:/mnt/Data/MeteoFrance/mapper8.txt
sudo scp /mnt/Data/MeteoFrance/synop2019.csv root@morge-smac-cluster-9.local:/mnt/Data/MeteoFrance/mapper9.txt
sudo scp /mnt/Data/MeteoFrance/synop2019.csv root@morge-smac-cluster-10.local:/mnt/Data/MeteoFrance/mapper10.txt
sudo scp /mnt/Data/MeteoFrance/synop2019.csv root@morge-smac-cluster-11.local:/mnt/Data/MeteoFrance/mapper11.txt
sudo scp /mnt/Data/MeteoFrance/synop2019.csv root@morge-smac-cluster-12.local:/mnt/Data/MeteoFrance/mapper12.txt
sudo scp /mnt/Data/MeteoFrance/synop2019.csv root@morge-smac-cluster-13.local:/mnt/Data/MeteoFrance/mapper13.txt
sudo scp /mnt/Data/MeteoFrance/synop2019.csv root@morge-smac-cluster-14.local:/mnt/Data/MeteoFrance/mapper14.txt
sudo scp /mnt/Data/MeteoFrance/synop2019.csv root@morge-smac-cluster-15.local:/mnt/Data/MeteoFrance/mapper15.txt
sudo scp /mnt/Data/MeteoFrance/synop2019.csv root@morge-smac-cluster-16.local:/mnt/Data/MeteoFrance/mapper16.txt

