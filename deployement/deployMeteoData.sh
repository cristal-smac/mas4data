sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-1.local 'mkdir /mnt/Data/MeteoFrance'
sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-2.local 'mkdir /mnt/Data/MeteoFrance'
sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-3.local 'mkdir /mnt/Data/MeteoFrance'
sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-4.local 'mkdir /mnt/Data/MeteoFrance'
sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-5.local 'mkdir /mnt/Data/MeteoFrance'
sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-6.local 'mkdir /mnt/Data/MeteoFrance'
sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-7.local 'mkdir /mnt/Data/MeteoFrance'
sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-8.local 'mkdir /mnt/Data/MeteoFrance'
sudo scp /mnt/Data/MeteoFrance/mapper1.txt root@morge-smac-cluster-1.local:/mnt/Data/MeteoFrance
sudo scp /mnt/Data/MeteoFrance/mapper2.txt root@morge-smac-cluster-2.local:/mnt/Data/MeteoFrance
sudo scp /mnt/Data/MeteoFrance/mapper3.txt root@morge-smac-cluster-3.local:/mnt/Data/MeteoFrance
sudo scp /mnt/Data/MeteoFrance/mapper4.txt root@morge-smac-cluster-4.local:/mnt/Data/MeteoFrance
sudo scp /mnt/Data/MeteoFrance/mapper5.txt root@morge-smac-cluster-5.local:/mnt/Data/MeteoFrance
sudo scp /mnt/Data/MeteoFrance/mapper6.txt root@morge-smac-cluster-6.local:/mnt/Data/MeteoFrance
sudo scp /mnt/Data/MeteoFrance/mapper7.txt root@morge-smac-cluster-7.local:/mnt/Data/MeteoFrance
sudo scp /mnt/Data/MeteoFrance/mapper8.txt root@morge-smac-cluster-8.local:/mnt/Data/MeteoFrance
