sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-1.local 'mkdir /mnt/Data/Artificial'
sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-2.local 'mkdir /mnt/Data/Artificial'
sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-3.local 'mkdir /mnt/Data/Artificial'
sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-4.local 'mkdir /mnt/Data/Artificial'
sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-5.local 'mkdir /mnt/Data/Artificial'
sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-6.local 'mkdir /mnt/Data/Artificial'
sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-7.local 'mkdir /mnt/Data/Artificial'
sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-8.local 'mkdir /mnt/Data/Artificial'
sudo scp /mnt/Data/Artificial/mapper1.txt root@morge-smac-cluster-1.local:/mnt/Data/Artificial/
sudo scp /mnt/Data/Artificial/mapper2.txt root@morge-smac-cluster-2.local:/mnt/Data/Artificial/
sudo scp /mnt/Data/Artificial/mapper3.txt root@morge-smac-cluster-3.local:/mnt/Data/Artificial/
sudo scp /mnt/Data/Artificial/mapper4.txt root@morge-smac-cluster-4.local:/mnt/Data/Artificial/
sudo scp /mnt/Data/Artificial/mapper5.txt root@morge-smac-cluster-5.local:/mnt/Data/Artificial/
sudo scp /mnt/Data/Artificial/mapper6.txt root@morge-smac-cluster-6.local:/mnt/Data/Artificial/
sudo scp /mnt/Data/Artificial/mapper7.txt root@morge-smac-cluster-7.local:/mnt/Data/Artificial/
sudo scp /mnt/Data/Artificial/mapper8.txt root@morge-smac-cluster-8.local:/mnt/Data/Artificial/
