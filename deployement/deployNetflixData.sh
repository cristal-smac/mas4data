sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-1.local 'mkdir /mnt/Data/Netflix'
sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-2.local 'mkdir /mnt/Data/Netflix'
sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-3.local 'mkdir /mnt/Data/Netflix'
sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-4.local 'mkdir /mnt/Data/Netflix'
sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-5.local 'mkdir /mnt/Data/Netflix'
sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-6.local 'mkdir /mnt/Data/Netflix'
sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-7.local 'mkdir /mnt/Data/Netflix'
sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-8.local 'mkdir /mnt/Data/Netflix'
sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-9.local 'mkdir /mnt/Data/Netflix'
sudo ssh -o 'StrictHostKeyChecking no' root@morge-smac-cluster-10.local 'mkdir /mnt/Data/Netflix'
sudo scp /mnt/Data/Netflix/mapper1.txt root@morge-smac-cluster-1.local:/mnt/Data/Netflix/mapper1.txt
sudo scp /mnt/Data/Netflix/mapper2.txt root@morge-smac-cluster-2.local:/mnt/Data/Netflix/mapper2.txt
sudo scp /mnt/Data/Netflix/mapper3.txt root@morge-smac-cluster-3.local:/mnt/Data/Netflix/mapper3.txt
sudo scp /mnt/Data/Netflix/mapper4.txt root@morge-smac-cluster-4.local:/mnt/Data/Netflix/mapper4.txt
sudo scp /mnt/Data/Netflix/mapper5.txt root@morge-smac-cluster-5.local:/mnt/Data/Netflix/mapper5.txt
sudo scp /mnt/Data/Netflix/mapper6.txt root@morge-smac-cluster-6.local:/mnt/Data/Netflix/mapper6.txt
sudo scp /mnt/Data/Netflix/mapper7.txt root@morge-smac-cluster-7.local:/mnt/Data/Netflix/mapper7.txt
sudo scp /mnt/Data/Netflix/mapper8.txt root@morge-smac-cluster-8.local:/mnt/Data/Netflix/mapper8.txt
sudo scp /mnt/Data/Netflix/mapper9.txt root@morge-smac-cluster-9.local:/mnt/Data/Netflix/mapper9.txt
sudo scp /mnt/Data/Netflix/mapper10.txt root@morge-smac-cluster-10.local:/mnt/Data/Netflix/mapper10.txt
