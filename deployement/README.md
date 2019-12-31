# Deployement of the MAS4Data prototype 

Please use the following instructions.

## Preamble

We call here `Monitor` the master node where the run is launched and the results are written.

### Name of the ActorSystem

The remote mappers and reducers are created within an ActorSystem. The
latter must have a name which is required for the
pre-configuration. We will call it `RemoteAS` and we will use the port
`6666`.

### Create a file which contains the IP addresses of the remote nodes. 

This file contains one IP address per line. We will call it `my_ips.txt`.


### Create two files which distribute the mappers and the reducers among the nodes.

The `Monitor` must know where the agents are deployed. For this
purpose, we create two files called `remoteMappersCluster.txt` and
`remoteReducersCluster.txt`.  Each line contains the IP address of the node
followed by a space and then the number of agents deployed on this
node. For instance, 5 mappers are deployed on `172.18.12.221`
according to the followed `remote_mappers.txt` file:

```
akka.tcp://RemoteAS@172.18.12.221:6666 5
```

### Split the data and distribute them among the nodes.


A data split is called `mapperX.txt` where `X` is the number of the
split. We create one data split per mapper. Each data split must be on
the node where the corresponding mapper is. See the scripts in this
directory.


### Create a configuration file 

#### Setup

| Configuration parameter | Type | Example |
|:-----------------------------------|:-----|:------------------|
| nb-mapper | INT | 16 |
| nb-reducer | INT | 16 |
| pb | STRING | `meteoRecordByTemperatureStation` |
| task-bundle-management-strategy | STRING | `csdb` or `cbds` or `ownership` |
| threshold | DOUBLE | `1` without negotiation or `0` with negotiation |
| bidder-max-auction | STRING/INT | `none` or maximum number of concurrent bids, e.g. `1` |
| partition-strategy | STRING | `naive` |
| task-cost-strategy | STRING | `(multiplier, 2)` |
| inform-contribution-frequency | INT |  `1` |
| init-source-type | STRING | `file` |
| init-file | STRING | `/mnt/Data/MeteoFrance/synop2019.csv` |
| init-chunks | BOOLEAN | `false` |
| init-chunks-path | STRING | `/mnt/Data/MeteoFrance/` |
| init-chunks-number | INT | 8 |
| chunk-size | INT | 50000 |
| result-path | STRING | `/mnt/results` |
| initiator-timeout | INT | 300  |
| bidder-timeout | INT | 600 |
| contractor-timeout | INT | 300 |
| acknowledgment-timeout | INT | 6000 |
| pause-mili | INT | 0 |
| pause-nano | INT | 0 |
| debug-mapper | BOOLEAN | `false`  |
| debug-reducer | BOOLEAN | `false`  |
| debug-manager | BOOLEAN | `false`  |
| debug-broker | BOOLEAN | `false`  |
| debug-rfh | BOOLEAN | `false`  |
| debug-monitor | BOOLEAN | `false`  |
| remote | BOOLEAN | `true` |
| remote-mappers | STRING | `config/remoteMappersCluster.txt` |
| remote-reducers | STRING | `config/remoteReducersCluster.txt` |
| gnuplot-max-taskdone-number | INT | 80000 |
| gnuplot-title | STRING | `contribution` |
| gnuplot-output-filename | STRING | `contribution` |
| gnuplot-output-format | STRING | `png` |
| task-monitor | BOOLEAN | `false`  |
| monitor-task-scale | STRING | 150000 |
| monitor-task-scale-step | INT | 100  |


#### File `config/configLocation.txt`

This file indicates the path of the configuration file for the next
run.

### File `application.conf`

We modify in the file `src/main/resources/application.conf`, the IP
adresses of the master node where the `Monitor` is deployed.

## Create the jar file


```
sbt daemon:assembly
```

The path of the jar file is `./target/scala-2.11/daemon.jar`.

## Deploy the jar file on the nodes

```
./deployement/deployCluster.sh 
```

## Run the daemons

```
./scripts/launchCluster.sh
```

## Run the system

```
sbt monitor:assembly
java -jar target/scala-2.11/monitor.jar mapreduce.adpative.Monitor
```

### Run several experiments

It is also possible to run several experiments with various
parameters. For this purpose, we create a meta-configuration file in
order to generate multiple configuration files. In the
meta-configuration, each variable parameter is prefixed with the joker
character `*` and the different values are in square brackets and
separated by semicolon. E.g.

```
* task-bundle-management-strategy : [ownership; (k-eligible-big, 2)]
```

The execution is performed using the class `ExperimentsBuilder` 

```
$ sbt console
> import utils.experiments.ExperimentsBuilder
> val eb = new ExperimentsBuilder(5, "exp", "config/configLocation.txt", "exp/config.txt")
> eb.runAdaptive
```