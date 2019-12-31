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

This file contain one IP address per line. We will call it `my_ips.txt`.


### Create two files which distribute the mappers and the reducers among the nodes.

The `Monitor` must know where the agents are deployed. For this
purpose, we create two files called `remote_mappers.txt` and
`remote_reducers.txt`.  Each line contains the IP address of the node
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

Le fichier `config/configLocagion.txt` indique quel est le chemin du fichier de configuration à considérer lors de la prochaine exécution. Il permet de pouvoir faire coexister plusieurs fichier de configurations et de passer de l'un à l'autre sans avoir à faire de copier-coller et ainsi risquer de perdre une configuration précédemment définie.

### Indiquer la bonne adresse IP dans le fichier `application.conf`

Dans le fichier `src/main/resources/application.conf`, indiquer l'adresse IP du `Monitor` pour le champ `akka.remote.netty.tcp.hostname`.

### Ouvrir les ports sur les machines

Sur l'ensemble des machines utilisées lors de l'exécution, s'assurer que le port `6666` est ouvert.

Sur chaque VM du cluster, utiliser la commande suivante :

```iptables -A INPUT -p tcp --dport 6666```

## Créer le .jar à déployer

Depuis la racine.

```
sbt daemon:assembly
```

Au terme de la commande, le .jar à déployer se trouve au chemin suivant `./target/scala-2.11/daemon.jar`.

## Déployer le .jar sur les machines distantes

```
./deployement/deploy.sh <login> <nodesFile> <daemonJar> <distantPath>
```

Avec :

- `<login>` : le login de l'utilisateur sur les machines distantes ;
- `<nodesFile>` : le chemin vers le fichier qui contient la liste des adresses IP des machines distantes sur lesquels déployer le .jar (`mes_ip.txt`);
- `<daemonJar>` : le chemin vers le .jar `daemon.jar` sur la machine locale ;
- `<distantPath>` : le chemin auquel déposer le .jar sur les machines distantes.

## Initialiser les `ActorSystem` sur les machines distantes

```
./deployement/lauch.sh <login> <nodesFile> <daemonJar> <asName> <port>
```

Avec :

- `<login>` : le login de l'utilisateur sur les machines distantes ;
- `<nodesFile>` : le chemin vers le fichier qui contient la liste des adresses IP des machines distantes sur lesquels déployer le .jar ;
- `<daemonJar>` : le chemin vers le .jar `daemon.jar` sur les machines distantes.
- `<asName>` : nom de l'`ActorSystem` (par exemple `RemoteAS`) ;
- `<port>` : port sur lequel écoute l'`ActorSystem`.

## Lancer l'exécution

### Lancer une exécution unique

Tout d'abord, s'assurer que le fichier `config/configLocation.txt` pointe bien vers le fichier de configuration à utiliser.

Ensuite, générer le .jar exécutable.

```
sbt monitor:assembly
```

Enfin, lancer le .jar crée.

```
java -jar target/scala-2.11/monitor.jar mapreduce.adpative.Monitor
```

### Lancer un ensemble d'exécution à l'aide d'un méta fichier de configuration

#### Méta fichier de configuration

Il est également possible de lancer un ensemble d'exécution en faisant varier les paramètres des exécutions.

Pour cela, il faut créer un méta fichier de configuration, c'est-à-dire un fichier de configuration à partir duquel générer plusieurs fichiers de configurations différents.

La syntaxe d'un méta fichier de configuration est simple. Pour chaque entrée à faire varier, préfixer la ligne d'un caractère `*` et indiquer les différentes valeurs entre crochets, séparées d'un `;`. Par exemple, pour faire varier la stratégie de sélection de tâches utilisée :

```
* task-bundle-management-strategy : [ownership; (k-eligible-big, 2)]
```

La ligne précédente donnera lieu à deux fichiers de configuration différents. Un premier pour lequel la valeur associée au champ `task-bundle-management-strategy` sera `ownership`. Un second pour lequel la valeur sera `(k-eligible-big,2)`.

Un méta fichier de configuration représente l'ensemble des fichiers de configuration qui correspond au produit cartésien de chacun des champs de configuration marqué du caractère `*`.

#### Lancement de l'exécution

Il n'existe pour l'instant pas de .jar exécutable pour lancer un ensemble d'exécutions. Il faut donc utiliser la console de `sbt` pour être en mesure d'accéder à la classe `ExperimentsBuilder` du prototype.

```
$ sbt console
> import utils.experiments.ExperimentsBuilder
> val eb = new ExperimentsBuilder(5, "exp", "config/configLocation.txt", "exp/config.txt")
> eb.runAdaptive
```

L'extrait de terminal précédent provoque les actions suivantes :

1. Ouvrir la console de `sbt`.

2. Importer la classe `ExperimentsBuilder` du prototype.

3. Construire une instance `eb` de `ExperimentsBuilder`. D'après ces paramètres :
    - `eb` lancera 5 exécutions par fichier de configuration généré à partir du méta fichier de configuration `exp/config.txt`.
    - `eb` stockera les résultats de chacune des exécutions dans le dossier `exp`.
    - `eb` écrira successivement le fichier de configuration courant dans le fichier `config/configLocation.txt`.

4. Lancer l'ensemble des exécutions avec `eb.runAdaptive`.