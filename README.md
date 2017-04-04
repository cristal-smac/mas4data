# MAS4Data : Multi-Agents Systems for analyzing very large Data sets 

## Context 

### MapReduce

Data Science aims at processing large volumes of data to extract
knowledge or insights. The volume and velocity of the available data to
analyze requires to parallelize the processing as it can be done with
the MapReduce design pattern [](). The latter takes its
name from the functions on which it is based: the *map* function
which filters the data and the *reduce* function which aggregates
them. The most popular framework for MapReduce is Hadoop but many
other implementations exist, like the cluster computing framework
Spark [](), or the distributed NoSQL database Riak
built from Amazon Dynamo []().


Data flows can have periodic (daily, weekly or seasonal) and
event-triggered peak data loads. These peaks can be challenging to
manage. In the existing frameworks, an efficient task distribution
(i.e. the key partitioning) requires prior knowledge of the data
distribution. The partitioning is *a priori* fixed and so the workload
is not necessarily uniformly distributed. In a similar way, the usage
of an heterogeneous cluster environment can lead to a workload
unevenly distributed between the reducers. 

These have been studied and
Ces biais ont fait l'objet d'études
 et propositions de correction

1.  soit avec un prétraitement de la donnée, ce
qui pose le problème de construction d'un échantillon représentatif ;

2. soit avec une historisation des informations sur les précédentes
exécutions, ce qui n'est pas adapté à une variation des données et des
traitements dans le temps ;

3. soit avec un système nécessitant une
expertise forte sur la nature des données (connaissance a priori) afin
de paramétrer au mieux les outils.

## Multiagent systems

By contrast, multiagent
systems are inherently adaptive and thus particularly suitable when
workloads constantly evolve.

The SMAC team (Multiagents systems and behaviours) of the laboratory
CRIStAL employ the **multiagents systems ** paradigm (MAS), in
particularen for complex distributed problem solving. For this
purpose, a MAS is composed of many entities which aim at solving a
complex problem which cannot be done by a single entity. A MAS is
characterizd by the the fact that :

1. each agent has incomplete information and limited capabilities;

2. there is no central authority;

3. data are decentralized;

4. the computation are asynchronously .

Dans un SMA, l'autonomie des agents permet au système de s'adapter
dynamiquement aux variations, voire aux perturbations de leur
environnement.  Pour cette raison, nous défendons la thèse selon
laquelle **les SMA sont particulièrement appropriés pour
s'adapter à des données inconnues, à des flux qui évoluent constamment
et à un environnement informatique dynamique**.

    
## Scientific project

Ce projet consiste ainsi à adopter une approche interdisciplinaire,
mêlant Informatique et Économétrie, pour relever :
*	les défis liés à la gestion des données notamment le calcul
     intensif sur des grands volumes de données et le parallélisme
     dirigé par les données ;  
*	les défis liés à l’extraction de connaissances notamment la
     recherche d’information et les requêtes complexes sur les grandes
     données.  

Dans le cadre du projet **mas4data** nous travaillons sur la
conception d'un système multi-agents implémentant MapReduce où
l'allocation dynamique des tâches repose sur une négociation entre
reducers.  **Ce système est dynamique, décentralisé, sans historique
ni connaissance a priori des données et il ne nécessite pas de
paramétrage dépendant des données**.
  
Notre SMA utilise plusieurs enchères distribuées et simultanées auxquelles
participent des agents adaptatifs qui prennent des décisions basées
sur des informations locales.

Dans certains cas, pour permettre une meilleur répartition des tâches, nous sommes
capables de diviser les grandes tâches en plusieurs sous-tâches pour ensuite agréger
leurs résultats intermédiaires  et construire le résultat final.

Pour l'instant, nous nous sommes focalisés sur la phase Reduce mais
l'optimisation de la phase Map pourrait aussi être mise en oeuvre par
un SMA.  

Nous envisageons d'adapter le réseau d'accointances des
agents pour construire des groupes qui négocient indépendamment les
uns des autres. Typiquement dans un système distribué, le coût de
communication dépend de la topologie du réseau, c'est-à-dire des
contraintes physiques. Une solution consiste à adapter les groupes au
réseau physique et donc répartir physiquement les négociations.  Enfin
nous souhaitons optimiser non seulement la répartition de l'activité
au sein du SMA durant le traitement d'un job MapReduce, mais aussi
durant l'exécution d'un enchaînement de jobs (MapReduce Chaining), et
l'exécution concurrente de plusieurs jobs, en tenant compte de
l'activité des agents et la localité des données.

## References

Dean, J. and Ghemawat, S. (2008), Mapreduce: simplified data
processing on large clusters, Commun. ACM 51(1), 107–113.

Zaharia, M., Xin, R. S., Wendell, P., Das, T., Armbrust, M., Dave, A.,
Meng, X., Rosen, J., Venkataraman, S., Franklin, M. J., Ghodsi, A.,
Gonzalez, J., Shenker, S. and Stoica, I. (2016), Apache spark: a
unified engine for big data processing, Commun. ACM 59(11), 56– 65.

DeCandia, G., Hastorun, D., Jampani, M., Kakulapati, G., Lakshman, A.,
Pilchin, A., Sivasubramanian, S., Vosshall, P. and Vogels,
W. (2007), Dynamo: Amazon’s highly available key-value store, in
Proc. of the 21st ACM SIGOPS Symposium on Operating Systems
Principles (SOSP ’07), pp. 205–220.

## People

* Quentin BAERT, PhD candidtate

* Anne-Cécile CARON,  Associate Professor

* Maxime MORGE, Associate Professor

* Jean-Christophe ROUTIER, Professor


## Publications

- *Allocation équitable de tâches pour l'analyse de données massives.*
Quentin Baert, Anne-Cécile Caron, Maxime Morge, Jean-Christophe
Routier, dans les actes des 24èmes Journées Francophones sur les
Systèmes Multi-Agents 2016 (JFSMA'2016). Hermès. pp. 55-64. 2016.

- *Fair Multi-Agent Task Allocation for Large Data Sets Analysis.*
Quentin Baert, Anne-Cécile Caron, Maxime Morge, Jean-Christophe
Routier, dans les actes de 14th International Conference on Practical
Applications of Agents and Multi-Agent Systems (PAAMS'2016). LNAI
9662, pp. 24-35. 2016.

