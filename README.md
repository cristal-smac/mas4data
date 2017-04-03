
    git clone git@github.com:cristal-smac/mas4data.git



# mas4data : Multi-Agents Systems for very large Data sets

## Contexte 

### MapReduce

La science des données vise à traiter de grands volumes de données
pour y extraire de nouvelles connaissances.  Comme le potentiel
technologique et la demande des sociétés ont augmenté, de nouvelles
méthodes, de nouveaux modèles, systèmes et algorithmes sont
développés.  L'analyse de ces données, en raison de leur volume, de
leur vitesse d'acquisition et de leur hétérogénéïté, demande de
nouvelles formes de traitements.  À cette intention, Google a créé le
patron d'architecture de développement MapReduce. Le framework le plus
populaire pour MapReduce est Hadoop mais de nombreuses autres
implémentations existent.  MapReduce permet de traiter de grands
volumes de données et tient son nom des deux phases de traitement
basées sur deux fonctions : la fonction *map* qui filtre les données et
la fonction *reduce* qui les agrège.

Les données et les flux d'entrées peuvent faire l'objet de biais, de
pics d'activités périodiques (quotidiens, hebdomadaires ou mensuels)
ou de pics d'activités déclenchés par un événement particulier.  Ces
distorsions peuvent être particulièrement difficiles à gérer. Dans les
frameworks existants, une affectation efficace des tâches (c.à.d. la
répartition des clés) demande une connaissance a priori de la
distribution des données. Ces biais ont fait l'objet d'études
 et propositions de correction

1.  soit avec un prétraitement de la donnée, ce
qui pose le problème de construction d'un échantillon représentatif ;

2. soit avec une historisation des informations sur les précédentes
exécutions, ce qui n'est pas adapté à une variation des données et des
traitements dans le temps ;

3. soit avec un système nécessitant une
expertise forte sur la nature des données (connaissance a priori) afin
de paramétrer au mieux les outils.

### Systèmes Multi-Agents

L'équipe SMAC (Systèmes Multi-Agents et Comportements) du laboratoire
CRIStAL utilise le paradigme des **systèmes multi-agents** (SMA), en
particulier pour la résolution de problèmes complexes. Dans ce cas, un
SMA est composé de multiples entités dont le but est de résoudre un
problème qui ne pourrait pas l'être individuellement. Un SMA est
caractérisé par le fait que :

1. chaque agent a des informations incomplètes et des capacités
limitées ;

2. il n'y a pas de contrôle global du système ;

3. les données sont décentralisées ;

4. les calculs sont réalisés de manière asynchrone.

Dans un SMA, l'autonomie des agents permet au système de s'adapter
dynamiquement aux variations, voire aux perturbations de leur
environnement.  Pour cette raison, nous défendons la thèse selon
laquelle **les SMA sont particulièrement appropriés pour
s'adapter à des données inconnues, à des flux qui évoluent constamment
et à un environnement informatique dynamique**.

    
## Projet scientifique

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

## Publications

- *Allocation équitable de tâches pour l'analyse de données massives.*
Quentin Baert, Anne-Cécile Caron, Maxime Morge, Jean-Christophe Routier, dans les actes des 24èmes Journées Francophones sur les Systèmes Multi-Agents 2016 (JFSMA'2016). Hermès. pp. 55-64. 2016. 

- *Fair Multi-Agent Task Allocation for Large Data Sets Analysis.*
Quentin Baert, Anne-Cécile Caron, Maxime Morge, Jean-Christophe Routier, dans les actes de 14th International Conference on Practical Applications of Agents and Multi-Agent Systems (PAAMS'2016). LNAI 9662, pp. 24-35. 2016.

