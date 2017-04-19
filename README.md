# MAS4Data : Multiagent systems for analyzing very large data sets 

## Context 

### MapReduce

Data Science aims at processing large volumes of data to extract
knowledge or insights. The volume and velocity of the available data
to analyze requires to parallelize the processing as it can be done
with the MapReduce design pattern [(Dean and Ghemawat, 2008)](#1). The
latter takes its name from the functions on which it is based: the
*map* function which filters the data and the *reduce* function which
aggregates them. The most popular framework for MapReduce is Hadoop
but many other implementations exist, such as the cluster computing
framework Spark [(Zaharia et al., 2016)](#2), or the distributed NoSQL
database Riak built from Amazon Dynamo [(DeCandia et al., 2007)](#3).

Data flows can have periodic (daily, weekly or seasonal) and
event-triggered peak data loads. These peaks can be challenging to
manage. In the existing frameworks, an efficient task distribution
(i.e. the key partitioning) requires prior knowledge of the data
distribution. The partitioning is *a priori* fixed and so the workload
is not necessarily uniformly distributed. In a similar way, the usage
of an heterogeneous cluster environment can lead to a workload
unevenly distributed between the reducers. 

These skews have been studied and corrected with: 

1. a pre-processing of the data which raises the problem
   of building a representative sample;

2. a history of the information on the previous runs which
is not adapted with a variation of the data and treatments over time;

3. a system requiring an expertise on the data (a priori
knowledge) in order to set up the tools.

## Multiagent systems

By contrast, multiagent
systems are inherently adaptive and thus particularly suitable when
workloads constantly evolve.

The SMAC team (Multiagent systems and behaviours) of the laboratory
[CRIStAL](http://cristal.univ-lille.fr) use the **multiagent systems**
paradigm (MAS), in particular for complex distributed problem
solving. For this purpose, a MAS is composed of many entities which
aim at solving a complex problem which cannot be done by a single
entity. A MAS is characterized by the the fact that:

1. each agent has incomplete information and limited capabilities;

2. there is no central authority;

3. data are decentralized;

4. the computations are asynchronously performed.

In a MAS, the autonomy of agents allows the system to dynamically
adapt to the variations, eventually to the disturbances of their
environment. For this reason, we defend the thesis according to which
**MAS are particularly appropriate for adapting to unknown data, flows
that are constantly evolving and dynamic computing environments**.
    
## Scientific project

This project adopts an interdisciplinary approach, mixing Computer
Science and Economy, to identify:

* the challenges related to data management, including computing on
      large volumes of data and data-oriented parallelism;
* the challenges related to the knowledge discovery including
      information retrieval and complex queries on large data.

As part of the **MAS4Data** project we are working on the design of a
multiagent system implementing the MapReduce design pattern where the
dynamic allocation of tasks is based on negotiation between
reducers. **This system is dynamic, decentralized, with neither
history nor a priori knowledge of the data and it does not require a
parameterization dependent on the data**. Our MAS performs many
distributed and concurrent auctions which involve adaptive agents
taking decisions based on local information.

## References

<a name="1">Dean, J. and Ghemawat, S. (2008)</a>, Mapreduce: simplified data
processing on large clusters, Commun. ACM 51(1), 107–113.

<a name="2">Zaharia, M., Xin, R. S., Wendell, P., Das, T., Armbrust, M., Dave, A.,
Meng, X., Rosen, J., Venkataraman, S., Franklin, M. J., Ghodsi, A.,
Gonzalez, J., Shenker, S. and Stoica, I. (2016)</a>, Apache spark: a
unified engine for big data processing, Commun. ACM 59(11), 56– 65.

<a name="3">DeCandia, G., Hastorun, D., Jampani, M., Kakulapati, G.,
Lakshman, A., Pilchin, A., Sivasubramanian, S., Vosshall, P. and
Vogels, W. (2007)</a>, Dynamo: Amazon’s highly available key-value store,
in Proc. of the 21st ACM SIGOPS Symposium on Operating Systems
Principles (SOSP ’07), pp. 205–220.

## People

* Quentin BAERT, CRIStAL/Lille1 

* [Anne-Cécile CARON](http://www.lifl.fr/~caronc),  CRIStAL/Lille1 

* Virginie DELSART, Clersé/Lille1

* [Maxime MORGE](http://www.lifl.fr/~morge), CRIStAL/Lille1

* [Jean-Christophe ROUTIER](http://www.lifl.fr/~routier), CRIStAL/Lille1

* Nicolas VANEECLOO, Clersé/Lille1

## Publications

- *[Allocation équitable de tâches pour l'analyse de données massives](https://hal.archives-ouvertes.fr/hal-01383096).*
Quentin Baert, Anne-Cécile Caron, Maxime Morge, Jean-Christophe
Routier, dans les actes des 24èmes Journées Francophones sur les
Systèmes Multi-Agents 2016 (JFSMA'2016). Hermès. pp. 55-64. 2016.

- *[Fair Multi-Agent Task Allocation for Large Data Sets Analysis](https://hal.archives-ouvertes.fr/hal-01327522).*
Quentin Baert, Anne-Cécile Caron, Maxime Morge, Jean-Christophe Routier, in 14th
International Conference on Practical Applications of Agents and Multi-Agent
Systems (PAAMS'2016). LNAI 9662, pp. 24-35. 2016.

_Abstract_: Many companies are using MapReduce applications to pro- cess very
large amounts of data. Static optimization of such applications is complex
because they are based on user-defined operations, called map and reduce, which
prevents some algebraic optimization. In order to opti- mize the task
allocation, several systems collect data from previous runs and predict the
performance doing job profiling. However they are not effective during the
learning phase, or when a new type of job or data set appears. In this paper, we
present an adaptive multi-agent system for large data sets analysis with
MapReduce. We do not preprocess data and we adopt a dynamic approach, where the
reducer agents interact during the job. In order to decrease the workload of the
most loaded reducer - and so the execution time - we propose a task
re-allocation based on negotiation.


## Internal reports

- *[Fair Multi-Agent Task Allocation for Large Data Sets Analysis].*
Quentin Baert, Anne-Cécile Caron, Maxime Morge, Jean-Christophe Routier in
Knowledge and Information Systems (KAIS)

_Abstract_: MapReduce is a design pattern for processing large datasets
distributed on a cluster. Its performances are linked to the data structure and
the runtime environ- ment. Indeed, data skew can yield an unfair task
allocation, but even when the initial allocation produced by the partition
function is well balanced, an unfair allocation can occur during the reduce
phase due to the heterogeneous performance of nodes. For these reasons, we
propose an adaptive multi-agent system. In our approach, the re- ducer agents
interact during the job and the task re-allocation is based on negotiation in
order to decrease the workload of the most loaded reducer and so the runtime. In
this paper, we propose and evaluate two negotiation strategies. Finally, we
experiment our multi-agent system with real-world datasets over heterogeneous
runtime environment.


## Grant

This project is supported by the
[CNRS Challenge Mastodons](http://www.cnrs.fr/mi/spip.php?article53).
