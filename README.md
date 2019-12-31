# MAS4Data: Multiagent systems for processing very large datasets

## Context

### MapReduce

Data science involves the processing of large volumes of data which
requires distributed file system and parallel programming. This
emerging distributed computing topic brings new challenges related to
task allocation and load-balancing for the adaptivity of those systems
to various settings without configuration requiring user expertise,
and their responsiveness to rapid changes.

We consider MapReduce which is the most prominent distributed data
processing model for tackling vast amount of data in parallel on
commodity clusters [(Dean and Ghemawat, 2008)](#1). MapReduce jobs are
divided into a set of map tasks and reduce tasks that are distributed
on nodes. The task allocation among the reducers is <i>a priori</i>
fixed by the partition function. For instance, the default partition
of the most popular implementation Hadoop is a modulo of the number of
reducers on the key hashcode. Such a task allocation can be
problematic. Firstly, several data skews in the MapReduce applications
lead to an unbalanced workload during the reduce phase[(Kwon et al.,
2013)](#2). Secondly, an unfair allocation can occur during the reduce
phase due to the heterogeneous performance of nodes. Thirdly, the
load-balancing can be challenged by some performance variations due to
exogenous reasons.

### Multiagent systems

In order to tackle the problem of load-balancing and task allocation
in applications such as those that motivate this work, multi-agent
technologies have received a lot of attention [(Jiang, 2016)](#3). The
SMAC team (Multiagent systems and behaviours) of the laboratory
[CRIStAL](http://cristal.univ-lille.fr) use the **multiagent systems**
paradigm (MAS), in particular for complex distributed problem solving.
A multi-agent system is a decentralized system where multiple agents
takes local decisions based on their perceptions of the environment
such that a solution to a complex problem can emerge from the
interactions between simple individual behaviours. Most of the
existing works adopting the market-based approach model the
load-balancing problem as a non-cooperative game in order to optimize
user-centric metrics rather than system-centric ones such as the
global runtime we consider here.

## Prototype

We provide a multi-agent system for task reallocation among
distributed nodes based on the location of the required resources to
perform these tasks in order to minimize the makespan.  In particular,
we apply our negotiation framework for load-balancing the reduce phase
in the distributed MapReduce model in order to process large datasets.

The dynamic and on-going task reallocation process takes place
concurrently with the task execution and so the distributed system is
adaptive to disruptive phenomena (task consumption, slowing down
nodes). Apart from decentralization, i.e. avoiding performance
bottlenecks due to global control, our multi-agent approach for
situated task allocation supports two additional crucial requirements
(a) concurrency -- where task reallocation and task executions are
concurrent, and (b) adaptation -- where task reallocation is triggered
when a disruptive event is performed. 

## References

<a name="1">Dean, J. and Ghemawat, S. (2008)</a>, Mapreduce: simplified data
processing on large clusters, Commun. ACM 51(1), 107–113.

<a name="2"> Y. Kwon, K. Ren, M. Balazinska, B. Howe, Managing skew in
Hadoop., IEEE Data Eng. Bull. 36 (1) (2013) 24–33.

<a name="3">Y. Jiang, A survey of task allocation and load balancing
in distributed systems, IEEE Transactions on Parallel and Distributed
Systems 27 (2) (2016) 585–599.

## Demonstration

The [video](https://youtu.be/KLRTV9Wf0rg) shows some examples of tasks allocation with our MAS. We
consider here real-world weather
[data](https://donneespubliques.meteofrance.fr/?fond=produit&id_produit=90&id_rubrique=32g)
and we count the number of records by temperature in the whole
dataset.

You can observe the task negotiations during the reducing phase. We
use here the default Hadoop partitioning as a reference point in
order to illustrate several features of our MAS:

1. The basic adaptation of our multiagent system, where each reducer
is a negotiating agent, improves the tasks allocation;

2. The extension of our multiagent system, where the tasks are
divisible, allows the negotiation of expensive tasks;

3. The multi-auctions extension, which allows reducers to be a bidder
in more than one auction at the same time, improves the efficiency of
the negotiation process.

Please note that in the video:

* the tasks with a black border are currently performed by the reducer
  while the tasks with a green border are already performed;

* the cheap tasks are graphically bigger than they should be in order
  to be see properly. Thus, the unfairness of the tasks allocation is
  a graphical effect.

## People

* Quentin BAERT, CRIStAL/Lille1

* [Anne-Cécile CARON](http://www.lifl.fr/~caronc),  CRIStAL/Lille1

* [Maxime MORGE](http://www.lifl.fr/~morge), CRIStAL/Lille1

* [Jean-Christophe ROUTIER](http://www.lifl.fr/~routier), CRIStAL/Lille1

* [Kostas STATHIS](https://pure.royalholloway.ac.uk/portal/en/persons/kostas-stathis(7f422719-142b-409c-97a8-f3efd9113f6d).html), RHUL

## Publications


- *[A Location-Aware Strategy for Agents Negotiating Load-balancing](https://hal.archives-ouvertes.fr/hal-02344457)*
Quentin Baert, Anne-Cécile Caron, Maxime Morge, Jean-Christophe
Routier, Kostas Stathis. in 31st International Conference on Tools
with Artificial Intelligence (ICTAI), Nov 2019, Portland, Oregon,
United States.

- *[Stratégie situationnelle pour l'équilibrage de charge](https://hal.archives-ouvertes.fr/hal-02173687)*
Quentin Baert, Anne-Cécile Caron, Maxime Morge, Jean-Christophe
Routier, Kostas Stathis. Dans Vingt-septièmes journées francophones
sur les systèmes multi-agents (JFSMA), Jul 2019, Toulouse,
France. pp.9-18.

- *[Adaptive Multi-agent System for Situated Task Allocation](https://hal.archives-ouvertes.fr/hal-02137346)*
Quentin Baert, Anne-Cécile Caron, Maxime Morge, Jean-Christophe
Routier, Kostas Stathis. Proceedings of the 18th International
Conference on Autonomous Agents and Multiagent Systems (AAMAS),
pp.1790-1792.


_Abstract_: We study a novel location-aware strategy for distributed
systems where cooperating agents perform the load-balancing. The
strategy allows agents to identify opportunities within a current
unbalanced allocation , which in turn triggers concurrent and
one-to-many negotiations amongst agents to locally reallocate some
tasks. The tasks are reallocated according to the proximity of the
resources and they are performed in accordance with the capabilities
of the nodes in which agents are situated. This dynamic and ongoing
negotiation process takes place concurrently with the task execution
and so the task allocation process is adaptive to disruptions (task
consumption, slowing down nodes). We evaluate the strategy in a
multi-agent deployment of the MapReduce design pattern for processing
large datasets. Empirical results demonstrate that our strategy
significantly improves the overall runtime of the data processing.


- *Negotiation Strategy of Divisible Tasks for Large Dataset Processing*
Quentin Baert, Anne-Cécile Caron, Maxime Morge, Jean-Christophe Routier in the 15th
European Conference on Multi-Agent Systems (EUMAS'2017). Evry, 14-15 December 2017.

_Abstract_: MapReduce is a design pattern for processing large datasets on a cluster. 
Its performances depend on some data skews and on the runtime environment. 
In order to tackle these problems, we propose an adaptive multiagent system. 
The agents interact during the data processing and the dynamic task allocation is the outcome of negotiations. 
These negotiations aim at improving the workload partition among the nodes within a cluster 
and so decrease the runtime of the whole process. 
Moreover, since the negotiations are iterative the system is responsive in case of node performance variations. 
In this full original paper, we show how, when a task is divisible, an agent may split it in order to negotiate its subtasks.

- *[Fair Multi-Agent Task Allocation for Large Data Sets Analysis.](https://doi.org/10.1007/s10115-017-1087-4)*
Quentin Baert, Anne-Cécile Caron, Maxime Morge, Jean-Christophe Routier in
Knowledge and Information System (KAIS) 2017.

_Abstract_: MapReduce is a design pattern for processing large datasets
distributed on a cluster. Its performances are linked to the data structure and
the runtime environ- ment. Indeed, data skew can yield an unfair task
allocation, but even when the initial allocation produced by the partition
function is well balanced, an unfair allocation can occur during the reduce
phase due to the heterogeneous performance of nodes. For these reasons, we
propose an adaptive multi-agent system. In our approach, the reducer agents
interact during the job and the task re-allocation is based on negotiation in
order to decrease the workload of the most loaded reducer and so the runtime. In
this paper, we propose and evaluate two negotiation strategies. Finally, we
experiment our multi-agent system with real-world datasets over heterogeneous
runtime environment.

- *[Stratégie de découpe de tâche pour le traitement de données massives](https://hal.archives-ouvertes.fr/hal-01558607v1)*
Quentin Baert, Anne-Cécile Caron, Maxime Morge, Jean-Christophe Routier, 
dans les actes des 25èmes Journées Francophones sur les
Systèmes Multi-Agents 2017 (JFSMA'2017). Hermès. pp. 65-75. 2017.

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
prevents some algebraic optimization. In order to optimize the task
allocation, several systems collect data from previous runs and predict the
performance doing job profiling. However they are not effective during the
learning phase, or when a new type of job or data set appears. In this paper, we
present an adaptive multi-agent system for large data sets analysis with
MapReduce. We do not preprocess data and we adopt a dynamic approach, where the
reducer agents interact during the job. In order to decrease the workload of the
most loaded reducer - and so the execution time - we propose a task
re-allocation based on negotiation.

## Grant

This project is supported by the
[CNRS Challenge Mastodons](http://www.cnrs.fr/mi/spip.php?article53).
