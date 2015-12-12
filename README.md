![Dagger](https://raw.githubusercontent.com/nsaje/nsaje.github.io/master/images/dagger.png)

Dagger is a dynamic realtime stream processing framework based on publishing of and subscribing to streams of data.

Computations are only executed on a stream (or streams) of data if there's someone actually subscribed to and using that computation. For example, a process that persists data streams to a DB is subscribed to a stream of CPU utilization for a node, averaged by a minute. This means a computation averaging CPU utilization over each minute will run as long as that process is running. When an administrator opens up a dashboard that would like to display a realtime chart of CPU utilization averaged over 5 seconds, a second computation will spin up that will supply average CPU utilization over 5 seconds. When the admin closes the dashboard, the computation shuts down, because there is no one interested in its data anymore.

This way, computation and data transfer over the network is only done when necessary, thus conserving compute cycles and reducing network traffic.

*This software is a prototype in the making. It is being developed for the purpose of my master's thesis.*

Properties
----------

* **Multilang plugins**

  Plugins for stream processing can be written in any language, interfaced via JSON-RPC.

* **Exactly once delivery**

  Ensured using retries and efficient deduplication.

* **Timestamp-ordered processing**

  Even when processing records from different streams, Dagger will process the records
   in the order of their timestamps. This is achieved using low watermarks, used also
   in Google's MillWheel stream processing system, which tell us when we have received
   all the records up to a certain point in time.

* **Fault tolerant**

  Achieved without using a separate database system, with multiple computations being executed in parallel enabling no-loss failover.

* **Kafka-like rewinds**

  Using the approach Kafka pioneered, Dagger persists all the streams to disk. This means you can decide from where on you want to receive stream data or even rewind to an
  earlier position in time.

* **Decentralized**

  No single point of failure. Workers are fail-fast and coordinate via a consistent KV store such as Consul or etcd.

* **Easy deployment**

  Written in Go, so all you need to deploy is a Consul or etcd cluster and a Go binary.


Dynamic work assignment example
-------------------------------

1: the monitored node has a 'cpu_util' stream available

    monitored node
    +---------------+
    |               |
    | pub: cpu_util |
    |               |
    +---------------+


2: a subscriber comes along that wants a stream of 'cpu_util' averaged by 5 minutes (say, a process that persists this to a DB)

    monitored node                                                        subscriber
    +---------------+                                                    +---------------------------+
    |               |                                                    |                           |
    | pub: cpu_util |                                                    | sub: avg(cpu_util, 5min)  |
    |               |                                                    |                           |
    +---------------+                                                    +---------------------------+


3: a Dagger worker spins up that starts computing this average and making it available for subscribers

    monitored node           worker node                                  subscriber
    +---------------+       +-----------------------------------+        +---------------------------+
    |               |       |                                   |        |                           |
    | pub: cpu_util +-------> computation: avg(cpu_util, 5min)  +--------> sub: avg(cpu_util, 5min)  |
    |               |       |                                   |        |                           |
    +---------------+       +-----------------------------------+        +---------------------------+


4: another subscriber shows up, this time wanting an average of 'cpu_util' over 10 seconds (say, an admin opens a web dashboard that displays live data)

    monitored node           worker node                                  subscriber
    +---------------+       +-----------------------------------+        +---------------------------+
    |               |       |                                   |        |                           |
    | pub: cpu_util +-------> computation: avg(cpu_util, 5min)  +--------> sub: avg(cpu_util, 5min)  |
    |               |       |                                   |        |                           |
    +---------------+       +-----------------------------------+        +---------------------------+

                                                                         +---------------------------+
                                                                         |                           |
                                                                         | sub: avg(cpu_util, 10sec) |
                                                                         |                           |
                                                                         +---------------------------+


5: a new worker is started, providing the required stream

    monitored node           worker node                                  subscriber
    +---------------+       +-----------------------------------+        +---------------------------+
    |               |       |                                   |        |                           |
    | pub: cpu_util +---+---> computation: avg(cpu_util, 5min)  +--------> sub: avg(cpu_util, 5min)  |
    |               |   |   |                                   |        |                           |
    +---------------+   |   +-----------------------------------+        +---------------------------+
                        |
                        |   +-----------------------------------+        +---------------------------+
                        |   |                                   |        |                           |
                        +---> computation: avg(cpu_util, 10sec) +--------> sub: avg(cpu_util, 10sec) |
                            |                                   |        |                           |
                            +-----------------------------------+        +---------------------------+


6: the second subscribers unsubscribes (say, the admin closes the dashboard)

    monitored node           worker node                                  subscriber
    +---------------+       +-----------------------------------+        +---------------------------+
    |               |       |                                   |        |                           |
    | pub: cpu_util +---+---> computation: avg(cpu_util, 5min)  +--------> sub: avg(cpu_util, 5min)  |
    |               |   |   |                                   |        |                           |
    +---------------+   |   +-----------------------------------+        +---------------------------+
                        |
                        |   +-----------------------------------+
                        |   |                                   |
                        +---> computation: avg(cpu_util, 10sec) |
                            |                                   |
                            +-----------------------------------+


7: a lack of subscribers is detected for the second worker so it shuts down

    monitored node           worker node                                  subscriber
    +---------------+       +-----------------------------------+        +---------------------------+
    |               |       |                                   |        |                           |
    | pub: cpu_util +-------> computation: avg(cpu_util, 5min)  +--------> sub: avg(cpu_util, 5min)  |
    |               |       |                                   |        |                           |
    +---------------+       +-----------------------------------+        +---------------------------+
