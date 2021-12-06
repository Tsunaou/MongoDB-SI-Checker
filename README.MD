# MongoDB Snapshot Isolation Checker

This is the checker for our paper in [[2111.14946\] Verifying Transactional Consistency of MongoDB (arxiv.org)](https://arxiv.org/abs/2111.14946).

This project is responsible for **checking** whether a given execution history satisfies its corresponding snapshot isolation variant.

The execution history of can be found in:

- WiredTiger(Standalone) deployment: [Tsunaou/wiredtiger-txn](https://github.com/Tsunaou/wiredtiger-txn)
- MongoDB replica set and sharded cluster deployment: [Tsunaou/mongodb](https://github.com/Tsunaou/mongodb)

## 0. Update 

- The version v0.2  with proof will be announced in arxiv at Tue, 7 Dec 2021 01:00:00 GMT.

  <img src="./images/arxiv-scheduled.png" width="80%"/>

## 1. Introduction

MongoDB is a popular general-purpose, document-oriented, distributed NoSQL database. It supports transactions in three different deployments: single-document transactions utilizing the WiredTiger storage engine in a standalone node, multi-document transactions in a replica set which consists of a primary node and several secondary nodes, and distributed transactions in a sharded cluster which is a group of multiple replica sets, among which data is sharded. A natural and fundamental question about MongoDB transactions is: 

**What transactional consistency guarantee do MongoDB Transactions in each deployment provide?** 

However, it lacks both concise pseudocode of MongoDB transactions in each deployment and formal specification of the consistency guarantees which MongoDB claimed to provide. 

In this work, we formally specify and verify the transactional consistency protocols of MongoDB. Specifically, 

- **We provide a concise pseudocode for the transactional consistency protocols** in each MongoDB deployment, namely WIREDTIGER, REPLICASET, and SHARDEDCLUSTER, based on the official documents and source code. 
- **We then prove that WIREDTIGER, REPLICASET, and SHARDEDCLUSTER satisfy different variants of snapshot isolation**, namely Strong-SI, Realtime-SI, and Session-SI, respectively.

**We also propose and evaluate efficient white-box checking algorithms for MongoDB transaction protocols against their consistency guarantees**, effectively circumventing the NP-hard obstacle in theory.



