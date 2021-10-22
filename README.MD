# WiredTiger Snapshot Isolation Checker



## Algorithm

### Visibility Relaiton: VIS = RB

A transaction $txn_1$ returns before $txn_2$ in real-time, denoted $txn_1 \stackrel{RB}{\longrightarrow} txn_2$, if
$$
commit(txn_1)<commit(txn_2)
$$

### Arbitray Relation.Relation: AR = CB

A transaction $txn_1$ commits before $txn_2$ in real-time, denoted $txn_1 \stackrel{CB}{\longrightarrow} txn_2$, if
$$
commit(txn_1)<start(txn_2)
$$

## Read From Relation.Relation: RF

A transaction $txn_1$ reads from $txn_2$, denoted $txn_1 \stackrel{RF}{\longrightarrow} txn_2$, if $txn_1$ writes $(key_i,val_i)$ and $txn_2$ reads $(key_i, val_i)$

 

### Tid Before: TB

A transaction $txn_1$ tid before $txn_2$, denoted $txn_1 \stackrel{TB}\longrightarrow txn_2$, if $txn_1$ and $txn_2$ write the same key and $txn_1.tid < txn_2.tid$



