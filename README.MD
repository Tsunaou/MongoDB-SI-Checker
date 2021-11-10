# MongoDB Snapshot Isolation Checker



## 1. Algorithm in WiredTiger Layer

### 1.1 Visibility Relaiton: VIS = RB

A transaction $txn_1$ returns before $txn_2$ in real-time, denoted $txn_1 \stackrel{RB}{\longrightarrow} txn_2$, if
$$
commit(txn_1)<start(txn_2)
$$

### 1.2 Arbitray Relation.Relation: AR = CB

A transaction $txn_1$ commits before $txn_2$ in real-time, denoted $txn_1 \stackrel{CB}{\longrightarrow} txn_2$, if
$$
commit(txn_1)<commit(txn_2)
$$

### 1.3 Read From Relation.Relation: RF

A transaction $txn_1$ reads from $txn_2$, denoted $txn_1 \stackrel{RF}{\longrightarrow} txn_2$, if $txn_1$ writes $(key_i,val_i)$ and $txn_2$ reads $(key_i, val_i)$

 

### 1.4 Tid Before: TB

A transaction $txn_1$ tid before $txn_2$, denoted $txn_1 \stackrel{TB}\longrightarrow txn_2$, if $txn_1$ and $txn_2$ write the same key and $txn_1.tid < txn_2.tid$



## 2. Algorithm in Replica Set Layer

### 2.1 Visibility Relaiton

$$
\forall txn_1, txn_2 \in \mathsf{RS\_TXN}. txn_1 \stackrel{VIS_{RS}}{\longrightarrow} txn_2 
\Longleftrightarrow	
txn_1.\mathsf{commit\_ts} \le txn_2.\mathsf{read\_ts}
$$

### 2.2 Arbitray Relation

$$
\forall txn_1, txn_2 \in \mathsf{RS\_TXN}. txn_1 \stackrel{VIS_{AR}}{\longrightarrow} txn_2 
\Longleftrightarrow	
txn_1.\mathsf{commit\_ts} \le txn_2.\mathsf{commit\_ts}
$$

## 3. Algorithm in Sharded Cluster Layer

### 2.2 Visibility Relaiton

$$
\forall txn_1, txn_2 \in \mathsf{SC\_TXN}. txn_1 \stackrel{VIS_{SC}}{\longrightarrow} txn_2 
\Longleftrightarrow	
txn_1.\mathsf{commit\_ts} \le txn_2.\mathsf{read\_ts}
$$

### 2.2 Arbitray Relation

$$
\forall txn_1, txn_2 \in \mathsf{SC\_TXN}. txn_1 \stackrel{VIS_{AR}}{\longrightarrow} txn_2 
\Longleftrightarrow	
txn_1.\mathsf{commit\_ts} \le txn_2.\mathsf{commit\_ts}
$$

### 
