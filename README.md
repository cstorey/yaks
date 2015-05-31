YakMQ
=====

This is intended to be an experimental message bus with a defined consistency model, albeit more inspired by Kafka than RabbitMQ.

I'm eventually aiming for a Causal+ consistency model (see the ChainReaction paper), but it seems wise to start out with a simpler model, following the original Chain replication paper.

Inspiration:

 * [Chain Replication for Supporting
High Throughput and Availability](http://www.cs.cornell.edu/fbs/publications%5CChainReplicOSDI.pdf)
 * [Chain Replication in Theory and in Practice](http://www.snookles.com/scott/publications/erlang2010-slf.pdf)
 * [Object storage on CRAQ](https://www.usenix.org/legacy/event/usenix09/tech/full_papers/terrace/terrace.pdf)
 * [ChainReaction](http://eurosys2013.tudos.org/wp-content/uploads/2013/paper/Almeida.pdf)
 * [The Potential Dangers of Causal Consistency
and an Explicit Solution](http://db.cs.berkeley.edu/papers/socc12-explicit.pdf)
