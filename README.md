## [Optimizing Distributed Protocols with Query Rewrites](https://dl.acm.org/doi/pdf/10.1145/3639257)
David C. Y. Chu, Rithvik Panchapakesan, Shadaj Laddad, Lucky Katahanas, Chris Liu, Kaushik Shivakumar, Natacha Crooks, Joseph M. Hellerstein, Heidi Howard  
_SIGMOD '24_  
[`Technical Report`](https://arxiv.org/abs/2404.01593)

## Bigger, not Badder: Safely Scaling Byzantine Protocols
David C. Y. Chu, Chris Liu, Natacha Crooks, Joseph M. Hellerstein, Heidi Howard  
_PaPoC '24_  
[`Paper`](https://github.com/rithvikp/autocomp/blob/master/benchmarks/vldb24/Evil_Serverless_Consensus__PaPoC_2024.pdf)

---
This repository contains the implementations of various state machine replication protocols in both Scala and Dedalus. It was initially forked from [frankenpaxos](https://github.com/mwhittaker/frankenpaxos) by Michael Whittaker.

Refer to the following on how to use this codebase:
- [Setup](SETUP.md)
- [Protocols](PROTOCOLS.md)
- [It doesn't work! (FAQ)](FAQ.md)

Although most of the example benchmark scripts in this repository are located in `benchmarks/vldb24`, they are not for the VLDB '24 submission. The folder was named after our first (rejected) submission and has been modified since.
