Project 3 — Disk-Based B-Tree Index
CS 4348
Diya Mehta 

---


This project implements a B-Tree stored on disk using fixed-size 4096-byte blocks.  
The program supports creating an index file, inserting k/v pairs, search, bulk loading
from CSV, printing sorted k/v pairs, and extracting index back to CSV. 



---

Files in repo

Project3.java- main program  
devlog.md- development
README.md- instructions 


---

Compilation Instructions
  Run following 
  javac Project3.java
  javac Project3.java
java Project3 create test.idx
java Project3 insert test.idx 15 100
java Project3 search test.idx 15
java Project3 load test.idx input.csv
java Project3 print test.idx
java Project3 extract test.idx output.csv
