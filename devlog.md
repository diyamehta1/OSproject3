# Dev Log — Project 3 (B-Tree Index)
CS 4348 — Operating Systems

---

## Session 1 dec 8 2025
-Read instructions, identified btree w 4 kb blocks are required 
-Noted required commands
-Decided language
-Planned data structures 
-Created project skeleton constants etc 
-commit 


---

## Session 2 — dec 9 2025
- Implemented the file header:
  - Magic string "4348PRJ3"
  - Root block id
  - Next free block id
- Implemented fixed 4096-byte blocks using ByteBuffer.
- Implemented reading/writing the header consistently in big-endian format.
- Created basic BTree class with constructor that loads header fields.

---

## Session 3 — node struct + Insert dec 10 2025 
- Implemented node layout
- Wrote serialize/deserialize for node blocks
- Added node splitting when number of keys exceeds 15
- Verified insert logic for:
  - leaf insertion 
  - Internal node split + promotion
  - root split creation
- successfully inserted multiple keys manually.

---

## Session 4 — Search, Load, Print, Extract dec 10 2025
- implemented search traversal down B-Tree 
- implemented print in key order using recursive traversal 
- implemented load using CSV reading + repeated inserts
- implemented extract by writing sorted key/value pairs back to CSV
- added error handling for missing files, invalid commands, malformed input

---

## Session 5 —  dec 10 2025
- added main dispatcher
- troubleshooted 

---

