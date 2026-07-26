1. Reduce state in code
2. Design function signatures/interfaces like a library for endusers and for testing
3. functions should minimally do one thing or concept as much as possible (mainly for testing)
4. make it simpler
5. panic on unexpected cases and address them as they occur. That said only one panic per file
6. Minimum of five logs within a function, otherwise make logging configurable via envs
7. no more than 4 if-else statements within a sequential flow of function if not use switches
    - if inside an for-loop statement, only one if statement is allowed, otherwise switch
    - inside a case statement, only one if statement is allowed


# These are from the Tiger Style coding standard
Zero Technical Debt
---
Code, like steel, is easier to change while it's hot. Do it right the first time, the best you 
know how, because you may not get another chance, and because quality builds momentum. This is 
the only way to make steady progress, knowing that the foundations are solid.


PERFORMANCE
---
The lack of back-of-the-envelope sketches is the root of all evil.

Think about performance from the outset. The time to solve performance, and get the 1000x wins,
is in the design phase, when you can't profile. It's hard to fix a system after implementation, 
and the gains are less. Have mechanical sympathy. Like a carpenter, work with the grain.

Primary Colors
---
Like a painter, perform back-of-the-envelope performance sketches with respect
to the four primary colors (network, storage, memory, compute) and their two
textures (bandwidth, latency) to be “roughly right” and land within 90% of the
global maxima.
