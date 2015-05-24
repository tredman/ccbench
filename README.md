# ccbench

## What

ccbench or "concurrency benchmark" - a tool for evaluating how query latency is affected by concurrent requests, document size, or number of documents scanned.

## Why?

Application A resides in database A. Application B resides in database B. At what point does activity in A start to affect latency in B? Is this influenced more by increased concurrent workers, larger documents, or by scanning more documents?
