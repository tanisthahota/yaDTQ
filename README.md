# YADTQ (Yet Another Distributed Task Queue)

YADTQ is a distributed task queue system built with Python, using Kafka as a message broker and Redis for result storage. It enables distributed task processing across multiple workers with reliable message delivery and task status tracking.

## Features

- Distributed task processing with multiple workers
- Task status tracking and result storage
- Reliable message delivery using Kafka
- Worker health monitoring via heartbeats
- Concurrent task submission and processing
- Simple client interface for task submission and status queries

## Architecture


The system consists of four main components:


1. **Message Broker (Kafka)**: Handles task distribution between clients and workers
2. **Workers**: Process tasks and update their status
3. **Result Backend (Redis)**: Stores task statuses and results
4. **Client Interface**: Submits tasks and queries their status

## Prerequisites

- Python 3.7+
- Apache Kafka
- Redis
- Required Python packages (install via pip):
 

## Tasks

1. Addition of two numbers
2. Subtraction of two numbers
3. Multiplication of two numbers
4. Division of two numbers


