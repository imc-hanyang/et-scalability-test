# EasyTrack Scalability Test

This repository documents the scalability assessment conducted for **EasyTrack**, evaluating its ability to handle large-scale concurrent data uploads under varying user loads and payload sizes. The experiments focus on system responsiveness and resource utilization in a distributed, cloud-based deployment.

---

## 1. Overview

The scalability test evaluates EasyTrack’s backend infrastructure under simulated real-world conditions, where large numbers of participants concurrently upload sensor and historical data. The primary objectives are to:

- Assess system behavior under increasing concurrent user loads  
- Measure server response time under parallel data uploads  
- Monitor CPU utilization to evaluate infrastructure stability  
- Validate EasyTrack’s suitability for large-scale mHealth and sensing studies  

---

## 2. Experimental Setup

### 2.1 Cloud Infrastructure

All experiments were conducted on **Microsoft Azure**, using a distributed and containerized architecture.

#### Database Layer
- **Database**: ScyllaDB  
- **Cluster size**: 9 nodes  
- **Deployment**: Distributed across 3 availability zones  
- **Storage**:  
  - 30 GB cloud SSD per node  
  - Configurable to support larger datasets if required  

This setup enables high-throughput, low-latency distributed writes.

#### Application Layer
- **Server**: gRPC-based backend services  
- **Orchestration**: Azure Kubernetes Service (AKS)  
- **Cluster size**: 9 nodes  
- **Pod configuration**:  
  - 2 gRPC server pods per node  
- **Load balancing**:  
  - Kubernetes built-in service load balancing  

---

### 2.2 Workload Simulation

Participant behavior was simulated using **Apache JMeter**, deployed via **Azure Load Testing**.

- **Geographic regions**:
  - Central US  
  - Germany West  
  - Central India  
  - East Asia  
  - Japan  
  - Australia East  

- **Concurrent user scale**:
  - 16 to 8192 users  

- **Upload behavior**:
  - All users upload data concurrently  
  - Parallel database write operations  

---

### 2.3 Data Payload Sizes

Two payload sizes were evaluated to reflect real-world sensing scenarios:

| Payload Size | Scenario Representation |
|-------------|--------------------------|
| 100 KB      | Real-time sensor data from wearables and smartphones |
| 1 MB        | Historical data or larger data types (e.g., audio, medical imaging, video) |

---

### 2.4 Collected Metrics

The following metrics were collected during each experiment:

- Average gRPC server response time  
- Average CPU utilization across the Kubernetes cluster  

---

## 3. Experimental Results

### 3.1 Workload Size: 100 KB

- **CPU Utilization**:
  - Stable across all tested user scales  
  - No significant increase under high concurrency  

- **Server Response Time**:
  - Stable up to 512 concurrent users  
  - Gradual increase thereafter  
  - Approximately 9 seconds at 8192 users  

The increase in response time is attributed to a large number of parallel database write operations.

---

### 3.2 Workload Size: 1 MB

- **CPU Utilization**:
  - Stable and consistent  
  - Below 20% up to 4096 concurrent users  

- **Server Response Time**:
  - Stable up to 1024 concurrent users  
  - Slight increase for 2048–4096 users (remaining under 10 seconds)  
  - Significant increase at 8192 users due to heavy parallel writes  

These results indicate that EasyTrack can efficiently manage larger workloads, though extreme concurrency may require further database or load-balancing optimizations.

---

## 4. Key Takeaways

- EasyTrack scales to thousands of concurrent users  
- CPU utilization remains stable across workloads  
- Response time degradation occurs primarily under extreme parallel write pressure  
- Suitable for large-scale mHealth and sensing deployments  

---

## 5. How to Reproduce

This section outlines the high-level steps required to reproduce the scalability experiments.

### 5.1 Prerequisites

- Microsoft Azure account with permissions to create:
  - Virtual machines
  - Azure Kubernetes Service (AKS)
  - Azure Load Testing resources
- Docker
- Kubernetes CLI (`kubectl`)
- Apache JMeter
- gRPC client configuration for EasyTrack

---

### 5.2 Deploy ScyllaDB Cluster

1. Provision 9 virtual machines across 3 availability zones.
2. Install and configure ScyllaDB on each node.
3. Configure the cluster with appropriate replication and partitioning settings.
4. Verify cluster health and connectivity.

---

### 5.3 Deploy gRPC Server on AKS

1. Create a 9-node AKS cluster.
2. Build the EasyTrack gRPC server Docker image.
3. Deploy the server using Kubernetes manifests:
   - Configure 2 pods per node
   - Enable Kubernetes service-based load balancing
4. Confirm that all pods are running and reachable.

---

### 5.4 Configure Load Testing

1. Create Apache JMeter test plans to simulate:
   - Concurrent user uploads
   - Payload sizes of 100 KB and 1 MB
2. Configure test plans for user scales ranging from 16 to 8192 users.
3. Upload JMeter scripts to Azure Load Testing.
4. Select global regions for test execution.

---

### 5.5 Run Experiments and Collect Metrics

1. Execute load tests for each combination of:
   - User count
   - Payload size
2. Monitor:
   - gRPC response times
   - Kubernetes cluster CPU utilization
3. Export metrics for analysis and visualization.

---

## 6. Notes on Reproducibility

- Ensure consistent cluster sizing across experiments.
- Avoid background workloads during testing.
- Use identical JMeter scripts across regions.
- Allow sufficient warm-up time before collecting metrics.

---

