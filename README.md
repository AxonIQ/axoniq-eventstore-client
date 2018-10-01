# AxonDB client modules
Contains public interface definitions, client code and sample programs for the AxonDB

## Version history

### 1.0

First release

### 1.1 

Query API in client

### 1.2.1 
   
API extensions for AxonHub integration

### 1.2.3

   - Keep-alive between client and server
   - maintain connection between client and server even if there are no requests to 
     detect lost connections.
   - Simplified servers property to use default port when omitted
   - Commit timeout configurable
   - Improved support for configuration properties in IDE when using spring-boot
   
### 1.3

   - Implementation of new AxonFramework 3.3 operations
   
### 1.3.1

- Fix for missing upcaster
- Resolved high memory consumption in EventBuffer when event stream was closed by client
- Performance improvement in readEvents
   
### 1.3.5

- changed connect thread to daemon thread
- aligned client version with latest server version