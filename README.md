# Simple-Dynamo DB
There are mainly three components of the Project <br/>
i) Partition <br/>
ii) Replication <br/>
iii) Failure handling <br/>

Main Goal of the project to provide linearizability and availability at the same time. It means a read operation should always return the most recent value and It also perfoms read and write operations successfully under the case of failure.
