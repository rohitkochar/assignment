Steps to use  Store
================
1. Create an object of type com.inmobi.assignment.StoreImpl
2. Call init on this object by passing config file path as the argument
3. Invoke get and put methods as required.
4. Call destroy on the same object to safely destroy the Store.


Assumptions And TradeOffs
=========================
1. Keys are comparable.This is needed for efficient storage
   in a file.
2. Total data stored in persistent store would at all time be less the maximum allowed size of the file.
3. After the data is transfered from memory to file,all the blocked threads are invoked.
	This could cause sudden spike in memory usage beyond threshold value.
	*TradeOff*:Alternative to this could be invoking blocked threads one by one which could have affected the performance
4. If multiple instances of Store are running under the same JVM than it is mandate to specify different paths of persistent storage file.
5. Null is not a valid value to be stored against any key.Storing null would behave as if the key doesn't exist.





Steps to run Test
=================
1. Execute mvn test to run all unit test
2. Execute mvn test -P coverage to run test along with clover code coverage.
3. Jmeter setup is required to run the load test.
   Load Test Plan is located at src/test/resources/Load Test Plan.jmx.
   Custom Jmeter samplers are located at src/test/com/inmobi/jmeter