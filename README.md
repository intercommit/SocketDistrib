SocketDistrib
=============

The basis for a Java multi-threaded socket network server. 
Not just an example, but actually used in our 24x7 production systems.
It is small (5 average sized classes) and fast (using Java concurrent) 
and designed (see Wiki pages) with previous experiences with network server implementations in mind.

For an usage example, have a look at the test-method 
TestSleepSocket.testOneConnect() in nl.intercommit.socketdistrib.

The Socket Distributor also includes a reporter which logs statistics at regular intervals, e.g.:
{
2011-08-16 10:10:56,793 INFO  [SocketReporter] accepted: 52, processing: 1, processed: 52, workers: 8
2011-08-16 10:13:56,795 INFO  [SocketReporter] accepted: 0, processing: 0, processed: 0, workers: 4
}
Go to the Maven directory target/test-classes and execute "runtest" to see this in action.
"runtest" calls the main-method in nl.intercommit.socketdistrib.TestSleepSocket.
Change the parameters in the main-method to see how the Socket Distributor handles
broken connections, too many connections, shutdown, etc..
