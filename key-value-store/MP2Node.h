/**********************************
 * FILE NAME: MP2Node.h
 *
 * DESCRIPTION: MP2Node class header file
 **********************************/

#ifndef MP2NODE_H_
#define MP2NODE_H_

/**
 * Header files
 */
#include <unordered_map>
#include "EmulNet.h"
#include "HashTable.h"
#include "Log.h"
#include "Message.h"
#include "Node.h"
#include "Params.h"
#include "Queue.h"
#include "stdincludes.h"

// Reply timeout
#define TFAIL 5
// transID used in stabilizationProtocol
#define STABLE -1

/**
 * STRUCT NAME: Transaction
 *
 * DESCRIPTION: Transaction information
 */
class Transaction {
 public:
  string key;
  string value;
  enum MessageType type;
  int timestamp;
  int replyCount;
  int successCount;
  Transaction(string key, string value, MessageType type, int timestamp,
              int replyCount, int successCount);
};

/**
 * CLASS NAME: HTValue
 *
 * DESCRIPTION: This class encapsulates the object stored by hashmap value
 */
class HTValue {
 public:
  string value;
  ReplicaType replica;
  // delimiter
  string delimiter;
  // construct HTValue from a string
  HTValue(string str);
  // construct HTValue from value and replica type
  HTValue(string value, ReplicaType replica);
  // serialze to a string
  string toString();
};

/**
 * CLASS NAME: MP2Node
 *
 * DESCRIPTION: This class encapsulates all the key-value store functionality
 * 				including:
 * 				1) Ring
 * 				2) Stabilization Protocol
 * 				3) Server side CRUD APIs
 * 				4) Client side CRUD APIs
 */
class MP2Node {
 private:
  // Vector holding the next two neighbors in the ring who have my replicas
  vector<Node> hasMyReplicas;
  // Vector holding the previous two neighbors in the ring whose replicas I have
  vector<Node> haveReplicasOf;
  // Ring
  vector<Node> ring;
  // Hash Table
  HashTable *ht;
  // Member representing this member
  Member *memberNode;
  // Params object
  Params *par;
  // Object of EmulNet
  EmulNet *emulNet;
  // Object of Log
  Log *log;
  // HashMap holding transactions
  unordered_map<int, Transaction *> transactions;

 public:
  MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log,
          Address *addressOfMember);
  Member *getMemberNode() { return this->memberNode; }

  // ring functionalities
  void updateRing();
  vector<Node> getMembershipList();
  size_t hashFunction(string key);
  void findNeighbors();

  // client side CRUD APIs
  void clientCreate(string key, string value);
  void clientRead(string key);
  void clientUpdate(string key, string value);
  void clientDelete(string key);

  // receive messages from Emulnet
  bool recvLoop();
  static int enqueueWrapper(void *env, char *buff, int size);

  // handle messages from receiving queue
  void checkMessages();

  // check the status of each transaction
  void checkTransactions();

  // log coordinator operation result
  void coordinatorLog(bool success, MessageType type, int transID, string key,
                      string value);

  // send reply message
  void sendReply(int transID, Address *toAddr, MessageType type, bool success,
                 string value);

  // coordinator dispatches messages to corresponding nodes
  void dispatchMessages(Message message);

  // find the addresses of nodes that are responsible for a key
  vector<Node> findNodes(string key);

  // server
  bool createKeyValue(string key, string value, ReplicaType replica);
  string readKey(string key);
  bool updateKeyValue(string key, string value, ReplicaType replica);
  bool deleteKey(string key);

  // stabilization protocol - handle multiple failures
  void stabilizationProtocol();

  ~MP2Node();
};

#endif /* MP2NODE_H_ */
