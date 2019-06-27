/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor from string for HTValue
 */
Transaction::Transaction(string key, string value, MessageType type,
                         int timestamp, int replyCount, int successCount) {
  this->key = key;
  this->value = value;
  this->type = type;
  this->timestamp = timestamp;
  this->replyCount = replyCount;
  this->successCount = successCount;
}

// /**
//  * constructor for HTValue
//  */
// HTValue::HTValue(string value, ReplicaType replica) {
//   this->delimiter = "::";
//   this->value = value;
//   this->replica = replica;
// }

// /**
//  * constructor from string for HTValue
//  */
// HTValue::HTValue(string str) {
//   this->delimiter = "::";
//   size_t pos = str.find(delimiter);
//   this->value = str.substr(0, pos);
//   this->replica = static_cast<ReplicaType>(stoi(str.substr(pos + 2)));
// }

/**
 * FUNCTION NAME: toString
 *
 * DESCRIPTION: Serialized HTValue in string format
 */
string HTValue::toString() { return value + delimiter + to_string(replica); }

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log,
                 Address *address) {
  this->memberNode = memberNode;
  this->par = par;
  this->emulNet = emulNet;
  this->log = log;
  ht = new HashTable();
  this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
  delete ht;
  delete memberNode;
  for (auto i = transactions.begin(); i != transactions.end(); i++) {
    delete i->second;
  }
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the
 * Membership Protocol (MP1Node) The membership list is returned as a vector of
 * Nodes. See Node class in Node.h 2) Constructs the ring based on the
 * membership list 3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
  /*
   * Implement this. Parts of it are already implemented
   */
  vector<Node> curMemList;
  bool change = false;

  /*
   *  Step 1. Get the current membership list from Membership Protocol / MP1
   */
  curMemList = getMembershipList();
  Node thisNode(memberNode->addr);
  curMemList.emplace_back(thisNode);

  /*
   * Step 2: Construct the ring
   */
  // Sort the list based on the hashCode
  sort(curMemList.begin(), curMemList.end());

  /*
   * Step 3: Run the stabilization protocol IF REQUIRED
   */
  // Run stabilization protocol if the hash table size is greater than zero and
  // if there has been a changed in the ring
  if (!ht->isEmpty()) {
    if (ring.size() != curMemList.size()) {
      change = true;
    } else {
      for (int i = 0; i < ring.size(); i++) {
        if (!(ring.at(i).nodeAddress == curMemList.at(i).nodeAddress)) {
          change = true;
          break;
        }
      }
    }
  }
  ring = curMemList;
  if (change) {
    stabilizationProtocol();
  }
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the
 * Membership protocol/MP1 and i) generates the hash code for each member ii)
 * populates the ring member in MP2Node class It returns a vector of Nodes. Each
 * element in the vector contain the following fields: a) Address of the node b)
 * Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
  unsigned int i;
  vector<Node> curMemList;
  for (i = 0; i < this->memberNode->memberList.size(); i++) {
    Address addressOfThisMember;
    int id = this->memberNode->memberList.at(i).getid();
    short port = this->memberNode->memberList.at(i).getport();
    memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
    memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
    curMemList.emplace_back(Node(addressOfThisMember));
  }
  return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the
 * ring HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
  std::hash<string> hashFunc;
  size_t ret = hashFunc(key);
  return ret % RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
  /*
   * Implement this
   */
  Transaction *trans =
      new Transaction(key, value, CREATE, par->getcurrtime(), 0, 0);
  transactions.emplace(g_transID, trans);
  Message msg(g_transID, memberNode->addr, CREATE, key, value);
  g_transID++;
  dispatchMessages(msg);
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key) {
  /*
   * Implement this
   */
  Transaction *trans = new Transaction(key, "", READ, par->getcurrtime(), 0, 0);
  transactions.emplace(g_transID, trans);
  Message msg(g_transID, memberNode->addr, READ, key);
  g_transID++;
  dispatchMessages(msg);
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value) {
  /*
   * Implement this
   */
  Transaction *trans =
      new Transaction(key, value, UPDATE, par->getcurrtime(), 0, 0);
  transactions.emplace(g_transID, trans);
  Message msg(g_transID, memberNode->addr, UPDATE, key, value);
  g_transID++;
  dispatchMessages(msg);
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key) {
  /*
   * Implement this
   */
  Transaction *trans =
      new Transaction(key, "", DELETE, par->getcurrtime(), 0, 0);
  transactions.emplace(g_transID, trans);
  Message msg(g_transID, memberNode->addr, DELETE, key);
  g_transID++;
  dispatchMessages(msg);
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or
 * failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
  /*
   * Implement this
   */
  // Insert key, value, replicaType into the hash table
  return ht->create(key, value);
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
  /*
   * Implement this
   */
  // Read key from local hash table and return value
  return ht->read(key);
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local
 * hash table 2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
  /*
   * Implement this
   */
  // Update key in local hash table and return true or false
  return ht->update(key, value);
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or
 * failure
 */
bool MP2Node::deleteKey(string key) {
  /*
   * Implement this
   */
  // Delete the key from the local hash table
  return ht->deleteKey(key);
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message
 * types
 */
void MP2Node::checkMessages() {
  /*
   * Implement this. Parts of it are already implemented
   */
  char *data;
  int size;

  /*
   * Declare your local variables here
   */

  // dequeue all messages and handle them
  while (!memberNode->mp2q.empty()) {
    /*
     * Pop a message from the queue
     */
    data = (char *)memberNode->mp2q.front().elt;
    size = memberNode->mp2q.front().size;
    memberNode->mp2q.pop();

    string message(data, data + size);

    Message recvMsg(message);

    /*
     * Handle the message types here
     */
    switch (recvMsg.type) {
      case CREATE: {
        if (recvMsg.transID != STABLE) {
          bool success =
              createKeyValue(recvMsg.key, recvMsg.value, recvMsg.replica);
          if (success) {
            log->logCreateSuccess(&memberNode->addr, false, recvMsg.transID,
                                  recvMsg.key, recvMsg.value);
          } else {
            log->logCreateFail(&memberNode->addr, false, recvMsg.transID,
                               recvMsg.key, recvMsg.value);
          }
          sendReply(recvMsg.transID, &recvMsg.fromAddr, REPLY, success, "");
        } else {
          if (ht->read(recvMsg.key) == "") {
            createKeyValue(recvMsg.key, recvMsg.value, recvMsg.replica);
          }
        }
        break;
      }
      case READ: {
        string value = readKey(recvMsg.key);
        if (value != "") {
          log->logReadSuccess(&memberNode->addr, false, recvMsg.transID,
                              recvMsg.key, value);
        } else {
          log->logReadFail(&memberNode->addr, false, recvMsg.transID,
                           recvMsg.key);
        }
        sendReply(recvMsg.transID, &recvMsg.fromAddr, READREPLY, true, value);
        break;
      }
      case UPDATE: {
        bool success =
            updateKeyValue(recvMsg.key, recvMsg.value, recvMsg.replica);
        if (success) {
          log->logUpdateSuccess(&memberNode->addr, false, recvMsg.transID,
                                recvMsg.key, recvMsg.value);
        } else {
          log->logUpdateFail(&memberNode->addr, false, recvMsg.transID,
                             recvMsg.key, recvMsg.value);
        }
        sendReply(recvMsg.transID, &recvMsg.fromAddr, REPLY, success, "");
        break;
      }
      case DELETE: {
        bool success = deleteKey(recvMsg.key);
        if (success) {
          log->logDeleteSuccess(&memberNode->addr, false, recvMsg.transID,
                                recvMsg.key);
        } else {
          log->logDeleteFail(&memberNode->addr, false, recvMsg.transID,
                             recvMsg.key);
        }
        sendReply(recvMsg.transID, &recvMsg.fromAddr, REPLY, success, "");
        break;
      }
      case REPLY: {
        if (transactions.find(recvMsg.transID) == transactions.end()) {
          break;
        }
        Transaction *trans = transactions[recvMsg.transID];
        trans->replyCount++;
        if (recvMsg.success) {
          trans->successCount++;
        }
        break;
      }
      case READREPLY: {
        if (transactions.find(recvMsg.transID) == transactions.end()) {
          break;
        }
        Transaction *trans = transactions[recvMsg.transID];
        trans->replyCount++;
        if (recvMsg.value != "") {
          trans->value = recvMsg.value;
          trans->successCount++;
        }
        break;
      }
    }
  }

  /*
   * This function should also ensure all READ and UPDATE operation
   * get QUORUM replies
   */
  checkTransactions();
}

/**
 * FUNCTION NAME: checkTransactions
 *
 * DESCRIPTION: Check the status of each transaction
 */
void MP2Node::checkTransactions() {
  for (auto i = transactions.begin(); i != transactions.end();) {
    Transaction *trans = i->second;
    bool complete = false;
    if (trans->successCount >= 2) {
      coordinatorLog(true, trans->type, i->first, trans->key, trans->value);
      complete = true;
    } else if (trans->replyCount - trans->successCount >= 2 ||
               par->getcurrtime() - trans->timestamp > TFAIL) {
      coordinatorLog(false, trans->type, i->first, trans->key, trans->value);
      complete = true;
    }
    if (complete) {
      delete i->second;
      i = transactions.erase(i);
    } else {
      i++;
    }
  }
}

/**
 * FUNCTION NAME: coordinatorLog
 *
 * DESCRIPTION: Log coordinator operation result
 */
void MP2Node::coordinatorLog(bool success, MessageType type, int transID,
                             string key, string value) {
  switch (type) {
    case CREATE: {
      if (success) {
        log->logCreateSuccess(&memberNode->addr, true, transID, key, value);
      } else {
        log->logCreateFail(&memberNode->addr, true, transID, key, value);
      }
      break;
    }
    case READ: {
      if (success) {
        log->logReadSuccess(&memberNode->addr, true, transID, key, value);
      } else {
        log->logReadFail(&memberNode->addr, true, transID, key);
      }
      break;
    }
    case UPDATE: {
      if (success) {
        log->logUpdateSuccess(&memberNode->addr, true, transID, key, value);
      } else {
        log->logUpdateFail(&memberNode->addr, true, transID, key, value);
      }
      break;
    }
    case DELETE: {
      if (success) {
        log->logDeleteSuccess(&memberNode->addr, true, transID, key);
      } else {
        log->logDeleteFail(&memberNode->addr, true, transID, key);
      }
      break;
    }
  }
}

/**
 * FUNCTION NAME: sendReply
 *
 * DESCRIPTION: Send the reply message
 */
void MP2Node::sendReply(int transID, Address *toAddr, MessageType type,
                        bool success, string value) {
  if (type == READREPLY) {
    Message rep(transID, memberNode->addr, value);
    emulNet->ENsend(&memberNode->addr, toAddr, rep.toString());
  } else {
    Message rep(transID, memberNode->addr, REPLY, success);
    emulNet->ENsend(&memberNode->addr, toAddr, rep.toString());
  }
}

/**
 * FUNCTION NAME: dispatchMessages
 *
 * DESCRIPTION: Send the message
 */
void MP2Node::dispatchMessages(Message message) {
  vector<Node> addr_vec = findNodes(message.key);
  for (auto i = addr_vec.begin(); i != addr_vec.end(); i++) {
    emulNet->ENsend(&memberNode->addr, &i->nodeAddress, message.toString());
  }
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the
 * replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
  size_t pos = hashFunction(key);
  vector<Node> addr_vec;
  if (ring.size() >= 3) {
    // if pos <= min || pos > max, the leader is the min
    if (pos <= ring.at(0).getHashCode() ||
        pos > ring.at(ring.size() - 1).getHashCode()) {
      addr_vec.emplace_back(ring.at(0));
      addr_vec.emplace_back(ring.at(1));
      addr_vec.emplace_back(ring.at(2));
    } else {
      // go through the ring until pos <= node
      for (int i = 1; i < ring.size(); i++) {
        Node addr = ring.at(i);
        if (pos <= addr.getHashCode()) {
          addr_vec.emplace_back(addr);
          addr_vec.emplace_back(ring.at((i + 1) % ring.size()));
          addr_vec.emplace_back(ring.at((i + 2) % ring.size()));
          break;
        }
      }
    }
  }
  return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
  if (memberNode->bFailed) {
    return false;
  } else {
    return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1,
                           &(memberNode->mp2q));
  }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
  Queue q;
  return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and
 *leaves It ensures that there always 3 copies of all keys in the DHT at all
 *times The function does the following: 1) Ensures that there are three
 *"CORRECT" replicas of all the keys in spite of failures and joins Note:-
 *"CORRECT" replicas implies that every key is replicated in its two neighboring
 *nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
  /*
   * Implement this
   */
  if (ring.size() < 3) {
    return;
  }
  for (auto i = ht->hashTable.begin(); i != ht->hashTable.end(); i++) {
    Message msg(STABLE, memberNode->addr, CREATE, i->first, i->second);
    dispatchMessages(msg);
  }
}
