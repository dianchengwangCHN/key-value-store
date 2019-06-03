/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log,
                 Address *address) {
  for (int i = 0; i < 6; i++) {
    NULLADDR[i] = 0;
  }
  this->memberNode = member;
  this->emulNet = emul;
  this->log = log;
  this->par = params;
  this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into
 * the queue This function is called by a node to receive messages currently
 * waiting for it
 */
int MP1Node::recvLoop() {
  if (memberNode->bFailed) {
    return false;
  } else {
    return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1,
                           &(memberNode->mp1q));
  }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
  Queue q;
  return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
  Address joinaddr;
  joinaddr = getJoinAddress();

  // Self booting routines
  if (initThisNode(&joinaddr) == -1) {
#ifdef DEBUGLOG
    log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
    exit(1);
  }

  if (!introduceSelfToGroup(&joinaddr)) {
    finishUpThisNode();
#ifdef DEBUGLOG
    log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
    exit(1);
  }

  return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
  /*
   * This function is partially implemented and may require changes
   */
  int id = *(int *)(&memberNode->addr.addr);
  int port = *(short *)(&memberNode->addr.addr[4]);

  memberNode->bFailed = false;
  memberNode->inited = true;
  memberNode->inGroup = false;
  // node is up!
  memberNode->nnb = 0;
  memberNode->heartbeat = 0;
  memberNode->pingCounter = TFAIL;
  memberNode->timeOutCounter = -1;
  initMemberListTable(memberNode);

  return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
  MessageHdr *msg;
#ifdef DEBUGLOG
  static char s[1024];
#endif

  if (0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr),
                  sizeof(memberNode->addr.addr))) {
    // I am the group booter (first process to join the group). Boot up the
    // group
#ifdef DEBUGLOG
    log->LOG(&memberNode->addr, "Starting up group...");
#endif
    memberNode->inGroup = true;
  } else {
    size_t msgsize =
        sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
    msg = (MessageHdr *)malloc(msgsize * sizeof(char));

    // create JOINREQ message: format of data is {struct Address myaddr}
    msg->msgType = JOINREQ;
    memcpy((char *)(msg + 1), &memberNode->addr.addr,
           sizeof(memberNode->addr.addr));
    memcpy((char *)(msg + 1) + 1 + sizeof(memberNode->addr.addr),
           &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
    sprintf(s, "Trying to join...");
    log->LOG(&memberNode->addr, s);
#endif

    // send JOINREQ message to introducer member
    emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

    free(msg);
  }

  return 1;
}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode() {
  /*
   * Your code goes here
   */
  memberNode->inited = true;
  memberNode->bFailed = false;
  memberNode->inGroup = false;
  memberNode->nnb = 0;
  memberNode->heartbeat = 0;
  memberNode->pingCounter = TFAIL;
  memberNode->timeOutCounter = -1;
  initMemberListTable(memberNode);

  return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform
 * membership protocol duties
 */
void MP1Node::nodeLoop() {
  if (memberNode->bFailed) {
    return;
  }

  // Check my messages
  checkMessages();

  // Wait until you're in the group...
  if (!memberNode->inGroup) {
    return;
  }

  // ...then jump in and share your responsibilites!
  nodeLoopOps();

  return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message
 * handler
 */
void MP1Node::checkMessages() {
  void *ptr;
  int size;

  // Pop waiting messages from memberNode's mp1q
  while (!memberNode->mp1q.empty()) {
    ptr = memberNode->mp1q.front().elt;
    size = memberNode->mp1q.front().size;
    memberNode->mp1q.pop();
    recvCallBack((void *)memberNode, (char *)ptr, size);
  }
  return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size) {
  /*
   * Your code goes here
   */
  MessageHdr *message = (MessageHdr *)malloc(size * sizeof(char));
  memcpy(message, data, sizeof(MessageHdr));

  if (message->msgType == JOINREQ) {
  } else if (message->msgType == JOINREP) {
  } else if (message->msgType == HEARTBEAT) {
  }
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and
 * then delete the nodes Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
  /*
   * Your code goes here
   */
  // Check if any node in memberList failed
  long now = par->getcurrtime();
  for (auto i = memberNode->memberList.begin();
       i != memberNode->memberList.end(); i++) {
    long timeStamp = i->gettimestamp();
    Address memberAddr;
    memberAddr = getMemberAddress(i->getid, i->getport);
    if (timeStamp - now > TFAIL + TREMOVE) {
      i = memberNode->memberList.erase(i);

#ifdef DEBUGLOG
      log->logNodeRemove(&memberNode->addr, &memberAddr);
#endif
    }
  }
  // Send heartbeat messages periodically
  memberNode->pingCounter--;
  if (memberNode->pingCounter == 0) {
    memberNode->heartbeat++;
    // Send heartbeat messages to members in memberList
    for (auto i = memberNode->memberList.begin();
         i != memberNode->memberList.end(); i++) {
      Address memberAddr;
      memberAddr = getMemberAddress(i->getid, i->getport);

      long timeStamp = i->gettimestamp();

      if (!(now - timeStamp > TFAIL || memberAddr == memberNode->addr)) {
        // send HEARTBEAT message
        sendHeartbeat(&memberAddr);
      }
    }
    memberNode->pingCounter = TFAIL;
  }

  return;
}

/**
 * FUNCTION NAME: sendHeartbeat
 *
 * DESCRIPTION: Function sends heatbeat to dstAddr
 */
int MP1Node::sendHeartbeat(Address *dstAddr) {
  MessageHdr *msg;
  size_t msgsize =
      sizeof(MessageHdr) + sizeof(memberNode->addr.addr) + sizeof(long);
  msg = (MessageHdr *)malloc(msgsize * sizeof(char));

  msg->msgType = HEARTBEAT;
  memcpy((char *)(msg + 1), &memberNode->addr.addr,
         sizeof(memberNode->addr.addr));
  memcpy((char *)(msg + 1) + sizeof(memberNode->addr.addr),
         &memberNode->heartbeat, sizeof(long));

  // send HEARTBEAT message to the member
  emulNet->ENsend(&memberNode->addr, dstAddr, (char *)msg, msgsize);
  free(msg);

  return 0;
}

/**
 * FUNCTION NAME: sendJoinResponse
 *
 * DESCRIPTION: Function send response to JOINREQ
 */
int MP1Node::sendJoinResponse(Address *dstAddr) {
  MessageHdr *msg;
  size_t entrySize = sizeof(int) + sizeof(short) + 2 * sizeof(long);
  int size = memberNode->memberList.size() + 1;
  size_t msgsize = sizeof(MessageHdr) + sizeof(int) + size * entrySize;
  msg = (MessageHdr *)malloc(msgsize * sizeof(char));

  msg->msgType = JOINREP;
  memcpy((char *)(msg + 1), &size, sizeof(int));
  int offset = sizeof(int);
  memcpy((char *)(msg + 1) + offset, &memberNode->addr.addr, sizeof(char) * 6);
  offset += sizeof(char) * 6;
  memcpy((char *)(msg + 1) + offset, &memberNode->heartbeat, sizeof(long));
  offset += sizeof(long);
  memcpy((char *)(msg + 1) + offset, &par->globaltime, sizeof(long));
  offset += sizeof(long);
  for (auto i = memberNode->memberList.begin();
       i != memberNode->memberList.end(); i++) {
    memcpy((char *)(msg + 1) + offset, &i->id, sizeof(int));
    offset += sizeof(int);

    memcpy((char *)(msg + 1) + offset, &i->port, sizeof(short));
    offset += sizeof(short);

    memcpy((char *)(msg + 1) + offset, &i->heartbeat, sizeof(long));
    offset += sizeof(long);

    memcpy((char *)(msg + 1) + offset, &i->timestamp, sizeof(long));
    offset += sizeof(long);
  }

  // send HEARTBEAT message to the member
  emulNet->ENsend(&memberNode->addr, dstAddr, (char *)msg, msgsize);
  free(msg);

  return 0;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
  return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
  Address joinaddr;

  memset(&joinaddr, 0, sizeof(Address));
  *(int *)(&joinaddr.addr) = 1;
  *(short *)(&joinaddr.addr[4]) = 0;

  return joinaddr;
}

/**
 * FUNCTION NAME: getMemberAddress
 *
 * DESCRIPTION: Returns the Address of the member with given id and port
 */
Address MP1Node::getMemberAddress(int id, int port) {
  Address memberAddr;

  memset(&memberAddr, 0, sizeof(Address));
  *(int *)(&memberAddr.addr) = id;
  *(short *)(&memberAddr.addr[4]) = port;

  return memberAddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
  memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr) {
  printf("%d.%d.%d.%d:%d \n", addr->addr[0], addr->addr[1], addr->addr[2],
         addr->addr[3], *(short *)&addr->addr[4]);
}
