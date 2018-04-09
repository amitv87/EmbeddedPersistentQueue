#include <list>
#include <iostream>
#include <queuefactory.h>
#include <concurrentqueue.h>
#include <junction/ConcurrentMap_Grampa.h>

class Queue;

typedef junction::ConcurrentMap_Grampa<size_t, Queue*> QueueMap;

typedef moodycamel::ConcurrentQueue<Msg*> MsgQueue;

class Queue{
  std::mutex mtx;
  std::list<uint64_t> files;
  MsgQueue* hq = new MsgQueue();  // head queue
  MsgQueue* tq = new MsgQueue();  // tail queue

  std::atomic<bool> maintenanceMode = false;
  std::atomic<uint64_t> popCount = 0;
  std::atomic<uint64_t> pushCount = 0;

  bool SaveTqToFile();
  bool LoadHqFromFile();
  bool SaveQToFile(MsgQueue* q, uint64_t ts);

public:
  char* name;
  char* dbPath;
  Queue(char* _name, char* _dbPath);
  Msg* Pop();
  bool Push(Msg* msg);
  void PrintStats();
  void DumpToDisk();
};

class QueueFactoryImpl : QueueFactory{
  char* dbPath;
  QueueMap qmap;
  std::mutex mtx;
  std::list<Queue*> queues;
  Queue* GetQ(char* name, bool create = false);
public:
  QueueFactoryImpl(char* dbPath);
  Msg* Pop(char* name);
  bool Push(char* name, Msg* msg);
  void PrintStats(char* name = 0);
  void DumpToDisk();
};
