#include <list>
#include <stdint.h>

typedef unsigned char Msg;
void FreeMsg(Msg* msg);
uint16_t GetSize(Msg* msg);
unsigned char* GetData(Msg* msg);
Msg* NewMsg(uint16_t size, unsigned char* data);

struct QStat{
  char* qname;
  size_t files = 0;
  uint64_t hqSize = 0;
  uint64_t tqSize = 0;
  uint64_t popCount = 0;
  uint64_t pushCount = 0;
};

class QueueFactory{
  static QueueFactory* qFactory;
public:
  static QueueFactory* GetQueueFactory(char* dbPath = (char*)"./"); // not thread safe
  static void SetLogLevel(int level, bool color); // level 0 -> 6
  virtual void DumpToDisk() = 0; // call only once when process is about to exit (mostly on SIGINT)
  virtual Msg* Pop(char* name) = 0;
  virtual bool Push(char* name, Msg* msg) = 0;
  virtual std::list<QStat*> GetStats() = 0;
};
