#include <log.h>
#include <glob.h>
#include <unistd.h>
#include <sys/stat.h>

#include <queue.h>

#define likely(x)       __builtin_expect((x),1)
#define unlikely(x)     __builtin_expect((x),0)

int LOG_LEVEL = L_TRC;
bool COLOR_LOG = true;

const size_t MAX_Q_SIZE = 1000000;
const uint8_t MSG_HEADER_SIZE = sizeof(uint16_t);

void _mkdir(char *dir) {
  char tmp[256];
  char *p = NULL;
  size_t len;

  snprintf(tmp, sizeof(tmp),"%s",dir);
  len = strlen(tmp);
  if(tmp[len - 1] == '/') tmp[len - 1] = 0;
  for(p = tmp + 1; *p; p++){
    if(*p == '/'){
      *p = 0;
      mkdir(tmp, S_IRWXU);
      *p = '/';
    }
  }
  mkdir(tmp, S_IRWXU);
}

size_t djb_hash(const char* cp){
  size_t hash = 5381;
  while (*cp) hash = 33 * hash ^ (unsigned char) *cp++;
  return hash;
}

void FreeMsg(Msg* msg){
  free(msg);
}

uint16_t GetSize(Msg* msg){
  return ((uint16_t*)(msg))[0];
}

unsigned char* GetData(Msg* msg){
  return msg + MSG_HEADER_SIZE;
}

Msg* NewMsg(uint16_t size, unsigned char* _data){
  Msg* msg = (Msg*)malloc(sizeof(Msg) * (size + MSG_HEADER_SIZE));
  memcpy(msg, &size, MSG_HEADER_SIZE);
  memcpy(msg + MSG_HEADER_SIZE, _data, size);
  return msg;
}

bool Queue::SaveQToFile(MsgQueue* q, uint64_t ts){
  std::ostringstream fname;
  fname << dbPath << name << "_" << ts;
  FILE* pFile = fopen(fname.str().c_str(), "wb");

  uint16_t size = 0;
  uint32_t numMessage = 0;
  Msg* msg = nullptr;

  uint64_t stts = getTS();

  q->try_dequeue(msg);
  while(msg){
    size = GetSize(msg);
    fwrite(msg, 1, MSG_HEADER_SIZE + size, pFile);
    numMessage += 1;
    FreeMsg(msg);
    msg = nullptr;
    q->try_dequeue(msg);
  }
  fclose(pFile);
  LOG(L_MSG) << "q " << name << ": dumped " << numMessage << " messages to " << ts << " in " << getTimeDiff(&stts) << " ms";
  return true;
}

bool Queue::LoadQFromFile(MsgQueue* q, uint64_t fileNo){
  FBEG;
  bool rc = false;

  std::ostringstream fname;
  fname << dbPath << name << "_" << fileNo;

  struct stat buffer;
  if(stat(fname.str().c_str(), &buffer) == 0){
    size_t fileSize = buffer.st_size;

    unsigned char* buff = (unsigned char*) malloc (sizeof(unsigned char)*fileSize);

    if(!buff){
      LOG(L_FAT) << "alloc failed";
      goto end;
    }

    unsigned char* orgBuff = buff;

    uint64_t ts = getTS();

    FILE* pFile = fopen(fname.str().c_str(), "rb");
    size_t bytesRead = fread(buff, 1, fileSize, pFile);
    if(bytesRead != buffer.st_size){
      LOG(L_ERR) << fname.str() << " size mismatch, " << " bytesRead: " << bytesRead << ", against fileSize: " << fileSize;
    }

    uint32_t numMessage = 0;

    while(bytesRead > 0){
      if(bytesRead <= MSG_HEADER_SIZE) break;
      uint16_t size = ((uint16_t*)(buff))[0];
      buff += MSG_HEADER_SIZE;
      bytesRead -= MSG_HEADER_SIZE;
      if(bytesRead < size) break;
      Msg* msg = NewMsg(size, buff);
      buff += size;
      bytesRead -= size;
      hq->enqueue(msg);
      numMessage += 1;
    }

    remove(fname.str().c_str());

    free(orgBuff);
    fclose(pFile);

    LOG(L_MSG) << "q " << name << ": loaded " << numMessage << " messages from "  << fileNo  << " in " << getTimeDiff(&ts) << " ms";

    rc = numMessage > 0 ? true : false;
  }
  else{
    FBEG << fname.str() << " not found";
  }
  end:
  FEND;
  return rc;
}

bool Queue::LoadHqFromFile(){
  bool rc = false;
  if(files.size() > 0 && hq->size_approx() < MAX_Q_SIZE / 3){
    mtx.lock();
    if(files.size() > 0 && hq->size_approx() < MAX_Q_SIZE / 3){
      uint64_t fileNo = files.front();
      files.pop_front();
      LOG(L_MSG) << "q " << name << ": loading hq from " << fileNo;
      rc = LoadQFromFile(hq, fileNo);
    }
    mtx.unlock();
  }
  return rc;
}

bool Queue::LoadTqFromFile(){
  bool rc = false;
  if(files.size() > 0 && tq->size_approx() == 0){
    mtx.lock();
    rc = true;
    if(files.size() > 0 && tq->size_approx() == 0){
      uint64_t fileNo = files.back();
      files.pop_back();
      LOG(L_MSG) << "q " << name << ": loading tq from " << fileNo;
      rc = LoadQFromFile(tq, fileNo);
    }
    mtx.unlock();
  }
  return rc;
}

Queue::Queue(char* _name, char* _dbPath){
  FBEG;
  name = new char[strlen(_name) + 1];
  strcpy(name, _name);

  dbPath = _dbPath;

  int i = 0;
  glob_t globbuf;

  stats.qname = name;

  std::ostringstream fname;
  fname << dbPath << name << "_*";

  if(!glob(fname.str().c_str(), 0, NULL, &globbuf)){
    for(i = 0; i < globbuf.gl_pathc; i++){
      std::string dumpFileName = globbuf.gl_pathv[i];
      std::string tsStr = dumpFileName.substr(fname.str().length() - 1);
      if(!tsStr.empty()){
        uint64_t fts = std::strtoul(tsStr.c_str(), 0 , 10);
        if(fts > 0){
          files.push_back(fts);
          LOG(L_MSG) << "q " << name << ": dump file with ts: " << fts << " found";
        }
      }
    }
    globfree(&globbuf);
  }
  else
    LOG(L_MSG) << "q " << name << ": no dump files found";

  files.sort();
  files.unique();

  while(LoadHqFromFile());
  LoadTqFromFile();

  FEND;
}

void Queue::DumpToDisk(){
  mtx.lock();
  QStat* stat = GetStats();
  printf("q: %s, hq len: %llu, tq len: %llu, pop rate: %llu, push rate: %llu\n",
    stat->qname,
    stat->hqSize,
    stat->tqSize,
    stat->popCount,
    stat->pushCount
  );
  if(hq->size_approx() > 0) SaveQToFile(hq, 1);
  if(tq->size_approx() > 0) SaveQToFile(tq, std::numeric_limits<uint64_t>::max());
  files.clear();
  mtx.unlock();
}

bool Queue::Push(Msg* msg){
  pushCount += 1;
  if(unlikely(tq->size_approx() >= MAX_Q_SIZE)){
    mtx.lock();
    if(tq->size_approx() >= MAX_Q_SIZE){
      maintenanceMode = true;
      if(hq->size_approx() == 0 && files.size() == 0){
        usleep(1);
        MsgQueue* _q = hq;
        hq = tq;
        tq = _q;
        usleep(1);
      }
      else{
        bool rc = false;
        usleep(1);
        MsgQueue* _q = tq;
        tq = new MsgQueue();
        uint64_t ts = getTS();
        rc = SaveQToFile(_q, ts);
        delete _q;
        files.push_back(ts);
        usleep(1);
      }
      maintenanceMode = false;
    }
    mtx.unlock();
  }
  return tq->enqueue(msg);
}

Msg* Queue::Pop(){
  Msg* msg = nullptr;
  if(maintenanceMode)
    return msg;
  hq->try_dequeue(msg);
  if(!msg){
    if(unlikely(LoadHqFromFile())) hq->try_dequeue(msg);
    if(!msg) tq->try_dequeue(msg);
  }
  if(msg){
    popCount += 1;
  }
  return msg;
}

QStat* Queue::GetStats(){
  stats.hqSize = hq->size_approx();
  stats.tqSize = tq->size_approx();
  stats.popCount = popCount;
  stats.pushCount = pushCount;
  stats.files = files.size();

  popCount = 0;
  pushCount = 0;
  return &stats;
}

QueueFactory* QueueFactory::qFactory = nullptr;

QueueFactory* QueueFactory::GetQueueFactory(char* dbPath){
  if(!qFactory){
    FBEG;
    qFactory = (QueueFactory*)new QueueFactoryImpl(dbPath);
    FEND;
  }
  return qFactory;
}

void QueueFactory::SetLogLevel(int l, bool c){
  _SetLogLevel(l,c);
}

QueueFactoryImpl::QueueFactoryImpl(char* _dbPath){
  size_t len = strlen(_dbPath);
  dbPath = new char[len + 2];
  strcpy(dbPath, _dbPath);
  if(len > 0){
    if(dbPath[len - 1] != '/') dbPath[len] = '/';
    _mkdir(dbPath);
  }
  LOG(L_MSG) << "QueueFactory initialised with path: " << dbPath;
}

Queue* QueueFactoryImpl::GetQ(char* name, bool create){
  size_t hash = djb_hash(name);
  Queue* q = qmap.get(hash);
  if(!q && create){
    mtx.lock();
    q = qmap.get(hash);
    if(!q){
      q = new Queue(name, dbPath);
      qmap.assign(hash, q);
      queues.push_back(q);
    }
    mtx.unlock();
  }
  if(q){
    if(strcmp(name, q->name) != 0){
      LOG(L_FAT) << "hash collision found names: " << name << " " << q->name << ", hash: " << hash;
    }
  }
  return q;
}

bool QueueFactoryImpl::Push(char* name, Msg* msg){
  Queue* q = GetQ(name, true);
  if(q) return q->Push(msg);
  return false;
}

Msg* QueueFactoryImpl::Pop(char* name){
  Queue* q = GetQ(name, true);
  Msg* msg = nullptr;
  if(q) msg = q->Pop();
  return msg;
}

std::list<QStat*> QueueFactoryImpl::GetStats(){
  std::list<QStat*> stats;
  for(std::list<Queue*>::iterator it=queues.begin(); it != queues.end(); ++it){
      Queue* q = *it;
      stats.push_back(q->GetStats());
    }
  return stats;
}

void QueueFactoryImpl::DumpToDisk(){
  FBEG;
  mtx.lock();
  for (std::list<Queue*>::iterator it=queues.begin(); it != queues.end(); ++it){
    Queue* q = *it;
    q->DumpToDisk();
  }
  mtx.unlock();
  FEND;
}
