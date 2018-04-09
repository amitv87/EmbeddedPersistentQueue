#include <thread>
#include <unistd.h>
#include <signal.h>
#include <queuefactory.h>

char* data = "barbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbar";
char* qname = "foo";

void HndleSignal(int signal){
  printf("signal caught %d\n", signal);
  QueueFactory::GetQueueFactory()->DumpToDisk();
  exit(0);
}

void InitSignalHandler(){
  struct sigaction sa;
  sa.sa_handler = &HndleSignal;
  sa.sa_flags = SA_RESTART;
  sigfillset(&sa.sa_mask);

  if(sigaction(SIGHUP, &sa, NULL) == -1) {
    printf("cannot handle SIGHUP\n"); // Should not happen
  }

  if(sigaction(SIGUSR1, &sa, NULL) == -1) {
    printf("cannot handle SIGUSR1\n"); // Should not happen
  }

  if(sigaction(SIGINT, &sa, NULL) == -1) {
    printf("cannot handle SIGINT\n"); // Should not happen
  }
}

typedef std::function<void()> Runnable;
void RunOnNewThread(Runnable callback){
  std::thread([callback]{
    callback();
  }).detach();
}

void testOne(){
  QueueFactory* qf = QueueFactory::GetQueueFactory();
  Msg* msgin = NewMsg(strlen(data), (unsigned char*)data);
  if(qf->Push(qname, msgin))
    printf("pushed message %s of size %d to queue %s\n", GetData(msgin), GetSize(msgin), qname);
  else
    printf("no message pushed\n");

  FreeMsg(msgin);
  msgin = nullptr;

  Msg* msgout = qf->Pop(qname);
  if(msgout){
    printf("popped message %s of size %d from queue %s\n", GetData(msgout), GetSize(msgout), qname);
    FreeMsg(msgin);
    msgout = nullptr;
  }
  else
    printf("no message popped\n");
}

void testRaw(){
  QueueFactory* qf = QueueFactory::GetQueueFactory();
  RunOnNewThread([qf](){
    while(true){
      usleep(1);
      // nanosleep((const struct timespec[]){{0, 0L}}, NULL);
      Msg* msg = NewMsg(strlen(data), (unsigned char*)data);
      if(! qf->Push(qname, msg)){
        FreeMsg(msg);
        break;
      }
    }
  });

  RunOnNewThread([qf](){
    Msg* msg;
    while(true){
      usleep(1);
      // nanosleep((const struct timespec[]){{0, 0L}}, NULL);
      Msg* msg = qf->Pop(qname);
      if(msg) FreeMsg(msg);
    }
  });
}

int main(int argc, char const *argv[]){
  InitSignalHandler();
  QueueFactory::SetLogLevel(6, true);

  QueueFactory* qf = QueueFactory::GetQueueFactory((char*)(argc > 1 ? (argv[1]) : "_db"));
  // testOne();
  testRaw();

  while(true){
    qf->PrintStats();
    usleep(1000 * 1000);
  }
  
  return 0;
}
