#ifndef GARUDA_TIME_UTILS_H
#define GARUDA_TIME_UTILS_H

#include <sys/time.h>

inline uint64_t getTS() {
  struct timeval te;
  gettimeofday(&te, NULL); // get current time
  uint64_t milliseconds = te.tv_sec*1000LL + te.tv_usec/1000; // caculate milliseconds
  return milliseconds;
  // return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

inline uint64_t getTimeDiff(uint64_t *t_prev){
  if(*t_prev == 0){
    *t_prev = getTS();
    return 0;
  }
  uint64_t t_now = getTS();
  uint64_t diff = t_now - *t_prev;
  *t_prev = t_now;
  return diff;
}

#endif
