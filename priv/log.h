#ifndef GARUDA_LOG_H
#define GARUDA_LOG_H

#include <sstream>
#include <iostream>
#include <sys/time.h>
#include <time_utils.h>

#define L_FAT 0
#define L_MSG 1
#define L_ERR 2
#define L_WRN 3
#define L_INF 4
#define L_DBG 5
#define L_TRC 6

#define L_MIN L_FAT
#define L_MAX L_TRC

#ifndef TARGET_LOG_LEVEL
#define TARGET_LOG_LEVEL L_TRC
#endif

inline int LOG_LEVEL = L_TRC;
inline bool COLOR_LOG = true;

inline void _SetLogLevel(int l, bool c){
  LOG_LEVEL = l;
  COLOR_LOG = c;
}

class LOG_IT{
  int sev = L_TRC;
  char tmbuf[64], buf[64];
public:
  LOG_IT(std::string func, std::string msg){
    putColor();
    getTS();
    _buffer << "|" << msg << "|" << func << "] ";
  }
  LOG_IT(std::string file, std::string func, int line, int log_level){
    sev = log_level;
    putColor();
    getTS();
    if(log_level == L_MSG) _buffer << "] ";
    else _buffer << "|" << file << ":" << line << "|" << func << "] ";
  }
  template <typename T>
  LOG_IT & operator<<(T const & value){
    _buffer << value;
    return *this;
  }
  ~LOG_IT(){
    if(COLOR_LOG) _buffer << "\033[0m";
    _buffer << std::endl;
    std::cerr << _buffer.str();

    // if(sev == L_FAT) exit(1);
  }

private:
  void getTS(){
    struct timeval tv;
    time_t nowtime;
    struct tm *nowtm;
    char tmbuf[64], buf[64];
    gettimeofday(&tv, NULL);
    nowtime = tv.tv_sec;
    nowtm = localtime(&nowtime);
    strftime(tmbuf, sizeof tmbuf, "%Y-%m-%d %H:%M:%S", nowtm);

    #ifdef __APPLE__
    const char* fmt = "%s.%03d";
    #elif
    const char* fmt = "%s.%03ld";
    #endif

    snprintf(buf, sizeof buf, fmt, tmbuf, tv.tv_usec / 1000);
    _buffer << "[" << buf;
  }
  void putColor(){
    if(COLOR_LOG){
      _buffer << "\033[";
      _buffer << 91 + sev;
      _buffer << 'm';
    }
  }
  std::ostringstream _buffer;
};

#define _FLOG(args) LOG_IT(__PRETTY_FUNCTION__, args)
#define FBEG if(L_TRC > LOG_LEVEL); else _FLOG("BEG")
#define FEND if(L_TRC > LOG_LEVEL); else _FLOG("END")

#define LOG(sev) if(sev > LOG_LEVEL); else LOG_IT(__FILENAME__, __PRETTY_FUNCTION__, __LINE__, sev)

inline void TestLogs(){
  LOG(L_MSG) << "L_MSG";
  LOG(L_ERR) << "L_ERR";
  LOG(L_WRN) << "L_WRN";
  LOG(L_INF) << "L_INF";
  LOG(L_DBG) << "L_DBG";
  LOG(L_TRC) << "L_TRC";
  LOG(L_FAT) << "L_FAT";
}

#endif
