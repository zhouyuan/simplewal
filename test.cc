#include "simplewal.h"

using namespace rbc;

static const char alphanum[] =
"0123456789"
"!@#$%^&*"
"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
"abcdefghijklmnopqrstuvwxyz";

int stringLength = sizeof(alphanum) - 1;

char genRandom()  // Random string generator function.
{

    return alphanum[rand() % stringLength];
}

const char* getString()
{
  char str[4096];
  int i = 0;
  for(i = 0; i < 4096; ++i)
  {
      char _t = genRandom();
      str[i] = _t;
  }

  return str;
}


int main() {

    simplewal *sbc = new simplewal("/dev/sdb", 4096*1024*1024L, 4096); 
    int i;
    for(i = 0; i < 1024*4096; i++)
    {
        const char* tmp = getString();;
        int len = 4096;
        sbc->write(i/4096, tmp, i*4096, len);
        char ntmp[4096];
        sbc->read(i/4096, ntmp, i*4096, len);
        assert(strcmp(tmp, ntmp));
    }

    
}
