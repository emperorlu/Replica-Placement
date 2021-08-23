#include <iostream>
#include <string>
#include <string.h>
using namespace std;

template<typename T>
inline static  void _key_encode_u32(uint32_t u, T *key)
{
    uint32_t bu;
    bu = __builtin_bswap32(u);
    key->append((char*)&bu, 4);
}

inline static const char *_key_decode_u32(const char *key, uint32_t *pu) {
  uint32_t bu;
  memcpy(&bu, key, 4);
  *pu = __builtin_bswap32(bu);
  return key + 4;
}

int main()
{
    string key1, key2;
    _key_encode_u32(123456789, &key1);
    _key_encode_u32(123456789, &key2);
    std::cout << key1 << " " << key2 << std::endl;
    bool eq = key1 == key2;
    cout << eq << endl;
    uint32_t v;
    _key_decode_u32(key1.c_str(), &v);
    std::cout << v << std::endl;
    return 0;
}

