#include <map>
#include <string>
#include <vector>
#include <iostream>
using namespace std;

typedef uint32_t __u32;

#define crush_hashmix(a, b, c) do {			\
		a = a-b;  a = a-c;  a = a^(c>>13);	\
		b = b-c;  b = b-a;  b = b^(a<<8);	\
		c = c-a;  c = c-b;  c = c^(b>>13);	\
		a = a-b;  a = a-c;  a = a^(c>>12);	\
		b = b-c;  b = b-a;  b = b^(a<<16);	\
		c = c-a;  c = c-b;  c = c^(b>>5);	\
		a = a-b;  a = a-c;  a = a^(c>>3);	\
		b = b-c;  b = b-a;  b = b^(a<<10);	\
		c = c-a;  c = c-b;  c = c^(b>>15);	\
	} while (0)

#define crush_hash_seed 1315423911

static __u32 crush_hash32_rjenkins1(__u32 a)
{
	__u32 hash = crush_hash_seed ^ a;
	__u32 b = a;
	__u32 x = 231232;
	__u32 y = 1232;
	crush_hashmix(b, x, hash);
	crush_hashmix(y, a, hash);
	return hash;
}

static __u32 crush_hash32_rjenkins1_2(__u32 a, __u32 b)
{
	__u32 hash = crush_hash_seed ^ a ^ b;
	__u32 x = 231232;
	__u32 y = 1232;
	crush_hashmix(a, b, hash);
	crush_hashmix(x, a, hash);
	crush_hashmix(b, y, hash);
	return hash;
}

class ConsistentHash {
public:
    ConsistentHash(int node_num, int vnode_num, const vector<int> &weights);
    ~ConsistentHash();

    size_t GetServerIndex(const int &key);
    vector<size_t> GetServerIndex(const int &key, int rep_num);

private:
    map<size_t, size_t> server_nodes_;
    int node_num_;
    int vnode_num_;
    vector<int> weights;
};
