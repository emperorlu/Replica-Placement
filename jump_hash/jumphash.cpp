#include <iostream>
#include <random>
using namespace std;

int32_t JumpConsistentHash(uint64_t key, int32_t num_buckets) {
    int64_t b = -1, j = 0;
    while (j < num_buckets) {
        b = j;
        key = key * 2862933555777941757ULL + 1;
        j = (b + 1) * (double(1LL << 31) / double((key >> 33) + 1));
    }
    return b;
}

int main() {
    int num = 3;
    int b[num];
    for (int i=0; i<num; i++){
        b[i] = 0;
    }
    for (int i=0; i<1100; i++){
        b[JumpConsistentHash(i,num)]++;
    }
    for (int i=0; i<num; i++){
        cout << b[i] << endl;
    }
    return 0;
}
