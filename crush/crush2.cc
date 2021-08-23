#include <iostream>
#include <stdio.h>
#include <time.h>
#include <iomanip>
#include <stdlib.h>
using namespace std;
typedef unsigned int __u32;
#define crush_hash_seed 1315423911

#define crush_hashmix(a, b, c) \
    do                         \
    {                          \
        a = a - b;             \
        a = a - c;             \
        a = a ^ (c >> 13);     \
        b = b - c;             \
        b = b - a;             \
        b = b ^ (a << 8);      \
        c = c - a;             \
        c = c - b;             \
        c = c ^ (b >> 13);     \
        a = a - b;             \
        a = a - c;             \
        a = a ^ (c >> 12);     \
        b = b - c;             \
        b = b - a;             \
        b = b ^ (a << 16);     \
        c = c - a;             \
        c = c - b;             \
        c = c ^ (b >> 5);      \
        a = a - b;             \
        a = a - c;             \
        a = a ^ (c >> 3);      \
        b = b - c;             \
        b = b - a;             \
        b = b ^ (a << 10);     \
        c = c - a;             \
        c = c - b;             \
        c = c ^ (b >> 15);     \
    } while (0)
//crush straw算法，参数1为pgid，参数2为一组item，参数3为副本数
static __u32 crush_hash32_rjenkins1_3(__u32 a, __u32 b, __u32 c)
{
    __u32 hash = crush_hash_seed ^ a ^ b ^ c;
    __u32 x = 231232;
    __u32 y = 1232;
    crush_hashmix(a, b, hash);
    crush_hashmix(c, x, hash);
    crush_hashmix(y, a, hash);
    crush_hashmix(b, x, hash);
    crush_hashmix(y, c, hash);
    return hash;
}
int main() //对straw算法进行测试
{
    int testpgid[1000];
    int pg_num = 50;
    srand((unsigned)time(NULL));
    for (int i = 0; i < pg_num; i++) //选取1000个pg进行测试，pgid取一个随机数
    {
        testpgid[i] = i;
    }

    for(int i = 0; i < pg_num; i++) {
        unsigned int weight[10] = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
        int weight_sum = 0;
        for(int j = 0; j < 10; j++) {
            weight_sum += weight[j];
        }

        int item[10] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}; //针对10个item来模拟
        unsigned long long high_draw = 0;
        unsigned long long draw;
        int high = 0;
        for(int j = 0; j < 10; j++) {
            draw = crush_hash32_rjenkins1_3(testpgid[i], item[j], 3);
            draw &= 0xffff;
            draw *= 1.0*weight[j]/weight_sum;
            if (j == 0 || draw > high_draw)
            {
                high = item[j];
                high_draw = draw;
            }
        }
        cout << left << setw(3) << high;
    }
    cout << endl;

    return 0;
}
