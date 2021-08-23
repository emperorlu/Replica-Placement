#include <iostream>
#include <stdio.h>
#include <time.h>
#include <stdlib.h>
#include <iomanip>

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
    srand((unsigned)time(NULL));
    for (int i = 0; i < 10; i++) //选取1000个pg进行测试，pgid取一个随机数
    {
        //testpgid[i] = rand();
        testpgid[i] = i;
    }
    //int testpgid[10] = {432879,32189,8879,3871,1048390,73167,46104,32169,34791,31280};
    for (int n = 0; n < 10; n++) //对1000个pg进行1000次item选举测试
    {
        int i;
        unsigned int weight = 1;
        int item[10] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}; //针对10个item来模拟
        int high = 0;
        unsigned long long high_draw = 0;
        unsigned long long draw;
        int result;
        for (i = 0; i < 10; i++)
        {
            draw = crush_hash32_rjenkins1_3(testpgid[n], item[i], 3);
            draw &= 0xffff;
            draw *= weight;
            if (i == 0 || draw > high_draw)
            {
                high = i;
                high_draw = draw;
            }
        }
        result = high; //result为最终在10个item中选出的item号

        int j;
        int item_del[9] = {1, 2, 3, 4, 5, 6, 8, 9, 10}; //模拟删掉一个item 7
        int high1 = 0;
        unsigned long long high_draw1 = 0;
        unsigned long long draw1;
        int result1;
        for (j = 0; j < 9; j++)
        {
            draw1 = crush_hash32_rjenkins1_3(testpgid[n], item_del[j], 3);
            draw1 &= 0xffff;
            draw1 *= weight;
            if (j == 0 || draw1 > high_draw1)
            {
                high1 = j;
                high_draw1 = draw1;
            }
        }
        result1 = high1; //result1为最终在9个item中选出的item号

        int k;
        int item_add[11] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}; //模拟新增一个item11
        int high2 = 0;
        unsigned long long high_draw2 = 0;
        unsigned long long draw2;
        int result2;
        for (k = 0; k < 11; k++)
        {
            draw2 = crush_hash32_rjenkins1_3(testpgid[n], item_add[k], 3);
            draw2 &= 0xffff;
            draw2 *= weight;
            if (k == 0 || draw2 > high_draw2)
            {
                high2 = k;
                high_draw2 = draw2;
            }
        }
        result2 = high2; //result2为最终在11个item中选出的item号
        cout << left << setw(12) << testpgid[n] << setw(5) << result << setw(3) << result1 << setw(3) << result2 << endl;
    } //打印的内容为：pgid号，没有osd变动时候选出的item， 删除一个osd后选出的item， 新增一个osd后选出的item
    return 0;
}
