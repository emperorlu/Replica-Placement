#include <algorithm>
#include <iostream>
#include <vector>
#include <map>
#include <set>
#include <cmath>
#include <queue>
using namespace std;

struct bucket {
  int id;
  int weight;
  int pg_num;
  int primary_pg;
  vector<int> item;
};
struct node{
  int id;
  int pg_num;
  node(int id, int num) : id(id), pg_num(num) {}
  bool operator < (const node& a) const {
    return pg_num < a.pg_num; //大顶堆
  }
};

vector<int> osd_weight;
vector<int> osd_pg_num;
vector<int> osd_primary_pg_num;
vector<bucket> root;
int root_weight;

map<int, vector<int>> pgs_to_osd;
map<int, set<int>> buckets_to_pgs;
map<int, set<int>> buckets_to_primary_pgs;
map<int, set<int>> osd_to_pgs;
map<int, set<int>> osd_to_primary_pgs;

void gen_map(int osd_num, int osd_per_host) {
  int osd_id = 0;
  int bucket_id = -1;
  bucket b;
  b.weight = 0;
  b.pg_num = 0;
  b.primary_pg = 0;
  for (int i = 0; i < osd_num; i++) {
    osd_weight.push_back(1);
    osd_pg_num.push_back(0);
    osd_primary_pg_num.push_back(0);
    if (i%osd_per_host == 0 && i > 0) {
      b.id = bucket_id--;
      root_weight += b.weight;
      root.push_back(b);
      b.weight = 0;
      b.item.clear();
    }
    b.item.push_back(osd_id++);
    b.weight++;
  }
  b.id = bucket_id--;
  root_weight += b.weight;
  root.push_back(b);
}

void set_crushmap_pg_target(unsigned pool_size, unsigned pool_pg_num) {
  int pool_pgs = (pool_size-1) * pool_pg_num;
  int pool_primary_num = pool_pg_num;
  float pgs_per_weight = 1.0 * pool_pgs / root_weight;
  float primary_per_weight = 1.0 * pool_primary_num / root_weight;
  for(auto & b : root) {
    b.pg_num = ceil(b.weight * pgs_per_weight);
    b.primary_pg = ceil(b.weight * primary_per_weight);
  }
  for(int i = 0; i < osd_weight.size(); i++) {
    osd_pg_num[i] = ceil(osd_weight[i] * pgs_per_weight);
    osd_primary_pg_num[i] = ceil(osd_weight[i] * primary_per_weight);
  }
}

void init_pg_mappings(unsigned pool_size, unsigned pool_pg_num) {
  priority_queue<node> q;
  priority_queue<node> primary_q;
  for(auto& b : root) {
    q.push(node(b.id, b.pg_num));
    primary_q.push(node(b.id, b.primary_pg));
  }
  for(unsigned pg = 0; pg < pool_pg_num; pg++) { //choose bucket
    vector<int> out_bucket;
    vector<node> t;
    for(int i = 0; i < pool_size && !q.empty();) {
      if(i == 0) { //primary
        t.push_back(primary_q.top()); primary_q.pop();
        out_bucket.push_back(t[i].id);
        buckets_to_primary_pgs[t[i].id].insert(pg);
        t[i].pg_num--;
      } else {
        t.push_back(q.top()); q.pop();
        if(find(out_bucket.begin(), out_bucket.end(), t.back().id) == out_bucket.end()) {
          out_bucket.push_back(t.back().id);
          buckets_to_pgs[t.back().id].insert(pg);
          t.back().pg_num--;
        } else {
          continue;
        }
      }
      i++;
    }
    for(int i = 0; i < t.size(); i++) {
      if(t[i].pg_num > 0) {
        if(i == 0) {
          primary_q.push(t[i]);
        } else {
          q.push(t[i]);
        }
      }
    }
    /*
    for(int i = 0; i < out_bucket.size(); i++) {
      cout << -1*out_bucket[i] << " ";
    }
    cout << endl;
    */
  }
  
  for(auto& b : root) {
    while(!q.empty()) q.pop();
    while(!primary_q.empty()) primary_q.pop();
    for(int i = 0; i < b.item.size(); i++) {
      q.push(node(b.item[i], osd_pg_num[b.item[i]]));
      primary_q.push(node(b.item[i], osd_primary_pg_num[b.item[i]]));
    }
    for(auto pg : buckets_to_primary_pgs[b.id]) {
      node n = primary_q.top(); primary_q.pop();      
      if(pgs_to_osd[pg].empty()) {
        pgs_to_osd[pg].push_back(n.id);
      } else {
        pgs_to_osd[pg][0] = n.id;
      }
      osd_to_primary_pgs[n.id].insert(pg);
      n.pg_num--;                      
      if(n.pg_num > 0) {               
        primary_q.push(n);                     
      }
    }

    for(auto pg : buckets_to_pgs[b.id]) {
      node n = q.top(); q.pop();
      if(pgs_to_osd[pg].empty()) {
        pgs_to_osd[pg].push_back(-1);
      }
      pgs_to_osd[pg].push_back(n.id);
      osd_to_pgs[n.id].insert(pg);
      n.pg_num--;
      if(n.pg_num > 0) {
        q.push(n);
      }
    }
  }
}

void dump_map() {
  for (auto b : root) {
    cout << b.id << ":" << b.weight << "(" << b.pg_num << ")"<< endl;
    for(auto& i : b.item) {
      cout << i << "(" << osd_pg_num[i] << ")" << " ";
    }
    cout << endl;
  }
}

void dump_result(int pg_nums) {
  for(int i = 0; i < pg_nums; i++) {
    for(int j = 0; j < pgs_to_osd[i].size(); j++) {
      cout << pgs_to_osd[i][j] << " ";
    }
    cout << endl;
  }
}

int main()
{
  int pg_nums = 30000;
  int pool_size = 3;
  gen_map(1000, 10);
  set_crushmap_pg_target(pool_size, pg_nums);
  init_pg_mappings(pool_size, pg_nums);
  dump_result(pg_nums);
  return 0;
}

