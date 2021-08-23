#pragma once                                                                                       
#include <algorithm>
#include <iostream>
#include <vector>
#include <map>
#include <set>
#include <cmath>
#include <queue>
#include <assert.h>
#include <unordered_set>
using namespace std;

static const uint32_t invalid_id =  0x7fffffff;
static const int32_t virtual_node_id = 0x10000000;
struct bucket {
  int32_t id;
  float weight;
  float correct_weight;
  uint32_t primary_pg_num;
  uint32_t duplicate_pg_num;
  set<int> primary_pg;
  set<int> duplicate_pg;
  vector<int> item; //osds

  bucket(int id) : id(id), weight(0), duplicate_pg_num(0), primary_pg_num(0) {}
  bucket() : id(invalid_id), weight(0), duplicate_pg_num(0), primary_pg_num(0) {}
};

struct map_change {
  vector<int> crush_remove; //bucket
  vector<pair<int, float>> crush_add;  //bucketweight

  vector<int> osdmap_remove; //bucket
  vector<int> osdmap_add;  //bucket

  vector<pair<int, float>> weight_change; //bucket weight
  void clear() {
    crush_remove.clear();
    crush_add.clear();
    osdmap_remove.clear();
    osdmap_add.clear();
    weight_change.clear();
  }
};

class pool_bucket
{
public:
  pool_bucket(int size, int pg_num) : pool_size(size), pool_pg_num(pg_num) {
  }
  ~pool_bucket() {}

private:
  int pool_size;
  int pool_pg_num;
  map<int, bucket> buckets_map;
  set<int> virtual_nodes;
  int valid_bucket_num;
  float root_weight;
  float total_correct_weight;

  map<int, vector<int>> pgs_to_bucket;

  map<int, set<pair<int, int>>> out_bucket_pg_maps; //osd->(pg, duplicate_id)
  map<int, int> out_bucket_weight; //osd->weight;

  bool check_collide(int pg, int from, int to) {
    assert(from != to);
    for (int osd_bucket : pgs_to_bucket[pg]) {
      if (buckets_map[osd_bucket].id == to) {
        return true;
      }
    }
    return false;
  }

  int preprocess_map_change(map_change& change);
  void correct_bucket_weight();
public:
  void gen_map(vector<int>& weight);
  void init_crushmap_pg_target();
  void adjust_crushmap_pg_target(map<int, set<int>>& primary_change,
                                 map<int, set<int>>& duplicate_change);
  void init_pg_mappings();
  void dump_map();
  bool check_map();
  bool check_mapping_result();
  void dump_result();
  int overfull_remap(map<int, set<int>>& change_map, bool is_primary);
  int overfull_remap_step1(map<int, set<int>>& change_map, bool is_primary);
  int overfull_remap_step2(map<int, set<int>>& change_map, bool is_primary);
  int underfull_remap(map<int, set<int>>& change_map, bool is_primary);
  int apply_map_change(map_change& change);
};

