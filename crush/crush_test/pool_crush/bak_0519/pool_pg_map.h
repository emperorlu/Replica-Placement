#pragma once
#include <algorithm>
#include <iostream>
#include <vector>
#include <map>
#include <set>
#include <cmath>
#include <queue>
#include <assert.h>
using namespace std;

static const uint32_t invalid_id =  0x7fffffff;
struct bucket {
  int32_t id;
  uint32_t weight;
  uint32_t primary_pg_num;
  uint32_t duplicate_pg_num;
  set<int> primary_pg;
  set<int> duplicate_pg;
  
  bucket(int id) : id(id), weight(0), duplicate_pg_num(0), primary_pg_num(0) {}
  bucket() : id(invalid_id), weight(0), duplicate_pg_num(0), primary_pg_num(0) {}
};

struct osd_info : public bucket{
  int bucket_id;
  osd_info(int id) : bucket(id), bucket_id(0) {}
  osd_info() : bucket(), bucket_id(0) {}
};

struct bucket_info : public bucket {
  vector<int> item; //osds
  bucket_info(int id) : bucket(id) {}
  bucket_info() : bucket() {}
};

struct map_change {
  vector<pair<int, int>> osd_crush_remove; //bucket osd
  vector<pair<int, pair<int, int>>> osd_crush_add;  //bucket osd weight

  vector<pair<int, int>> osd_osdmap_remove; //bucket osd
  vector<pair<int, int>> osd_osdmap_add;  //bucket osd
  
  vector<pair<int, pair<int, int>>> osd_weight_change; //bucket osd weight
  void clear() {
    osd_crush_remove.clear();
    osd_crush_add.clear();
    osd_osdmap_remove.clear();
    osd_osdmap_add.clear();
    osd_weight_change.clear();
  }
};

class pool_pg_map
{
public:
  pool_pg_map(int size, int pg_num) : pool_size(size), pool_pg_num(pg_num) {
    root_weight = 0;
  }
  ~pool_pg_map() {}

private:
  int pool_size;
  int pool_pg_num;
  map<int, bucket_info> buckets_map;
  map<int, osd_info> osds_map;
  int root_weight;

  map<int, vector<int>> pgs_to_osd;

  map<int, set<pair<int, int>>> out_osd_pg_maps; //osd->(pg, duplicate_id)
  map<int, int> out_osd_weight; //osd->weight;

  bool check_collide(int pg, int from, int to) {
    for (int osd : pgs_to_osd[pg]) {
      if (osds_map[osd].bucket_id == osds_map[to].bucket_id && osds_map[osd].bucket_id != osds_map[from].bucket_id) {
        return true;
      }
    }
    return false;
  }

  int preprocess_map_change(map_change& change);

public:
  void gen_map(int osd_num, int osd_per_host);
  void init_crushmap_pg_target();
  void adjust_crushmap_pg_target(map<int, set<int>>& primary_change,
                                 map<int, set<int>>& duplicate_change);
  void init_pg_mappings();
  void dump_map();
  bool check_map();
  bool check_mapping_result();
  void dump_result();
  int overfull_remap(map<int, set<int>>& change_map, bool is_primary);
  int underfull_remap(map<int, set<int>>& change_map, bool is_primary);
  int apply_map_change(map_change& change);
};

