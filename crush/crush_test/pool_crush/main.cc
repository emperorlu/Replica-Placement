#include "pool_pg_map.h"
#include <string>

vector<string> split(const string& str,const string& delim) {
	vector<string> res;
	if ("" == str) return res;
	
	string strs = str + delim;
	size_t pos;
	size_t size = strs.size();
 
	for (int i = 0; i < size; ++i) {
		pos = strs.find(delim, i);
		if (pos < size) {
			string s = strs.substr(i, pos - i);
			res.push_back(s);
			i = pos + delim.size() - 1;
		}
		
	}
	return res;	
}

int main(int argc, char *argv[])
{
  if (argc >= 5) {
    int pool_size, pg_num, osd_num, osds_per_bucket;
    int type, bucket_id, osd_id, weight;
    pool_size = atoi(argv[1]);
    pg_num = atoi(argv[2]);
    osd_num = atoi(argv[3]);
    osds_per_bucket = atoi(argv[4]);

    pool_pg_map map(pool_size, pg_num);
    map.gen_map(osd_num, osds_per_bucket);
    map.init_crushmap_pg_target();
    //map.dump_map();
    map.init_pg_mappings();
    map_change c;
    if (!map.check_mapping_result()) {
      cout << __func__ << " mapping result failed" << endl;
    }
    //map.dump_result();
    
    for(int i = 5; i < argc; i++) {
      c.clear();
      vector<string> res = split(string(argv[i]), "/");
      assert(res.size() >= 3);
      bucket_id = stoi(res[1]);
      osd_id = stoi(res[2]);
      switch (stoi(res[0])) {
        case 0: //osd_crush_remove
          assert(res.size() == 3);
          c.osd_crush_remove.push_back({bucket_id, osd_id});
          break;
        case 1: //osd_crush_add
          assert(res.size() == 4);
          weight = stoi(res[3]);
          c.osd_crush_add.push_back({bucket_id, {osd_id, weight}});
          break;
        case 2: //osd_osdmap_remove
          assert(res.size() == 3);
          c.osd_osdmap_remove.push_back({bucket_id, osd_id});
          break;
        case 3: //osd_osdmap_add
          c.osd_osdmap_add.push_back({bucket_id, osd_id});
          assert(res.size() == 3);
          break;
        case 4: //osd_weight_change
          assert(res.size() == 4);
          weight = stoi(res[3]);
          c.osd_weight_change.push_back({bucket_id, {osd_id, weight}});
          break;
        default:
          cout << "argv error!" << endl;
      }
      //map.apply_map_change(c);
      cout << "remap count:" << map.apply_map_change(c) << endl;
      if (!map.check_mapping_result()) {
        cout << __func__ << " mapping result failed" << endl;
      }
      //map.dump_result();
    }
  }
}

