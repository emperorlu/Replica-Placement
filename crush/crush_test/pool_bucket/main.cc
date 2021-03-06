#include "pool_bucket.h"
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
  if (argc >= 3) {
    int pool_size, pg_num;
    int type, bucket_id, weight;
    pool_size = atoi(argv[1]);
    pg_num = atoi(argv[2]);

    pool_bucket map(pool_size, pg_num);
    vector<int> w(3, 1);
    map.gen_map(w);
    map.init_crushmap_pg_target();
    map.dump_map();
    map.init_pg_mappings();
    map.dump_result();
    map_change c;
    if (!map.check_mapping_result()) {
      cout << __func__ << " mapping result failed" << endl;
    }

    //c.crush_remove.push_back(0);
    //cout << "remap count:" << map.apply_map_change(c) << endl;
    //map.dump_result();
    //c.clear();
    c.crush_add.push_back({3, 1});
    cout << "remap count:" << map.apply_map_change(c) << endl;
    map.dump_result();

    /*
    for(int i = 3; i < argc; i++) {
      c.clear();
      vector<string> res = split(string(argv[i]), "/");
      assert(res.size() >= 2);
      bucket_id = stoi(res[1]);
      switch (stoi(res[0])) {
        case 0: //osd_crush_remove
          assert(res.size() == 2);
          c.crush_remove.push_back(bucket_id);
          break;
        case 1: //osd_crush_add
          assert(res.size() == 3);
          weight = stoi(res[2]);
          c.crush_add.push_back({bucket_id, weight});
          break;
        case 2: //osd_osdmap_remove
          assert(res.size() == 2);
          c.osdmap_remove.push_back(bucket_id);
          break;
        case 3: //osd_osdmap_add
          c.osdmap_add.push_back(bucket_id);
          assert(res.size() == 2);
          break;
        case 4: //osd_weight_change
          assert(res.size() == 3);
          weight = stoi(res[2]);
          c.weight_change.push_back({bucket_id, weight});
          break;
        default:
          cout << "argv error!" << endl;
      }
      map.apply_map_change(c);
      //cout << "remap count:" << map.apply_map_change(c) << endl;
      if (!map.check_mapping_result()) {
        cout << __func__ << " mapping result failed" << endl;
      }
      map.dump_result();
    }
    */
  }
}

