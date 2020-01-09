package foundationdb

import (
	"fmt"
	"encoding/json"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

type FoundationDB struct {
	connected bool
	ClusterFile string `toml:"cluster_file"`
	db fdb.Database
}

func (s *FoundationDB) SampleConfig() string {
	return `
## Specific clusterfile to use, or leave undefined/empty for default
# cluster_file = /etc/foundationdb/fdb.cluster
`
}

func (s *FoundationDB) Description() string {
	return "FoundationDB cluster metrics"
}

func (s *FoundationDB) StartConnection() {
	if !s.connected {
		fdb.MustAPIVersion(610)
		if len(s.ClusterFile) > 0 {
			s.db = fdb.MustOpenDatabase(s.ClusterFile)
		} else {
			s.db = fdb.MustOpenDefault()
		}
		s.connected = true
	}
}

func flattenInterface(input map[string]interface{}, prefix string, output map[string]interface{}) {
	for k, rawv := range input {
		realk := prefix + k
		switch v := rawv.(type) {
			/*
		case []interface{}:
			for i, val := range v {
				sub_realk := realk + "_" + string(i)
				flattenInterface(val, sub_realk, output)
			}
*/
		case map[string]interface{}:
			flattenInterface(v, realk + "_", output)
		default:
			output[realk] = v
		}
	}
}

func (s *FoundationDB) generatePoint(acc telegraf.Accumulator, measurement string, v map[string]interface{}, tags map[string]interface{}) error {
	flat := make(map[string]interface{})
	flattenInterface(v, "", flat)
	tmptags := make(map[string]string)
	for k, rawv := range tags {
		switch v := rawv.(type) {
		case string:
			tmptags[k] = v
		default:
			tmptags[k] = fmt.Sprintf("%v", v)
		}
	}
	acc.AddFields("fdb_"+measurement, flat, tmptags)
	return nil
}

func (s *FoundationDB) Gather(acc telegraf.Accumulator) error {
	s.StartConnection()

	ret, err := s.db.Transact(func (tr fdb.Transaction) (interface{}, error) {
		j := tr.Get(fdb.Key([]byte("\xff\xff/status/json"))).MustGet()
		return j, nil
	})

	if err != nil {
		return err
	}

	data := make(map[string]interface{})
	
	switch v := ret.(type) {
	case []byte:
		json.Unmarshal(v, &data)
	default:
		return nil
	}

	cluster := data["cluster"].(map[string]interface{})

	// TODO layer instances
	
	//fmt.Println("processes", cluster["processes"])
	processes := cluster["processes"].(map[string]interface{})
	delete(cluster, "processes")
	//fmt.Println("machines", cluster["machines"])
	machines := cluster["machines"].(map[string]interface{})
	delete(cluster, "machines")

	s.generatePoint(acc, "cluster", cluster, map[string]interface{}{})
	
	for _, v := range processes {
		process := v.(map[string]interface{})
		//fmt.Println("roles", process["roles"])
		roles := process["roles"].([]interface{})
		delete(process, "roles")
		locality := process["locality"].(map[string]interface{})
		delete(process, "locality")
		s.generatePoint(acc, "process", process, locality)
		for _, vr := range roles {
			role := vr.(map[string]interface{})
			locality["role"] = role["role"].(string)
			delete(role, "role")
			s.generatePoint(acc, "role", role, locality)
		}
	}

	for _, v := range machines {
		machine := v.(map[string]interface{})
		locality := machine["locality"].(map[string]interface{})
		delete(machine, "locality")
		s.generatePoint(acc, "machine", machine, locality)
	}

	for layername, rawmaybelayerinfo := range cluster["layers"].(map[string]interface{}) {
		switch maybelayerinfo := rawmaybelayerinfo.(type) {
		case map[string]interface{}:
			if _, ok := maybelayerinfo["instances"]; ok {
				// maybe is now actually
				instances := maybelayerinfo["instances"].(map[string]interface{})
				delete(maybelayerinfo, "instances")
				for k, rawv := range instances {
					s.generatePoint(acc, "layer_"+layername, rawv.(map[string]interface{}),
						map[string]interface{}{"instance": k})
				}
			}
		}
	}
	
	return nil
}

func init() {
	inputs.Add("foundationdb", func() telegraf.Input {
		s := FoundationDB{}
		s.connected = false
		return &s
	})
}
