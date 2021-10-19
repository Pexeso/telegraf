package foundationdb

import (
	"fmt"
	"encoding/json"
	"strings"
	"errors"

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
# IMPORTANT NOTES
# 1) This fetches the JSON status from the cluster and processes
# it. That's cluster-wide information, so you only need to run
# this plugin once per cluster.
# 2) Fetching the JSON status is slightly expensive, you don't
# want to do it more or faster than is necessary. So you should
# only run this plugin once per cluster.
# 3) This plugin produces a lot of data every time it runs, with
# the amount proportional to cluster size.
# Consider lowering the telegraf metric_batch setting dramatically
# to help out the output plugins. In particular, the Influx plugin
# experiences significant weirdness if metric_batch is left at
# the default value.

## Specific clusterfile to use, or leave undefined/empty for default
# cluster_file = /etc/foundationdb/fdb.cluster
`
}

func (s *FoundationDB) Description() string {
	return "FoundationDB cluster metrics"
}

func (s *FoundationDB) StartConnection() {
	if !s.connected {
		fdb.MustAPIVersion(620)
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

func (s *FoundationDB) generatePoint(acc telegraf.Accumulator, cluster string, measurement string, v map[string]interface{}, tags map[string]interface{}) error {
	flat := make(map[string]interface{})
	flattenInterface(v, "", flat)
	tmptags := make(map[string]string)
	tmptags["cluster"] = cluster
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

	var cluster map[string]interface{}
	switch v := data["cluster"].(type) {
	case map[string]interface{}:
		cluster = v
	}

	var clusterid string
	if connstr, ok := cluster["connection_string"]; ok {
		switch v := connstr.(type) {
		case string:
			clusterid = strings.SplitN(v, ":", 2)[0]
		default:
			return errors.New("cluster connection string isn't a string")
		}
	} else {
		// no connection string, so no cluster identifier
		return errors.New("no cluster identifier")
	}
	
	// TODO layer instances

	var processes map[string]interface{}
	switch v := cluster["processes"].(type) {
	case map[string]interface{}:
		processes = v
	}
	delete(cluster, "processes")

	var machines map[string]interface{}
	switch v := cluster["machines"].(type) {
	case map[string]interface{}:
		machines = v
	}
	delete(cluster, "machines")

	var layers map[string]interface{}
	switch v := cluster["layers"].(type) {
	case map[string]interface{}:
		layers = v
	}
	delete(cluster, "layers")
	
	s.generatePoint(acc, clusterid, "cluster", cluster, map[string]interface{}{})
	
	for _, v := range processes {
		process := v.(map[string]interface{})
		var roles []interface{}
		switch v := process["roles"].(type) {
		case []interface{}:
			roles = v
		}
		delete(process, "roles")

		var locality map[string]interface{}
		switch v := process["locality"].(type) {
		case map[string]interface{}:
			locality = v
		default:
			// without locality, don't produce data points
			continue
		}
		delete(process, "locality")
		s.generatePoint(acc, clusterid, "process", process, locality)

		for _, vr := range roles {
			role := vr.(map[string]interface{})
			locality["role"] = role["role"].(string)
			delete(role, "role")
			s.generatePoint(acc, clusterid, "role", role, locality)
		}
	}

	for _, v := range machines {
		machine := v.(map[string]interface{})
		var locality map[string]interface{}
		switch v := machine["locality"].(type) {
		case map[string]interface{}:
			locality = v
		default:
			// without locality, don't produce data points
			continue
		}
		delete(machine, "locality")
		s.generatePoint(acc, clusterid, "machine", machine, locality)
	}

	for layername, rawmaybelayerinfo := range layers {
		switch maybelayerinfo := rawmaybelayerinfo.(type) {
		case map[string]interface{}:
			if _, ok := maybelayerinfo["instances"]; ok {
				// maybe is now actually
				instances := maybelayerinfo["instances"].(map[string]interface{})
				delete(maybelayerinfo, "instances")
				for k, rawv := range instances {
					s.generatePoint(acc, clusterid, "layer_"+layername, rawv.(map[string]interface{}),
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
