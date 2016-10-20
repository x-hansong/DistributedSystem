package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	log.Printf("Map inFile: %s, MapTaskNumber: %d, nReduce: %d, jobName: %s", inFile, mapTaskNumber, nReduce, jobName)
	buff, err := ioutil.ReadFile(inFile)
	if err != nil {
		log.Fatal("打开文件失败: ", err)
	}

	kvs := mapF(inFile, string(buff))

	for j := 0; j < nReduce; j++ {
		partitionFile := reduceName(jobName, mapTaskNumber, j)
		fout, err := os.Create(partitionFile)
		if err != nil {
			log.Fatal("创建文件失败: ", err)
		}
		enc := json.NewEncoder(fout)
		for _, kv := range kvs {
			if ihash(kv.Key) % uint32(nReduce) == uint32(j) {
				err = enc.Encode(&kv)
				if err != nil {
					log.Fatal("json编码失败: ", err)
				}
			}
		}
		fout.Close()
	}
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
