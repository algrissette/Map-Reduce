package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

// Map functions return a slice of KeyValue.
// This Keyvalue structure holds a Key and a Value together
type KeyValue struct {
	Key   string
	Value string
}

// Worker executes map and reduce tasks
// main function for worker to execute all of its intended functions
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		var task Task
		if !call("Coordinator.RequestTask", &EmptyArs{}, &task) {
			log.Fatal("Failed to request task from coordinator")
		}

		switch task.TaskType {
		case "Map":
			performMapTask(task, mapf)
		case "Reduce":
			performReduceTask(task, reducef)
		}

		if !call("Coordinator.TaskCompleted", &task, &EmptyReply{}) {
			log.Fatal("Failed to report task completion to coordinator")
		}
	}
}

func performMapTask(task Task, mapf func(string, string) []KeyValue) {
	fileContent := readFileContent(task.Filename)
	intermediate := mapf(task.Filename, fileContent)

	// Create a map to hold intermediate data for each hash value
	intermediateData := make(map[int][]KeyValue)

	// Partition intermediate data based on hash value
	for _, kv := range intermediate {
		hashValue := ihash(kv.Key) % task.NumReduce
		intermediateData[hashValue] = append(intermediateData[hashValue], kv)
	}

	// Save intermediate data to files
	saveIntermediate(task, intermediateData)
}

// Helper function to read file content
func readFileContent(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	return string(content)
}

func saveIntermediate(task Task, intermediateData map[int][]KeyValue) {
	for hashValue := 0; hashValue < task.NumReduce; hashValue++ {
		values := intermediateData[hashValue]
		filename := fmt.Sprintf("mr-%v-%v", task.NumMap, hashValue)
		ofile, err := os.Create(filename)
		if err != nil {
			log.Fatalf("cannot create intermediate file %s: %v", filename, err)
		}

		enc := json.NewEncoder(ofile)
		for _, kv := range values {
			if err := enc.Encode(&kv); err != nil {
				log.Fatalf("error encoding intermediate data to file %s: %v", filename, err)
			}
		}

		if err := ofile.Close(); err != nil {
			log.Fatalf("error closing file %s: %v", filename, err)
		}
	}
}

// reduce task
func performReduceTask(task Task, reducef func(string, []string) string) {
	// Open the output file for writing reduce results
	oname := fmt.Sprintf("mr-out-%v", task.Bucketnum)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("cannot create output file: %v", err)
	}

	// Retrieve intermediate data for the specific reducer task
	intermediate := retrieveIntermediate(task)

	// Map to hold intermediate values for each distinct key
	intermediateMap := make(map[string][]string)

	// Populate the intermediate map with values
	for _, kv := range intermediate {
		intermediateMap[kv.Key] = append(intermediateMap[kv.Key], kv.Value)
	}

	// Perform reduction for each distinct key
	for key, values := range intermediateMap {
		output := reducef(key, values)
		// Write the reduce output to the output file
		fmt.Fprintf(ofile, "%v %v\n", key, output)
	}

	// Close the output file
	if err := ofile.Close(); err != nil {
		log.Fatalf("error closing output file: %v", err)
	}
}

// Updated retrieveIntermediate function
// Updated retrieveIntermediate function
func retrieveIntermediate(task Task) []KeyValue {
	var intermediate []KeyValue
	for i := 1; i <= task.NumMap; i++ {
		filename := fmt.Sprintf("mr-%v-%v", i, task.Bucketnum)
		file, err := os.Open(filename)
		if err != nil {
			if os.IsNotExist(err) {
				// File does not exist, continue to the next file
				continue
			} else {
				log.Fatalf("cannot open intermediate file: %v", err)
			}
		}

		dec := json.NewDecoder(file)
		for {
			var keyval KeyValue
			err := dec.Decode(&keyval)
			if err == io.EOF {
				break
			} else if err != nil {
				log.Fatalf("error decoding intermediate data: %v", err)
			}
			intermediate = append(intermediate, keyval)
		}
		file.Close() // Close the file after reading is done
	}
	return intermediate
}

// Helper function to save output

// Universal Task structure

// CallExample sends an Example RPC to the coordinator.
func CallExample() {
	args := ExampleArgs{}
	args.X = 99
	reply := ExampleReply{}
	call("Coordinator.Example", &args, &reply)
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// call sends an RPC request to the coordinator and waits for the response.
// It usually returns true, and false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println("Unable to Call", rpcname, "- Got error:", err)
	return false
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
