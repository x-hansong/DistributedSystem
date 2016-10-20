package mapreduce

import (
	"fmt"
	"sync"
	"log"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	
	var wg sync.WaitGroup
	for i := 0; i < ntasks; i++ {
		wg.Add(1)
		go func(taskNum int, nios int, phase jobPhase) {
			log.Printf("current taskNum: %v, nios: %v, phase: %v\n",  taskNum, nios, phase)
			defer wg.Done()
			for {
				worker := <-mr.registerChannel
				log.Printf("Current worker: %v\n", worker)

				args := DoTaskArgs{
					JobName:mr.jobName,
					File:mr.files[taskNum],
					Phase:phase,
					TaskNumber:taskNum,
					NumOtherPhase:nios,
				}
				ok := call(worker, "Worker.DoTask", &args, new(struct{}))
				if ok { //任务完成
					go func() {
						//worker重新投入使用
						mr.registerChannel <- worker
					}()
					break //退出循环
				}
			}
		}(i, nios, phase)
		wg.Wait() //等待所有任务完成
	}
	fmt.Printf("Schedule: %v phase done\n", phase)
}
