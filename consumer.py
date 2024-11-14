from kafka import KafkaConsumer
import json 
import time
from result_backend import ResultBackend


class Consumer:
    def __init__(self, kafka_broker, result_backend):
        self.consumer = KafkaConsumer(  # Corrected the typo 'comsumer' to 'consumer'
            "task_queue",
            bootstrap_servers=kafka_broker,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id="consumer_group"
        )
        self.result_backend = result_backend
        
    def process_task(self, task):
        task_id = task["task_id"]
        task_type = task["task"]
        args = task["args"]
        
        self.result_backend.set_status(task_id, "processing")
        print(f"Processing task {task_id}: {task_type} with args {args}")
        
        result = None
        try:
            if task_type == "add":
                result = sum(args)
            elif task_type == "multiply":
                result = 1
                for arg in args:
                    result *= arg
            self.result_backend.set_status(task_id, "success", result)  # Corrected 'sucess' to 'success'
        except Exception as e:  # Changed 'error' to 'Exception'
            self.result_backend.set_status(task_id, "failed", str(e))
            
    def run(self):
        for message in self.consumer:
            task = message.value
            self.process_task(task)
            time.sleep(1)
            
if __name__ == "__main__":
    result_backend = ResultBackend()
    consumer = Consumer(kafka_broker="localhost:9092", result_backend=result_backend)
    consumer.run()

