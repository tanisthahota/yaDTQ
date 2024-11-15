from kafka import KafkaProducer 
import json 
import uuid 

class Producer :
	def __init__(self,kafka_broker):
		self.producer = KafkaProducer(
		value_serializer = lambda m: json.dumps(m).encode('ascii'))
	def send_task(self,task_type,args):
		task_id = str(uuid.uuid4())
		task = {
		  "task_id" : task_id ,
		  "task_type" : task_type,
		  "args" : args
		}
		self.producer.send("task: ",task)
		print(f"Task sent : {task}")
		return task_id 

if __name__ == "__main__":
	producer = Prodcuer(kafka_broker = "localhost:9092)
	task_id = ("add",[1,2])
	
