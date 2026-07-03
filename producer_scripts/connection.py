
import os
import time
import json
from dotenv import load_dotenv
from azure.eventhub import EventHubProducerClient, EventData


from producer import get_ticker_data, get_product_data, get_candle_data, base_url, product_ids
load_dotenv()



# Connection string and Event Hub name from environment variables
EVENT_HUB_CONNECTION_STR = os.getenv("EVENT_HUB_CONNECTION_STRING")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")


# Initialise producer client
producer = EventHubProducerClient.from_connection_string(
    conn_str=EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME)
 

def send_to_eventhub(events):
    """
    Sends a batch of events to Event Hub
    """

    batch = producer.create_batch()
    sent_count = 0

    for event in events:
        event_json = json.dumps(event)

        try: 
            batch.add(EventData(event_json)) 
            sent_count += 1

        except ValueError:
            print("Batch reached its maximum size limit")


            if len(batch) > 0:
            # if batch is full, send and start new batch
                batch = producer.create_batch(batch)
                batch.add(EventData(event_json))
                sent_count += 1 

        except Exception as e:
            print(f"Unexpected transmission error on an individual event: {e}")

    if len(batch) > 0:
        producer.send_batch(batch)

    return sent_count 



# Get all events from the three streams and combine them into a single list    
def get_all_events():    
  
  while True:
    try:
      total_events = 0

      print("Streaming events to Event Hub...")
      total_events += send_to_eventhub(get_ticker_data(base_url, product_ids))
      total_events += send_to_eventhub(get_product_data(base_url, product_ids))
      total_events += send_to_eventhub(get_candle_data(base_url, product_ids))

      
      # print total sent 
      print(f"Sent {total_events} events to Event Hub")

      time.sleep(80)

    except Exception as e:
      print(f"Pipeline loop failed: {e}")
      time.sleep(5)



if __name__ == "__main__":
   get_all_events()