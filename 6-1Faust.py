import faust

# Create the faust app with a name and broker
# See: https://faust.readthedocs.io/en/latest/userguide/application.html#application-parameters
app = faust.App("hello-world-faust", broker="localhost:9092")

# Connect Faust to com.udacity.streams.clickevents
# See: https://faust.readthedocs.io/en/latest/userguide/application.html#app-topic-create-a-topic-description
topic = app.topic("com.udacity.lesson2.sample4.purchases")

# Provide an app agent to execute this function on topic event retrieval
# See: https://faust.readthedocs.io/en/latest/userguide/application.html#app-agent-define-a-new-stream-processor

@app.agent(topic)
async def clickevent(clickevents):
    # Define the async for loop that iterates over clickevents
    # See: https://faust.readthedocs.io/en/latest/userguide/agents.html#the-stream
    async for event in clickevent:
        print(event)

if __name__ == "__main__":
    app.main()
