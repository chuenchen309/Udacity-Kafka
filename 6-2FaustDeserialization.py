from dataclasses import asdict, dataclass
import json
import faust

# Define a ClickEvent Record Class with an email (str), timestamp (str), uri(str), and number (int)
# See: https://docs.python.org/3/library/dataclasses.html
# See: https://faust.readthedocs.io/en/latest/userguide/models.html#model-types

@dataclass()
class ClickEvent(faust.Record):
    username: str
    currency: str
    amount: int

app = faust.App("sample2", broker="kafka://localhost:9092")

# Provide the key (uri) and value type to the clickevent
clickevents_topic = app.topic("com.udacity.streams.clickevents",
                              key_type=str,
                              value_type=ClickEvent)

@app.agent(clickevents_topic)
async def clickevent(clickevents):
    async for ce in clickevents:
        print(json.dumps(asdict(ce), indent=2))

if __name__ == "__main__":
    app.main()
