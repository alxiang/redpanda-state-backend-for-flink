import uuid
import string
import random
import json

payload_length = 10000
lines = 10000
text_file = open("json-10k.txt", "w")

for i in range(lines):
    json_object =  {
        "id": str(uuid.uuid1()),
        "payload": ''.join(random.choices(string.ascii_uppercase +
                             string.digits, k = payload_length))
    }
    text_file.write(json.dumps(json_object)+"\n")

text_file.close()
